/*
 * Copyright (c) 2010 Nutanix Inc. All rights reserved.
 *
 * Author: aron@nutanix.com
 *
 */

#ifndef _STARGATE_EXTENT_STORE_EGROUP_READ_REPLICA_OP_H_
#define _STARGATE_EXTENT_STORE_EGROUP_READ_REPLICA_OP_H_

#include <unordered_map>

#include "stargate/extent_store/disk_manager.h"
#include "stargate/extent_store/egroup_base_op.h"
#include "stargate/extent_store/egroup_manager.h"
#include "stargate/extent_store/egroup_state_helper_op.h"
#include "stargate/extent_store/extent_group_state.h"
#include "stargate/extent_store/shared_extent_write_state.h"
#include "util/base/iobuffer.h"
#include "util/base/tuple256.h"
#include "util/block_store/object.h"

namespace nutanix { namespace stargate { namespace extent_store {

class ExtentStore::DiskManager::ExtentGroupReadReplicaOp :
    public ExtentStore::DiskManager::ExtentGroupBaseOp {
 public:
  
  
  

  typedef shared_ptr<ExtentGroupReadReplicaOp> Ptr;
  typedef shared_ptr<const ExtentGroupReadReplicaOp> PtrConst;

  // Constructor used when the op is created to serve a ReplicaReadEgroup RPC.
  ExtentGroupReadReplicaOp(DiskManager *disk_manager,
                           net::Rpc *rpc,
                           const interface::ReplicaReadEgroupArg *arg,
                           interface::ReplicaReadEgroupRet *ret,
                           const Function<void()>& rpc_done_cb);

  // Constructor used when the op is invoked directly by another op to read the
  // data of the extent group. 'eg_state' must be valid and either a read or a
  // write lock should be held on the egroup. 'read_complete_egroup' specifies
  // whether the entire egroup should be read regardless of the work unit bytes
  // limit specified by FLAGS_estore_replication_work_unit_bytes. The
  // 'ignore_corrupt_flag' specifies whether we should read the egroup even if
  // it is corrupt. If the 'skip_checksum_verification' is set to true, the
  // read data is returned without verifying the checksum. The
  // 'slice_state_vec' contains the list of slices to be read. If
  // 'target_checksum_type' is specified, and it doesn't match the current
  // checksum type of this replica, the op will compute the requested target
  // checksums as it processes the slices. This will only be applicable if
  // 'read_complete_egroup' is true. The resulting slice states will be placed
  // in 'converted_slice_state_vec_'. 'cipher_key_id' must be provided if the
  // egroup is encrypted and checksum conversion is required.
  ExtentGroupReadReplicaOp(DiskManager *disk_manager,
                           int64 extent_group_id,
                           StargateQosPriority::Type qos_priority,
                           ExtentGroupState::Ptr eg_state,
                           bool read_complete_egroup,
                           bool ignore_corrupt_flag = false,
                           bool skip_checksum_verification = false,
                           const vector<const ExtentGroupState::SliceState *> *
                             slice_state_vec = nullptr,
                           int target_checksum_type = 0,
                           const string& cipher_key_id = string());

  ~ExtentGroupReadReplicaOp();

  virtual void StartImpl();
  virtual void ExtentGroupLocksAcquired();
  virtual void ExtentGroupStateFetched(StargateError::Type error,
                                       const ExtentGroupState::Ptr& eg_state);

  virtual int32 Cost() const;

  bool MaybeAddToQosPrimaryQueue() const { return is_primary_; }

  // Accessors.
  const interface::ReplicaReadEgroupArg *rpc_arg() const { return rpc_arg_; }

  const interface::ReplicaReadEgroupRet *rpc_ret() const { return rpc_ret_; }

  int64 extent_group_id() const { return extent_group_id_; }

  const ArenaVector<ExtentGroupState::SliceState::UPtrConst>&
  all_slices_state_vec() const { return all_slices_state_vec_; }

  const ArenaVector<std::vector<int64>>& all_slices_block_info_vec() const {
    return all_slices_block_info_vec_;
  }

  ArenaVector<ExtentGroupState::SliceState::UPtr>&&
  mutable_converted_slice_state_vec() {
    return move(converted_slice_state_vec_);
  }

  const IOBuffer::Ptr& egroup_data() const { return egroup_data_; }
  IOBuffer::Ptr&& mutable_egroup_data() { return move(egroup_data_); }

 private:
  DISALLOW_COPY_AND_ASSIGN(ExtentGroupReadReplicaOp);

  // Release all locks held by this op.
  void ReleaseLocks();

  // Callback invoked when 'disk_manager_->egroup_map_' lookup completes.
  void EgroupMapLookupDone(block_store::Object::Ptr&& key,
                           block_store::CharArrayObject&& value);

  // Callback invoked when lookup for an extent group physical state from
  // Medusa is complete.
  void LookupExtentGroupPhysicalStateDone(
    medusa::MedusaError::Type error,
    int medusa_err_src_id,
    medusa::MedusaValue::PtrConst&& mval);

  // Composes the partial set of slices to be read for the egroup.
  void ComposePartialSlicesToRead();

  // Starts a helper op that looks up the physical metadata of info egroups
  // associated to the parity egroup. Applicable for AES parity egroups only.
  void StartInfoEgroupHelperOp();

  // Callback invoked when the helper op finishes the lookup of the physical
  // metadata of the AES info egroups.
  void InfoEgroupLookupDone(StargateError::Type error);

  // Composes the metadata portion for an AES info egroup in the response
  // protobuf sent to the caller. Applicable if the RPC was issued to an
  // AES parity egroup.
  void ComposeInfoEgroupResponseMetadata(
    const ExtentGroupState::PtrConst& info_eg_state,
    int64 local_info_egroup_id,
    int32 index);

  // Register this op with SharedExtentWriteState if there is an existing
  // registration for any extent of egroup 'extent_group_id_' due to
  // outstanding sequential writes.
  void MaybeRegisterOp();

  // Handler for egroups in the extent based metadata format.
  void HandleExtentBasedMetadataFormat();

  // Fetch the cipher key from Mantle if the metadata 'mvalue' contains a valid
  // cipher key id, in case the egroup is encrypted.
  void MaybeFetchCipherKey(
    const medusa::MedusaExtentGroupPhysicalStateEntry::PtrConst& mvalue);

  // Populates 'slice_state_vec_' from the extent group metadata in 'rpc_ret_'
  // starting with slice at index 'next_slice_idx_' until a work unit amount of
  // data (dictated by FLAGS_estore_replication_work_unit_bytes) is assembled.
  void PopulateSliceStateVec();

  // Callback invoked once the cipher key is fetched from Mantle when the
  // egroup is encrypted. 'error' is the error code returned by Mantle.
  void FetchCipherKeyDone(mantle::MantleErrorProto::Type error);

  // Callback invoked when this op has been registered with the extent group
  // state for 'extent_group_id_'. Once this happens, it is guaranteed that
  // no write operations on 'extent_group_id_' will be active.
  void ReadOpRegistered();

  // Fetches descriptor for reading the extent group file.
  void FetchEgroupDescriptor();

  // Issue the read request to the disk.
  void IssueDiskRead();

  // The following is executed by the disk thread to read the slice data from
  // the disk.
  void DiskReadData();

  // Callback invoked when the disk read finishes.
  void DiskReadDataDone(StargateError::Type err);

  // Callback invoked when the asynchronous disk read finishes if the disk read
  // was for multiple slices.
  void DiskReadVecDataDone(
    std::vector<IOBuffer::Ptr>&& iobuf_vec,
    int read_errno,
    int64 start_time_usecs);

  // Read the slice data asynchronously from the disk.
  void AIODiskReadData();
  // Read the slice data asynchronously from the disk slice by slice.
  void AIODiskReadDataPerSlice();

  // Callback invoked when the asynchronous disk read finishes.
  void AIODiskReadDataDone(
    std::vector<IOBuffer::Ptr>&& iobuf_vec,
    int read_errno);


  // Callback invoked when the asynchronous disk read finishes per slice.
  void AIODiskReadDataDonePerSlice(
    IOBuffer::Ptr&& iobuf,
    int read_errno,
    size_t xx);

  // Method executed by a thread in Stargate's compute pool to verify the
  // checksum of the transformed slice buffer read from disk. 'slice_vec_index'
  // is the index of the corresponding slice in 'slice_state_vec_'.
  void VerifyChecksum(int slice_vec_index);

  // Method called in the main thread when the above computation completes.
  void VerifyChecksumDone(StargateError::Type err);

  // Compute the payload to be sent in the response.
  void ComputeResponsePayload();

  // Set the operation's trace attributes.
  void SetTraceAttributes();

  // Finishes the read replica op with the given error. For AES-EC parity
  // egroups, this also finishes any helper op instantiated for reading the
  // metadata of its associated info egroups.
  void FinishWithError(StargateError::Type error);

 private:
  // The RPC argument and response.
  const interface::ReplicaReadEgroupArg *rpc_arg_;
  interface::ReplicaReadEgroupRet *rpc_ret_;

  // The extent group id that needs tentative resolution.
  const int64 extent_group_id_;

  // Pointer to the extent group state.
  ExtentGroupState::Ptr eg_state_;

  // List of slice group ids that are locked for reading.
  Container<std::list<Tuple256>, 1> locked_slice_group_id_list_;

  // Whether this reader op has been registered with the state of
  // 'extent_group_id_'.
  bool registered_reader_op_;

  // The following contains a mapping from a slice id to the corresponding
  // data we read from the disk for that slice.
  typedef ArenaUnorderedMap<int64, IOBuffer::PtrConst> SliceIdMap;
  SliceIdMap slice_data_map_;

  // Contains states for slices that'll be read.
  ArenaVector<ExtentGroupState::SliceState::UPtrConst> slice_state_vec_;

  // Contains the list of blocks we will read per slice in 'slice_state_vec_'.
  ArenaVector<std::vector<int64>> slice_block_info_vec_;

  // The encryption transformations of the extent group.
  std::vector<DataTransformation::Type> encryption_type_vec_;

  // Disk IO start time.
  int64 disk_io_start_time_usecs_;

  // Pointer to the helper op that is used to lookup the physical metadata of
  // the AES info egroups associated to the parity egroup being read by the op.
  shared_ptr<ExtentGroupStateHelperOp> info_egroup_helper_op_;

  // Unique pointer for holding the self-constructed RPC argument when not
  // invoked from an RPC.
  unique_ptr<const interface::ReplicaReadEgroupArg> rpc_arg_storage_;

  // Unique pointer for holding the self-constructed RPC return value when not
  // invoked from an RPC.
  unique_ptr<interface::ReplicaReadEgroupRet> rpc_ret_storage_;

  // Storage for the egroup data when not invoked using an RPC.
  IOBuffer::Ptr egroup_data_;

  // The vector containing states of all the slices in the extent group (as
  // opposed to 'slice_state_vec_' which only contains the states of the slices
  // that the op is currently working on).
  ArenaVector<ExtentGroupState::SliceState::UPtrConst> all_slices_state_vec_;

  // Contains the list of blocks we will read per slice in
  // 'all_slices_state_vec_'.
  ArenaVector<std::vector<int64>> all_slices_block_info_vec_;

  // The target checksum type requested by the caller which should be computed
  // for the slices that are read.
  Optional<interface::ChecksumType> target_checksum_type_;

  // The vector containing states of all the slices in the extent group, with
  // the checksums updated to 'target_checksum_type_'.
  ArenaVector<ExtentGroupState::SliceState::UPtr> converted_slice_state_vec_;

  // The next index in the 'slices' array in the extent group metadata in
  // 'rpc_ret_' that we will process next. This is used to resume the op after
  // processing the work unit bytes amount of data specified by
  // FLAGS_estore_replication_work_unit_bytes when 'read_complete_egroup_' is
  // true.
  int next_slice_idx_;

  // Number of outstanding computations given to Stargate's compute pool.
  int32 outstanding_compute_reqs_;

  // Whether we are retrying read due to a checksum mismatch in the previous
  // attempt.
  bool is_retry_on_checksum_mismatch_;

  // Whether the op was created by another extent store op or not. If it was
  // then 'is_primary_' is set to false.
  bool is_primary_;


  int slices_callback_pending_;
  bool is_error_=false;
  vector<IOBuffer::Ptr> iobuf_vec_;


  // Whether this extent group is managed by AES.
  const bool managed_by_aes_;

  // Indicates whether the extent group is being converted to AES format.
  const bool aes_conversion_in_progress_;

  // Whether the extent group is using the extent based metadata format.
  bool extent_based_metadata_format_;

  // Whether the entire egroup should be read regardless of the work unit bytes
  // limit specified by FLAGS_estore_replication_work_unit_bytes.
  bool read_complete_egroup_;

  // Should the data be untransformed.
  bool untransform_needed_;

  // The reason for the egroup being marked corrupt. Use this field only when
  // error_ is set to StargateError::kDataCorrupt.
  EgroupCorruptReason corrupt_reason_;

  // The compression transformations of this extent group.
  std::vector<DataTransformation::Type> compression_type_vec_;

  // Whether the extent group supports both compressed and uncompressed slices.
  bool mixed_compression_egroup_;

  // If this op performed any registrations with the active op map in
  // SharedExtentWriteState, then this map will provide the slice group lock id
  // and the corresponding active op state pointer obtained through the
  // registration.
  ArenaUnorderedMap<Tuple256, SharedExtentWriteState::ActiveOpState *>
    active_op_state_map_;

  // If set to true, the checksum verification is skipped.
  bool skip_checksum_verification_;
};

} } } // namespace

#endif // _STARGATE_EXTENT_STORE_EGROUP_READ_REPLICA_OP_H_
