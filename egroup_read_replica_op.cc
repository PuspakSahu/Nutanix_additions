/*
 * Copyright (c) 2010 Nutanix Inc. All rights reserved.
 *
 * Author: aron@nutanix.com
 *
 */

#include "cdp/client/stargate/stargate_base/stargate_util.h"
#include "content_cache/stargate_crypto_util.h"
#include "mantle/mantle.h"
#include "medusa/medusa_printer_util.h"
#include "stargate/extent_store/blockstore_egroup_manager.h"
#include "stargate/extent_store/disk_perf_stat.h"
#include "stargate/extent_store/disk_WAL.h"
#include "stargate/extent_store/disk_WAL_record.h"
#include "stargate/extent_store/egroup_getstate_op.h"
#include "stargate/extent_store/egroup_read_replica_op.h"
#include "stargate/extent_store/shared_extent_write_state.h"
#include "stargate/stargate.h"
#include "stargate/stargate_compute_pool.h"
#include "util/base/adler32.h"
#include "util/base/char_array.h"
#include "util/base/scoped_executor.h"
#include "util/base/string_util.h"
#include "util/base/walltime.h"
#include "util/block_store/backing_store.h"
#include "util/block_store/map.h"
#include "util/misc/random.h"
#include "util/misc/boost_util.h"


using namespace nutanix::block_store;
using namespace nutanix::content_cache;
using namespace nutanix::mantle;
using namespace nutanix::medusa;
using namespace nutanix::misc;
using namespace nutanix::net;
using namespace nutanix::thread;
using namespace std;
using namespace std::placeholders;


DEFINE_bool(error, false,
            "error");

DEFINE_bool(make_the_slices_corrupt, false,
            "make_the_slices_corrupt");

DEFINE_string(list_of_corrupt_slice_ids, "",
            "list_of_corrupt_slice_ids");

DEFINE_int64(unreadable_egroup ,-1,
"unreadabel_egroup - egroup which is" 
"unreadable from the disk , "
"other egrouops can be read perfectly");


DEFINE_bool(allow_partial_unreadable_egroups, false,
            "doesnt throw error and return the"
            " slices even though some slices are unreadable");
            "doesnt throw error and return the slices even though some slices are unreadable");
            "doesnt throw error and return the"
            " slices even though some slices are unreadable");


DEFINE_int64(disk_1 ,-1,
"disk_1");


DEFINE_int64(disk_2 ,-1,
"disk_2");

DEFINE_bool(mark_disc_wal_corrupt, false,
            "mark_disc_wal_corrupt");

DEFINE_bool(set_estore_experimental_inject_egstate_corruption, false,
            "set_estore_experimental_inject_egstate_corruption");

            


DEFINE_int64(estore_experimental_drop_egroup_read_request_egid, -1,
             "If set to a positive value, drop any read request for egroup "
             "with this egid and return error specfied by "
             "'estore_experimental_drop_egroup_read_request_error'.");

DEFINE_string(estore_experimental_drop_egroup_read_request_error, "kNoError",
             "Type of error to return when dropping read request for egroup "
             "specified by "
             "'estore_experimental_drop_egroup_read_request_egid'.");

DEFINE_int64(estore_experimental_read_replica_delay_after_lookup, -1,
             "Unit test flag to delay the read replica op for the specified "
             "egroup id after perfoming the physical state lookup from "
             "Medusa.");

DEFINE_bool(estore_experimental_read_replica_skip_checksum, false,
            "If set checksum check will not be performed, and always it will "
            "be considered as valid");

DECLARE_bool(estore_aio_enabled);
DECLARE_int32(estore_disk_access_threshold_msecs);
DECLARE_int32(estore_disk_queue_drop_ilm_request_threshold_secs);
DECLARE_int32(estore_replication_work_unit_bytes);
DECLARE_bool(estore_experimental_dump_corrupt_slices);
DECLARE_int32(estore_experimental_simulate_corrupt_read_frequency);
DECLARE_bool(estore_experimental_verify_logical_checksums_on_read);

DECLARE_bool(estore_experimental_inject_egstate_corruption);
DECLARE_int64(estore_experimental_inject_non_existent_egroup_id);


namespace nutanix { namespace stargate { namespace extent_store {

using namespace interface;

//-----------------------------------------------------------------------------

ExtentStore::DiskManager::ExtentGroupReadReplicaOp::ExtentGroupReadReplicaOp(
  DiskManager *disk_manager,
  Rpc *rpc,
  const ReplicaReadEgroupArg *arg,
  ReplicaReadEgroupRet *ret,
  const Function<void()>& rpc_done_cb) :
  ExtentGroupBaseOp(
    disk_manager,
    arg->has_qos_priority() ? arg->qos_priority() :
      (arg->qos_principal_name() != StargateUtil::kBackgroundQosPrincipal ?
       StargateQosPriority::kRead : StargateQosPriority::kDefault),
    rpc,
    rpc_done_cb,
    nullptr /* arena */,
    arg->has_classification_id() ? arg->classification_id() :
      (arg->has_classification_name() ?
       disk_manager->globals_->classifier->Register(
         arg->classification_name(), true /* ephemeral */) :
       disk_manager->extent_store()->classification_ids().
         egroup_read_replica_op_id)),
  rpc_arg_(arg),
  rpc_ret_(ret),
  extent_group_id_(arg->extent_group_id()),
  registered_reader_op_(false),
  slice_data_map_(arena_),
  slice_state_vec_(arena_),
  slice_block_info_vec_(arena_),
  disk_io_start_time_usecs_(-1),
  egroup_data_(make_shared<IOBuffer>()),
  all_slices_state_vec_(arena_),
  all_slices_block_info_vec_(arena_),
  converted_slice_state_vec_(arena_),
  next_slice_idx_(0),
  outstanding_compute_reqs_(0),
  is_retry_on_checksum_mismatch_(false),
  is_primary_(!arg->is_secondary()),
  managed_by_aes_(ManagedByAes(arg).managed_by_aes),
  aes_conversion_in_progress_(arg->aes_conversion_in_progress()),
  extent_based_metadata_format_(
    ManagedByAes(arg).extent_based_metadata_format),
  read_complete_egroup_(false),
  untransform_needed_(false),
  corrupt_reason_(kSliceChecksumMismatch),
  mixed_compression_egroup_(false),
  active_op_state_map_(arena_),
  skip_checksum_verification_(false) {

  SetLogMessagePrefix(
    StringJoin("opid=", operation_id_,
               " egroup_id=", extent_group_id_,
               " managed_by_aes=", managed_by_aes_ ? "true" : "false",
               " extent_based=", extent_based_metadata_format_ ?
                 "true" : "false",
               " disk=", disk_manager_->disk_id_,
               (aes_conversion_in_progress_ ?
                  " aes_conversion_in_progress=true" : ""), ": "));

  TAGGED_DCHECK(CheckPrioritySetAfterUpgrade(
                  arg->has_qos_priority(),
                  arg->qos_principal_name())) << arg->ShortDebugString();

  ++disk_manager_->num_outstanding_readreplica_ops_;

  // Setup the activity proto in the base op to allow the op to return its
  // activity traces in the RPC reponse if required.
  SetRpcActivityProto(rpc_ret_->mutable_activity_traces());

  // Set untransform_needed_ only when egroup has compression enabled and all
  // required info is available to untransform such as encryption_key etc.
  if (rpc_arg_->untransform_slices()) {
    if (rpc_arg_->transformation_type().empty() &&
      rpc_arg_->transformation_type_list_size() == 0) {
      TAGGED_VLOG(3) << "Empty transformation list, no untransform is needed";
    } else {
      // Extract the transformation info from the rpc.
      bool support_multiple_transformations = false;
      if (!StargateUtil::ExtractTransformationInfo<
            allocator<DataTransformation::Type>>(
            arg->transformation_type(),
            arg->transformation_type_list(),
            nullptr /* out_transformation_type_vec */,
            &compression_type_vec_,
            &encryption_type_vec_,
            &support_multiple_transformations)) {
        TAGGED_LOG(WARNING) << "Failed to extract transformation info, cannot "
                            << "untransform";
      } else if (!compression_type_vec_.empty() &&
                 !encryption_type_vec_.empty() &&
                 rpc_arg_->cipher_key_id().empty()) {
        TAGGED_LOG(WARNING) << "Compression and Encryption is enabled, but "
                            << "cipher_key_id is missing, cannot untransform";
      } else if (compression_type_vec_.empty()) {
        TAGGED_VLOG(3) << "Compression is not enabled, not doing untransform";
      } else {
        untransform_needed_ = true;
        mixed_compression_egroup_ = support_multiple_transformations;
        TAGGED_VLOG(3) << OUTVARS(untransform_needed_,
                                  mixed_compression_egroup_);
      }
    }
  }
}

//-----------------------------------------------------------------------------

namespace { // anonymous namespace

static ReplicaReadEgroupArg *CreateRpcArg(
  const int64 extent_group_id,
  const int64 disk_id,
  const ExtentStore::DiskManager::ExtentGroupState *const eg_state,
  const StargateQosPriority::Type qos_priority,
  const bool ignore_corrupt_flag,
  const vector<const nutanix::medusa::SliceState *> *const slice_state_vec,
  const bool read_complete_egroup,
  const int target_checksum_type,
  const string& cipher_key_id) {

  ReplicaReadEgroupArg *const arg = new ReplicaReadEgroupArg;
  arg->set_extent_group_id(extent_group_id);
  arg->set_disk_id(disk_id);
  arg->set_qos_priority(qos_priority);
  arg->set_managed_by_aes(eg_state->managed_by_aes());
  arg->set_extent_based_metadata_format(
    eg_state->extent_based_metadata_format());
  arg->set_expected_intent_sequence(-1);
  arg->set_ignore_corrupt_flag(ignore_corrupt_flag);
  if (slice_state_vec) {
    for (const auto& slice_state : *slice_state_vec) {
      const auto arg_slice = arg->add_slices();
      slice_state->CopyTo(arg_slice);
    }
  }

  if (eg_state->has_global_metadata_intent_sequence()) {
    arg->set_global_metadata_intent_sequence(
      eg_state->global_metadata_intent_sequence());
  }

  if (read_complete_egroup && target_checksum_type > 0 &&
      eg_state->has_transformation_type_vec()) {
    const auto& transformation_type_vec = eg_state->transformation_type_vec();
    if (!transformation_type_vec.empty()) {
      // We will need to untransform the slices to compute the target logical
      // checksums.
      for (const auto& type : transformation_type_vec) {
        arg->add_transformation_type_list(type);
      }
      arg->set_untransform_slices(true);
      arg->set_cipher_key_id(cipher_key_id);
    }
  }
  return arg;
}

} // namespace

//-----------------------------------------------------------------------------

ExtentStore::DiskManager::ExtentGroupReadReplicaOp::ExtentGroupReadReplicaOp(
  DiskManager *const disk_manager,
  const int64 extent_group_id,
  const StargateQosPriority::Type qos_priority,
  ExtentGroupState::Ptr eg_state,
  const bool read_complete_egroup,
  const bool ignore_corrupt_flag,
  const bool skip_checksum_verification,
  const vector<const ExtentGroupState::SliceState *> *const slice_state_vec,
  const int target_checksum_type,
  const string& cipher_key_id) :
  ExtentGroupReadReplicaOp(
    disk_manager,
    nullptr /* rpc */,
    CreateRpcArg(extent_group_id, disk_manager->disk_id_, eg_state.get(),
                 qos_priority, ignore_corrupt_flag, slice_state_vec,
                 read_complete_egroup, target_checksum_type, cipher_key_id),
    new ReplicaReadEgroupRet,
    nullptr /* done_cb */) {

  skip_checksum_verification_ = skip_checksum_verification;
  rpc_arg_storage_ = unique_ptr<const ReplicaReadEgroupArg>(rpc_arg_);
  rpc_ret_storage_ = unique_ptr<ReplicaReadEgroupRet>(rpc_ret_);
  eg_state_ = move(eg_state);

  if (read_complete_egroup && target_checksum_type > 0) {
    TAGGED_DCHECK(!extent_based_metadata_format_);
    target_checksum_type_ = static_cast<ChecksumType>(target_checksum_type);

    // Set 'untransform_needed_' to true if we have been asked to re-compute
    // the checksums and the egroup is transformed, so that we can also compute
    // the logical checksums.
    if (!compression_type_vec_.empty() || !encryption_type_vec_.empty()) {
      TAGGED_DCHECK(encryption_type_vec_.empty() || !cipher_key_id.empty());
      untransform_needed_ = true;
    }
  }

  // Since we are not going to fetch the extent group state, we need to
  // manually initialize the block store descriptor when this disk uses block
  // store or the extent group uses extent based metadata format. When egroup
  // data is stored in ext4 files, we don't need to initialize the block store
  // descriptor.
  if (eg_state_->extent_based_metadata_format()) {
    TAGGED_DCHECK(disk_manager_->btree_aesdb_);
    disk_manager_->shared_extent_write_state_->PopulateEgState(
      eg_state_.get());
    if (!disk_manager_->shared_extent_write_state_->data_on_ext4_files()) {
      block_store_descriptor_ =
        arena_alloc_.MakeUnique<
          BlockStoreEgroupManager::BlockStoreExtentDescriptor>(
            disk_manager_->shared_extent_write_state_->egroup_manager_.get(),
            extent_group_id);
    }
  } else if (disk_manager_->block_store_) {
    block_store_descriptor_ =
      arena_alloc_.MakeUnique<BlockStoreEgroupManager::BlockStoreDescriptor>(
        disk_manager_->egroup_manager_.get(), extent_group_id,
        &eg_state_->physical_segment_vec());
  }

  read_complete_egroup_ = read_complete_egroup;
}

ExtentStore::DiskManager::ExtentGroupReadReplicaOp::
~ExtentGroupReadReplicaOp() {

  --disk_manager_->num_outstanding_readreplica_ops_;

  ReleaseLocks();

  if (eg_state_ && rpc_) {
    MaybeDeleteExtentGroupState(extent_group_id_, eg_state_);
  }
}

//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReadReplicaOp::ReleaseLocks() {
  if (registered_reader_op_) {
    TAGGED_CHECK(eg_state_);
    eg_state_->UnregisterReaderWriterOp(false /* is_writer */);
    registered_reader_op_ = false;
  }

  if (!locked_slice_group_id_list_.empty()) {
    TAGGED_DCHECK(disk_manager_->shared_extent_write_state_);
    for (const Tuple256& id : locked_slice_group_id_list_) {
      if (!active_op_state_map_.empty()) {
        auto it = active_op_state_map_.find(id);
        if (it != active_op_state_map_.end()) {
          TAGGED_VLOG(1) << "Unregistering slice group " << id;
          disk_manager_->shared_extent_write_state_->UnregisterOp(
            id, it->second, false /* is_writer */,
            0 /* write_offset */, -1 /* write_len */, at());
        }
      }
      TAGGED_VLOG(1) << "Releasing lock on slice group: " << id;
      disk_manager_->shared_extent_write_state_->slice_group_locker_.Release(
        id);
    }
    active_op_state_map_.clear();
    locked_slice_group_id_list_.Clear();
  }

  if (egid_locks_held_) {
    UnlockAllExtentGroups();
  }
}

//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReadReplicaOp::StartImpl() {
  TAGGED_DCHECK(fe_->IsWorkerThread());



  LOG(INFO)<<endl<<endl<<endl<<"Startimpl started________myown__part2"<<endl<<endl<<endl;
  


  if (rpc_arg_->has_ilm_migration()) {
    TRACE_ADD_ATTRIBUTE(at_, "ilm_migration", rpc_arg_->ilm_migration());
  }

  // Report the queueing delay seen by this op.
  ReportQueueingDelay();

  // Check if the average queueing delay observed for the local disk is larger
  // that the specified threshold. If so, it indicates that the local disk is
  // very busy. In this case, we will defer any replication triggered for the
  // purpose of non-urgent ILM, to avoid adding more pressure on the disk.
  if (rpc_arg_->ilm_migration() &&
      qos_priority_ == StargateQosPriority::kCurator) {
    disk_manager_->rwlock_.Lock();
    const int32 avg_q_delay_usecs =
      disk_manager_->perf_stat_->AvgQueueingDelayUsecs();
    disk_manager_->rwlock_.Unlock();
    if (avg_q_delay_usecs >
          FLAGS_estore_disk_queue_drop_ilm_request_threshold_secs *
            1000000LL) {
      TAGGED_VLOG(0) << "Dropping ILM read replica request for extent group "
                     << extent_group_id_ << "(expected_intent_seq: "
                     << rpc_arg_->expected_intent_sequence() << ") from local "
                     << "disk due to high avg queueing delay of "
                     << avg_q_delay_usecs << " usecs in the local disk queue";

      FinishWithError(StargateError::kRetry);
      return;
    }



  LOG(INFO)<<endl<<endl<<endl<<"Startimpl endedStartimpl started________myown"<<endl<<endl<<endl;
  


  }

  if (extent_group_id_ ==
      FLAGS_estore_experimental_drop_egroup_read_request_egid) {
    TAGGED_LOG(ERROR)
      << "Dropping read request for egroup : " << extent_group_id_
      << " and returning error: "
      << FLAGS_estore_experimental_drop_egroup_read_request_error;

    StargateErrorProto::Type type;
    const bool success = StargateErrorProto::Type_Parse(
        FLAGS_estore_experimental_drop_egroup_read_request_error, &type);
    TAGGED_CHECK(success)
      << FLAGS_estore_experimental_drop_egroup_read_request_error;
    FinishWithError(type);
    return;
  }

  ostringstream msg;
  msg << "Reading replica, expected_intent_seq "
      << rpc_arg_->expected_intent_sequence();

  if (VLOG_IS_ON(1)) {
    msg << ", slices";
    for (int xx = 0; xx < rpc_arg_->slices_size(); ++xx) {
      msg << " " << rpc_arg_->slices(xx).slice_id();
    }
  }

  TAGGED_VLOG(0) << msg.str();

  // Prepare slice state vector with the slices that we are asked to read.
  if (rpc_arg_->slices_size() > 0) {
    slice_state_vec_.clear();
    slice_state_vec_.reserve(rpc_arg_->slices_size());
    for (int xx = 0; xx < rpc_arg_->slices_size(); ++xx) {
      slice_state_vec_.emplace_back(
        ExtentGroupState::SliceState::Create(&rpc_arg_->slices(xx), arena_));
    }
  }

  if (eg_state_) {
    // The owner op provided us with the extent group state. The owner must
    // have also acquired a lock on the extent group.
    ExtentGroupStateFetched(StargateError::kNoError, eg_state_);
    return;
  }

  // Acquire a write lock on the egroup if this is an extent based replica. We
  // will not be registering this read with the extent group state for such
  // replicas, so we need to ensure that there is no parallel write in
  // progress. The lock will be downgraded to a read lock once the metadata
  // lookup is done and the slice groups are locked, as subsequent writer will
  // block on the slice group lock from that point onwards. This allows
  // concurrent read replicas to proceed in parallel after the initial metadata
  // lookup is done. For regular egroup, we can take a read lock since the op
  // registration workflow will ensure that no writes are admitted while the
  // read is in progress.
  const int exclusive_lock =
    rpc_arg_->extent_based_metadata_format();

  if (disk_manager_->egroup_locker_.TryAcquire(
        extent_group_id_, exclusive_lock)) {
    lock_egid_ = extent_group_id_;
    egid_locks_held_ = true;
    ExtentGroupLocksAcquired();
    return;
  }

  if (rpc_arg_->extent_based_metadata_format() &&
      disk_manager_->shared_extent_write_state_->HasPendingCommit(
        extent_group_id_)) {
    disk_manager_->shared_extent_write_state_->ResolvePendingCommit(
        extent_group_id_);
  }

  // Acquire the lock asynchronously.
  RegisterExtentGroup(extent_group_id_, exclusive_lock);
  AcquireExtentGroupLocks();
}

//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReadReplicaOp::
ExtentGroupLocksAcquired() {
  TAGGED_DCHECK(fe_->IsWorkerThread());

  // Lookup the extent group state. If we don't find it in memory, we'll lookup
  // the physical state map in Medusa, unless the RPC arg has explicitly set
  // managed_by_aes to false for non-AES egroups.
  FetchExtentGroupState(
    extent_group_id_, false /* create_if_needed */,
    EgroupFormatSpec(managed_by_aes_ || aes_conversion_in_progress_,
                     extent_based_metadata_format_));
}

//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReadReplicaOp::
ExtentGroupStateFetched(const StargateError::Type err,
                        const ExtentGroupState::Ptr& eg_state) {

  TAGGED_DCHECK(fe_->IsWorkerThread());

  if (err != StargateError::kNoError) {
    FinishWithError(err);
    return;
  }

  if (!eg_state) {
    TAGGED_LOG(WARNING) << "Failing readop as the extent group does not exist";
    FinishWithError(StargateError::kExtentGroupNonExistent);
    return;
  }

  if (RpcHasTimedOut()) {
    // No need to continue doing work if the corresponding RPC has already
    // timed out.
    const char *msg = "Egroup state fetch took too much time, failing op";
    TRACE(at_, 0) << msg;
    TAGGED_LOG(ERROR) << msg;
    FinishWithError(StargateError::kRetry);
    return;
  }

  eg_state_ = eg_state;

  TAGGED_DCHECK(
    !extent_based_metadata_format_ ||
    (eg_state->base_state() &&
     eg_state->base_state()->ControlBlock()->extent_based_format()));

  if (eg_state->base_state() &&
      eg_state->base_state()->ControlBlock()->extent_based_format()) {
    TAGGED_DCHECK(extent_based_metadata_format_);
    disk_manager_->shared_extent_write_state_->PopulateEgState(eg_state.get());

    if (FLAGS_estore_experimental_read_replica_delay_after_lookup ==
        extent_group_id_) {
      TRACE(at_, 0) << "Delaying after doing a lookup";
      TAGGED_LOG(INFO) << "Delaying after metadata lookup for extent_based "
                       << "egroup";
      globals_->delay_ed->Add(BindInFe<void()>(
        fe_, &ExtentGroupReadReplicaOp::ExtentGroupStateFetched, this,
        err, eg_state), 2000);
      return;
    }

    // Acquire slice group locks on all the extents in the extent group.
    for (const auto& entry : eg_state->base_state()->slice_group_map()) {
      if (!entry.second) {
        continue;
      }

      // Grab a read lock on the extent.
      locked_slice_group_id_list_.EmplaceBack(
        SharedExtentWriteState::GetSliceGroupLockId(
          extent_group_id_, *entry.first.extent_id));

      TAGGED_VLOG(1) << "Acquiring lock on slice group: "
                     << locked_slice_group_id_list_.Back();
    }

    disk_manager_->shared_extent_write_state_->slice_group_locker_.Acquire(
      &locked_slice_group_id_list_.GetCtr(),
      nullptr /* write_ids */,
      BoundFunction<void()>::Create<
        ExtentGroupReadReplicaOp,
        &ExtentGroupReadReplicaOp::MaybeRegisterOp>(this));

    return;
  }

  // Set the checksum type.
  const ChecksumType checksum_type =
    eg_state_->crc32_checksums() ? ChecksumType::kCrc32c :
                                   ChecksumType::kAdler32;
  rpc_ret_->set_checksum_type(static_cast<int>(checksum_type));

  if (eg_state->managed_by_aes() != managed_by_aes_ &&
      !aes_conversion_in_progress_) {
    // The replica managed_by_aes state does not match the arg. Abort the op.
    const char *msg = "Unexpected extent group managed_by_aes state";
    TRACE(at_, 0) << msg;
    TAGGED_LOG(ERROR) << msg
                      << OUTVARS(eg_state->managed_by_aes(), managed_by_aes_);
    TAGGED_DCHECK(!managed_by_aes_);
    FinishWithError(StargateError::kStaleConfig);
    return;
  }

  // Fetch cipher_key for non extent based metadata.
  if (untransform_needed_ && !encryption_type_vec_.empty()) {
    cipher_key_id_ = rpc_arg_->cipher_key_id();
    FetchCipherKey(
      BindInFe<void(MantleErrorProto::Type)>(
        fe_, arena_, &ExtentGroupReadReplicaOp::FetchCipherKeyDone, this, _1));
    return;
  }

  // Let's now register this op with the extent group state. Once the
  // registration succeeds, no write ops will be let through - so we won't
  // conflict with them.
  eg_state_->RegisterReaderWriterOp(
    disk_manager_,
    BoundFunction<void()>::Create<
      ExtentGroupReadReplicaOp,
      &ExtentGroupReadReplicaOp::ReadOpRegistered>(this),
    false /* is_writer */);
}

void ExtentStore::DiskManager::ExtentGroupReadReplicaOp::
HandleExtentBasedMetadataFormat() {
  TAGGED_DCHECK(fe_->IsWorkerThread());
  TAGGED_DCHECK(extent_based_metadata_format_);

  if (egid_locks_held_) {
    // If we have acquired a lock on the egroup id at the start of this op,
    // downgrade it to a shared lock to allow other readers to proceed in
    // parallel.
    disk_manager_->egroup_locker_.Downgrade(extent_group_id_);

    // Although not required, move the egroup id out of
    // 'exclusive_lock_egid_list_' if present to avoid issues in any future
    // code changes.
    if (!exclusive_lock_egid_list_.empty()) {
      TAGGED_DCHECK(shared_lock_egid_list_.empty());
      TAGGED_DCHECK_EQ(exclusive_lock_egid_list_.size(), 1U);
      TAGGED_DCHECK_EQ(exclusive_lock_egid_list_.front(), extent_group_id_);
      lock_egid_ = extent_group_id_;
      exclusive_lock_egid_list_.clear();
    }
  }

  TAGGED_DCHECK(eg_state_);
  const auto& mvalue = eg_state_->base_state();

  // Fill the metadata in the RPC response. If the slice vec is provided in the
  // args, then we don't need anything more than egroup's control block.
  mvalue->FillExtentBasedMetadataEntry(
    disk_manager_->disk_id_, rpc_ret_->mutable_extent_group_metadata(),
    extent_group_id_, !slice_state_vec_.empty() /* control_block_only */);

  // Set the checksum type.
  rpc_ret_->set_checksum_type(static_cast<int>(ChecksumType::kCrc32c));

  const auto cb = &rpc_ret_->extent_group_metadata().control_block();

  // The latest intent sequence field in 'cb' is the highest global metadata
  // intent sequence among all the extents of the egroup. If that value is
  // higher than the global metadata intent sequence provided in the RPC, then
  // the RPC must be stale.
  TAGGED_DCHECK(!rpc_ || rpc_arg_->has_global_metadata_intent_sequence());
  const bool valid_global_intent_sequence =
    !rpc_arg_->has_global_metadata_intent_sequence() ||
    (cb->latest_intent_sequence() <=
       rpc_arg_->global_metadata_intent_sequence());
  const bool valid_expected_intent_sequence =
    rpc_arg_->expected_intent_sequence() < 0 ||
    cb->replicas(0).intent_sequence() == rpc_arg_->expected_intent_sequence();
  const bool rpc_valid =
    valid_global_intent_sequence && valid_expected_intent_sequence;

  if (!rpc_valid) {
    TAGGED_LOG(WARNING) << "Stale RPC - rpc expected intent sequence: "
                        << rpc_arg_->expected_intent_sequence()
                        << " rpc global intent sequence: "
                        << rpc_arg_->global_metadata_intent_sequence()
                        << " estore applied intent sequence: "
                        << cb->replicas(0).intent_sequence()
                        << " estore global intent sequence: "
                        << cb->latest_intent_sequence();

    FinishWithError(StargateError::kUnexpectedIntentSequence);
    return;
  }

  if (!slice_state_vec_.empty()) {
    // This is a either continuation RPC to fetch the next batch of slice data
    // from the egroup, or a call to fetch data for specific slices. So we will
    // directly proceed to read.
    MaybeFetchCipherKey(mvalue);
    return;
  }

  TAGGED_DCHECK_EQ(rpc_arg_->slices_size(), 0);
  slice_state_vec_.reserve(rpc_ret_->extent_group_metadata().slices_size());
  if (rpc_arg_->has_read_range()) {
    TAGGED_DCHECK(rpc_);
    TAGGED_DCHECK_EQ(next_slice_idx_, 0);
    ComposePartialSlicesToRead();
  } else {
    PopulateSliceStateVec();
  }

  ScopedArenaMark am;
  ArenaVector<pair<int32, int32>> allocated_bitmap_vec(am.arena());
  allocated_bitmap_vec.reserve(
    rpc_ret_->extent_group_metadata().slices_size());
  for (const auto& entry : mvalue->slice_group_map()) {
    if (!entry.second) {
      continue;
    }

    const auto slice_group = GetSliceStateGroupFB(entry.second->data());
    const auto extent_physical_state = slice_group->extent_physical_state();

    // In case this extent group is marked corrupt, finish the op right here.
    if (!rpc_arg_->ignore_corrupt_flag() &&
        extent_physical_state->is_corrupt()) {
      TAGGED_LOG(WARNING) << "Extent group is marked corrupt";

      rpc_ret_->Clear();
      FinishWithError(StargateError::kDataCorrupt);
      return;
    }

    if (extent_physical_state->needs_local_tentative_resolution()) {
      ReleaseLocks();
      slice_state_vec_.clear();
      next_slice_idx_ = 0;
      eg_state_.reset();
      rpc_ret_->Clear();

      // The replica requires local tentative resolution. This implies that the
      // caller issued this RPC without running the fixer op. Let's use
      // GetEgroupStateOp to drive the resolution, similar to what would have
      // happened if a fixer op was run. We will resume from the beginning once
      // the op completes.
      TRACE(at_, 0) << "Replica needs local tentative resolution";
      TAGGED_LOG(WARNING) << "Replica needs local tentative resolution";
      auto arg = make_unique<GetEgroupStateArg>();
      arg->set_extent_group_id(extent_group_id_);
      arg->set_managed_by_aes(true);
      arg->set_extent_based_metadata_format(true);
      arg->set_qos_priority(qos_priority_);
      arg->set_read_only(true);
      arg->set_intent_sequence(rpc_arg_->global_metadata_intent_sequence());
      auto ret = make_unique<GetEgroupStateRet>();
      auto op = make_shared<GetEgroupStateOp>(
        disk_manager_, nullptr /* rpc */, arg.get(), ret.get(),
        nullptr /* rpc_done_cb */);
      op->Start(
        Bind([this, arg = move(arg), ret = move(ret), op_ptr = op.get()]() {
               if (op_ptr->error() != StargateError::kNoError) {
                 TRACE(at_, 0) << "GetEgroupStateOp failed with error: "
                               << op_ptr->error();
                 TAGGED_LOG(WARNING) << "GetEgroupStateOp failed with error: "
                                     << op_ptr->error();
                 FinishWithError(op_ptr->error());
                 return;
               }

               StartImpl();
             }));
      return;
    }

    auto extent_physical_states_proto = rpc_ret_->add_extent_physical_states();
    extent_physical_states_proto->mutable_extent_id()->CopyFrom(
      *entry.first.extent_id->extent_id_proto());
    extent_physical_states_proto->set_largest_seen_intent_sequence(
      extent_physical_state->latest_intent_sequence());
    extent_physical_states_proto->set_last_mutator_incarnation_id(
      extent_physical_state->last_mutator_incarnation_id());
    extent_physical_states_proto->set_latest_applied_intent_sequence(
      extent_physical_state->applied_intent_sequence());
    if (!extent_physical_state->needs_tentative_resolution()) {
      extent_physical_states_proto->set_highest_committed_intent_sequence(
        extent_physical_state->applied_intent_sequence());
    }
    extent_physical_states_proto->set_mtime_secs(
      extent_physical_state->mtime_secs());
    const auto cb = mvalue->ControlBlock();
    const int untransformed_slice_length =
      extent_physical_state->untransformed_slice_length_blocks() *
      cb->block_size();
    const int first_slice_id =
      entry.first.extent_id ?
        slice_group->extent_index() * cb->slice_group_size() :
        entry.first.id;

    for (int ii = 0; ii < extent_physical_state->slice_states_size(); ++ii) {
      uint32 block_bitmap = 0;
      const PhysicalSliceFB& slice_fb =
        extent_physical_state->slice_states(ii);
      const int block_size_bytes =
        disk_manager_->shared_extent_write_state_->egroups_component()->
          block_size_bytes();
      const int transformed_length =
        slice_fb.transformed_length() > 0 ?
          slice_fb.transformed_length() : untransformed_slice_length;
      const int num_blocks =
        (transformed_length + block_size_bytes - 1) / block_size_bytes;
      TAGGED_DCHECK_LE(num_blocks, sizeof(block_bitmap) * 8);
      for (int block_idx = 0; block_idx < num_blocks; ++block_idx) {
        const int64 block_num =
          disk_manager_->shared_extent_write_state_->GetBlockNum(slice_fb,
                                                                 block_idx);
        if (block_num > 0) {
          block_bitmap |= (1 << block_idx);
        }
      }
      allocated_bitmap_vec.emplace_back(first_slice_id + ii, block_bitmap);
    }
  }

  if (!active_op_state_map_.empty()) {
    // There shouldn't be any outstanding TUs on the slice groups.
    for (const auto& entry : active_op_state_map_) {
      TAGGED_DCHECK(entry.second->tu_state.tu_list.empty())
        << OUTVARS(entry.first, entry.second->ToString());
    }
  }
  rpc_ret_->set_is_erasure_coded(eg_state_->IsErasureCodedAESEgroup());

  // Sort the 'allocated_bitmap_vec' in the order of slice_id. The bitmap
  // for the slices should be in the same order as slices in the egid metadata
  // returned by this RPC.
  sort(allocated_bitmap_vec.begin(), allocated_bitmap_vec.end());
  for (const auto& iter : allocated_bitmap_vec) {
    rpc_ret_->add_allocated_block_bitmaps(iter.second);
  }
  TAGGED_DCHECK_EQ(rpc_ret_->allocated_block_bitmaps_size(),
                   rpc_ret_->extent_group_metadata().slices_size());

  if (!read_complete_egroup_) {
    // Resize 'slice_state_vec_' to the first work unit chunk that we need to
    // read from the disk.
    int32 work_unit_bytes = 0;
    int32 work_unit_count = 0;
    for (size_t ii = 0; ii < slice_state_vec_.size() &&
           work_unit_bytes < FLAGS_estore_replication_work_unit_bytes; ++ii) {
      TAGGED_CHECK(slice_state_vec_[ii]->has_transformed_length());
      work_unit_bytes += slice_state_vec_[ii]->transformed_length();
      ++work_unit_count;
    }
    slice_state_vec_.resize(work_unit_count);
  }

  if (slice_state_vec_.empty()) {
    if (rpc_) {
      rpc_->set_response_payload(move(egroup_data_));
    }
    Finish();
    return;
  }

  MaybeFetchCipherKey(mvalue);
}

//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReadReplicaOp::MaybeRegisterOp() {
  TAGGED_DCHECK(fe_->IsWorkerThread());
  TAGGED_DCHECK(extent_based_metadata_format_);

  SharedExtentWriteState *const shared_state =
    disk_manager_->shared_extent_write_state_.get();

  if (shared_state->active_op_map_.empty()) {
    HandleExtentBasedMetadataFormat();
    return;
  }

  BlockStoreUtil::TaskAggregatorNoError ta(
    BoundFunction<void()>::Create<
      ExtentGroupReadReplicaOp,
      &ExtentGroupReadReplicaOp::HandleExtentBasedMetadataFormat>(this),
    arena_);

  for (const auto& id : locked_slice_group_id_list_) {
    auto it = shared_state->active_op_map_.find(id);
    if (it == shared_state->active_op_map_.end()) {
      // Registration not required for this slice group.
      continue;
    }

    if (shared_state->TryRegisterOp(
          id, false /* is_writer */, 0 /* write_offset */, -1 /* write_len */,
          at(), &it->second)) {
      active_op_state_map_[id] = &it->second;
      continue;
    }

    // Register asynchronously.
    shared_state->RegisterOp(
      id, false /* is_writer */,
      FuncDisabled<void(SharedExtentWriteState::ActiveOpState *)>(
        [this, id, ta](SharedExtentWriteState::ActiveOpState *const state) {
          active_op_state_map_[id] = state;
        }), 0 /* write_offset */, -1 /* write_len */, at());
  }
}

//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReadReplicaOp::MaybeFetchCipherKey(
  const MedusaExtentGroupPhysicalStateEntry::PtrConst& mvalue) {

  // Fetch the cipher key id for decryption if needed.
  if (mvalue->ControlBlock()->cipher_key_id_valid()) {
    cipher_key_id_ =
      string(reinterpret_cast<const char *>(
               mvalue->ControlBlock()->cipher_key_id()->data()),
             mvalue->ControlBlock()->cipher_key_id_size());
    FetchCipherKey(
      BindInFe<void(MantleErrorProto::Type)>(
        fe_, arena_, &ExtentGroupReadReplicaOp::FetchCipherKeyDone, this, _1));
    return;
  }

  FetchEgroupDescriptor();
}

void ExtentStore::DiskManager::ExtentGroupReadReplicaOp::
PopulateSliceStateVec() {
  // We will enter this method with a non-zero 'next_slice_idx_' only when
  // reading the complete egroup, in which case we make multiple passes through
  // the op working on 'FLAGS_estore_replication_work_unit_bytes' at a time.
  TAGGED_DCHECK(read_complete_egroup_ || next_slice_idx_ == 0)
    << OUTVARS(read_complete_egroup_, next_slice_idx_);

  int32 work_unit_bytes = 0;

  // When not reading the complete egroup, we will populate 'slice_state_vec_'
  // with all the slices in the egroup metadata because they need to be sorted
  // by extent group offset before we select the slices that compose a work
  // unit. When reading the entire egroup, we will just follow the order of the
  // slices in the extent group metadata regardless of the extent group offset.
  for (; next_slice_idx_ < rpc_ret_->extent_group_metadata().slices_size() &&
         (!read_complete_egroup_ ||
          work_unit_bytes < FLAGS_estore_replication_work_unit_bytes);
         ++next_slice_idx_) {
    const auto& ss =
      rpc_ret_->extent_group_metadata().slices(next_slice_idx_);
    if (ss.has_extent_group_offset()) {
      work_unit_bytes += ss.transformed_length();
      slice_state_vec_.emplace_back(
        ExtentGroupState::SliceState::Create(ss, true /* compact */, arena_));
    }
  }

  if (read_complete_egroup_) {
    // When reading the complete egroup, the caller does not expect us to sort
    // the slices by the extent group offset. If the requirement for sorting
    // comes up in the future, the sorting needs to happen at the end once all
    // slices have been accumulated in 'all_slices_state_vec_'.
    return;
  }

  // Sort slice state vector in increasing order of extent group offset.
  sort(slice_state_vec_.begin(), slice_state_vec_.end(),
       ExtentGroupState::SliceState::SliceStateUPtrOrderIncreasingFileOffset);
}

//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReadReplicaOp::FetchCipherKeyDone(
  const MantleErrorProto::Type error) {

  TAGGED_DCHECK(fe_->IsWorkerThread());

  // Unable to fetch the cipher key, abort the op.
  if (error != MantleErrorProto::kNoError) {
    error_ = StargateError::kCipherKeyFetchError;
    TAGGED_LOG(ERROR) << "Finishing op with error " << error_
                      << " due to cipher key fetch failure with error "
                      << error_ << " for key id: " << cipher_key_id_;
    FinishWithError(error_);
    return;
  }

  if (untransform_needed_) {
    if (!extent_based_metadata_format_) {
      // Let's now register this op with the extent group state. Once the
      // registration succeeds, no write ops will be let through - so we won't
      // conflict with them.
      eg_state_->RegisterReaderWriterOp(
        disk_manager_,
        BoundFunction<void()>::Create<
          ExtentGroupReadReplicaOp,
          &ExtentGroupReadReplicaOp::ReadOpRegistered>(this),
        false /* is_writer */);
      return;
    }
    // We should have encryption info extracted already.
    TAGGED_DCHECK(!encryption_type_vec_.empty());
    FetchEgroupDescriptor();
    return;
  }

  // Extract the encryption information from the metadata.
  TAGGED_DCHECK(eg_state_);
  TAGGED_DCHECK(eg_state_->base_state());

  if (!encryption_type_vec_.empty()) {
    // The encryption type is already populated from the RPC arg.
    FetchEgroupDescriptor();
    return;
  }

  for (const auto& entry : eg_state_->base_state()->slice_group_map()) {
    if (!entry.second) {
      continue;
    }

    const auto slice_group = GetSliceStateGroupFB(entry.second->data());
    const auto extent_physical_state = slice_group->extent_physical_state();

    for (int ii = 0;
         ii < extent_physical_state->transformation_type_vec_size(); ++ii) {
      const int transformation_type =
        extent_physical_state->transformation_type_vec(ii);
      if (transformation_type == 0) {
        break;
      }
      TAGGED_DCHECK(DataTransformationType::IsValidTransformationType(
        transformation_type)) << OUTVARS(transformation_type);
      const DataTransformation::Type transform_type =
        static_cast<DataTransformation::Type>(transformation_type);

      if (DataTransformationType::IsEncryptionType(transform_type)) {
        encryption_type_vec_.emplace_back(transform_type);
      }
    }
    break;
  }

  TAGGED_DCHECK(!encryption_type_vec_.empty());

  FetchEgroupDescriptor();
}

//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReadReplicaOp::ReadOpRegistered() {
  TAGGED_DCHECK(fe_->IsWorkerThread());

  TRACE(at_, 0) << "Read op registered";
  registered_reader_op_ = true;

  // In case this extent group is marked corrupt, finish the op right here.
  TAGGED_CHECK(eg_state_);
  if (!rpc_arg_->ignore_corrupt_flag() && eg_state_->is_corrupt()) {
    TAGGED_LOG(WARNING) << "Extent group is marked corrupt";

    FinishWithError(StargateError::kDataCorrupt);
    return;
  }

  // There shouldn't be any tentative update left as the writers are all
  // blocked out. The only exception is when an egroup is marked corrupt, we
  // might still have tentative updates as we do not throw away the updates in
  // flight.
  TAGGED_CHECK(!eg_state_->HasTentativeUpdate() || eg_state_->is_corrupt());

  TAGGED_CHECK(rpc_arg_->expected_intent_sequence() >= 0 ||
               managed_by_aes_) << rpc_arg_->ShortDebugString();

  // For extent groups managed by AES it is okay for extent group's global
  // metadata intent sequence to fall behind the latest intent sequence present
  // in ExtentGroupIdMap's control block. However, expected intent sequence
  // must match with the latest applied intent sequence, if provided. This is
  // set if the caller is issuing multiple chunkified read requests at a
  // specific applied intent sequence. This ensures further reads are denied
  // if the egroup has mutated in the interim after the first read was
  // serviced.
  bool rpc_valid = true;
  if (managed_by_aes_) {
    const bool valid_global_intent_sequence =
      rpc_arg_->has_global_metadata_intent_sequence() &&
      (eg_state_->global_metadata_intent_sequence() <=
        rpc_arg_->global_metadata_intent_sequence());
    const bool valid_expected_intent_sequence =
      (rpc_arg_->expected_intent_sequence() < 0 ||
       (eg_state_->latest_applied_intent_sequence() ==
         rpc_arg_->expected_intent_sequence()));
    rpc_valid =
      valid_global_intent_sequence && valid_expected_intent_sequence;
  } else {
    rpc_valid =
      (eg_state_->latest_applied_intent_sequence() ==
        rpc_arg_->expected_intent_sequence());
  }
  if (!rpc_valid) {
    TAGGED_LOG(WARNING) << "Stale RPC - rpc expected intent sequence: "
                        << rpc_arg_->expected_intent_sequence()
                        << " rpc global intent sequence: "
                        << rpc_arg_->global_metadata_intent_sequence()
                        << " estore applied intent sequence: "
                        << eg_state_->latest_applied_intent_sequence()
                        << " estore global intent sequence: "
                        << eg_state_->global_metadata_intent_sequence()
                        << " managed by aes: "
                        << managed_by_aes_;

    FinishWithError(StargateError::kUnexpectedIntentSequence);
    return;
  }

  // Since the request to read a replica comes after consulting the metadata
  // in Medusa, this means that Medusa's metadata has sync'd up with us.
  // This is not applicable to extent groups managed by AES as the physical
  // metadata will be synced with the physical state map in Medusa via the
  // checkpoint process.
  // TODO(harshit): Currently we don't have a field in 'eg_state_' that tells
  // us upto what intent sequence we have synced with the backend. We get to
  // know that whenever we fetch the eg_state from Medusa. So we can have this
  // done in that location whenever a backend lookup happens and therefore
  // handle this for AES extent groups.
  if (!eg_state_->managed_by_aes() && eg_state_->HasUnsyncedAppliedUpdate()) {
    // Log an update to indicate that the metadata has sync'd with Medusa
    // up to rpc_arg_->expected_intent_sequence().
    ScopedArenaMark am;
    DiskWALRecord::Ptr record = DiskWALRecord::Create(am.arena());
    auto egroup_update = record->add_egroup_update();
    egroup_update->set_extent_group_id(extent_group_id_);
    egroup_update->set_synced_intent_sequence(
      rpc_arg_->expected_intent_sequence());
    record->Finish();

    disk_manager_->disk_wal_->LogDeltaRecordAsync(record.get());
  }

  if (slice_state_vec_.empty()) {
    // We need to fetch the extent group metadata before proceeding for AES
    // managed extent groups.
    TAGGED_DCHECK(managed_by_aes_);

     TAGGED_VLOG(1) << "Fetching extent group physical state from Medusa";
    TRACE(at_, 0) << "Looking up Medusa to fetch extent group physical state";

    if (!TryLockAESDB()) {
      LookupExtentGroupPhysicalStateDone(
        MedusaError::kRetry,
        -1 /* medusa_err_src_id */,
        MedusaValue::PtrConst());
      return;
    }

    Function<void(MedusaError::Type, int, MedusaValue::PtrConst&&)>
      lookup_done_cb =
        bind(&ExtentGroupReadReplicaOp::LookupExtentGroupPhysicalStateDone,
             this, _1, _2, _3);
    globals_->medusa->LookupExtentGroupPhysicalState(
      aesdb_locker_.DB(),
      extent_group_id_,
      disk_manager_->CreateAESDBCallback(
        move(lookup_done_cb), AESDBManager::AESDBOp::kReadOp),
      disk_manager_->perf_stat_->medusa_call_stats(),
      true /* fetch_all */, true /* cached_ok */,
      false /* cache_values_read */);
    return;
  }

  // We have the slice information. Proceed to fetch the extent group
  // descriptor to start the read.
  FetchEgroupDescriptor();
}

//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReadReplicaOp::
LookupExtentGroupPhysicalStateDone(
  const MedusaError::Type error,
  const int medusa_err_src_id,
  MedusaValue::PtrConst&& mval) {

  TAGGED_DCHECK(fe_->IsWorkerThread());
  TAGGED_DCHECK(managed_by_aes_);
  TAGGED_DCHECK(eg_state_);
  TAGGED_DCHECK(!aes_conversion_in_progress_);

  if (FLAGS_estore_experimental_read_replica_delay_after_lookup ==
      extent_group_id_) {
    TRACE(at_, 0) << "Delaying after doing a lookup";
    const bool physical_entry_found =
      (mval && mval->extent_group_physical_state_entry);
    TAGGED_LOG(INFO) << "Delaying after doing a lookup, medusa entry found: "
                     << boolalpha << physical_entry_found;
    globals_->delay_ed->Add(BindInFe<void()>(
      fe_, &ExtentGroupReadReplicaOp::LookupExtentGroupPhysicalStateDone, this,
      error, medusa_err_src_id, bind_move(mval)), 2000);
    return;
  }

  TRACE(at_, 0) << "Medusa lookup completed";

  if (error != MedusaError::kNoError) {
    TAGGED_LOG(ERROR) << "Error:" << error
                      << ", medusa_err_src_id: " << medusa_err_src_id
                      << " when quering Medusa";
    FinishWithError(StargateError::kMetadataLookupFailure);
    return;
  }

  if (RpcHasTimedOut()) {
    // No need to continue doing work if the corresponding RPC has already
    // timed out.
    const char *msg = "Physical state lookup took too much time, failing op";
    TRACE(at_, 0) << msg;
    TAGGED_LOG(ERROR) << msg;
    FinishWithError(StargateError::kRetry);
    return;
  }

  TAGGED_DCHECK(mval);
  MedusaExtentGroupPhysicalStateEntry::PtrConst entry =
    eg_state_->base_state();
  if (mval->extent_group_physical_state_entry) {
    if (VLOG_IS_ON(2)) {
      stringstream ss;
      ss << "Updating egroup physical state for egroup id " << extent_group_id_
         << " to ";
      MedusaPrinterUtil::PrintExtentGroupPhysicalStateEntry(
        &ss, mval, VLOG_IS_ON(4));
      TAGGED_LOG(INFO) << ss.str();
    }
    // Apply the state fetched from Medusa to eg_state and refresh entry.
    TAGGED_DCHECK(mval->extent_group_physical_state_entry->control_block());
    eg_state_->ApplyBaseState(
      extent_group_id_, mval->extent_group_physical_state_entry);
    BinaryLogExtentGroupState(extent_group_id_, eg_state_);
    entry = eg_state_->base_state();
  }

  // eg_state_ now contains the entire metadata of this egroup. Before we
  // proceed to fill the response, check if there is any unsynced applied
  // update in the ephemeral state and merge it with the medusa entry.
  if (eg_state_->HasUnsyncedControlBlockUpdate(extent_group_id_)) {
    TRACE(at_, 0) << "Merging metadata from ephemeral state";
    entry = CreateMergedPhysicalStateEntry(
      extent_group_id_, eg_state_, true /* merge_all */).first;
  }

  TAGGED_DCHECK(entry);
  if (!entry) {
    TAGGED_LOG(ERROR) << "Metadata lookup failed";
    FinishWithError(StargateError::kMetadataLookupFailure);
    return;
  }

  // Fill the metadata in the RPC response.
  entry->FillExtentGroupIdMapEntry(
    disk_manager_->disk_id_, rpc_ret_->mutable_extent_group_metadata());

  rpc_ret_->set_largest_seen_intent_sequence(
    eg_state_->largest_seen_intent_sequence());
  rpc_ret_->set_last_mutator_incarnation_id(
    eg_state_->last_mutator_incarnation_id());
  rpc_ret_->set_highest_committed_intent_sequence(
    eg_state_->highest_committed_intent_sequence());
  rpc_ret_->set_is_erasure_coded(
    eg_state_->IsErasureCodedAESEgroup());

  slice_state_vec_.reserve(rpc_ret_->extent_group_metadata().slices_size());
  if (rpc_arg_->has_read_range()) {
    TAGGED_DCHECK_EQ(next_slice_idx_, 0);
    ComposePartialSlicesToRead();
  } else {
    PopulateSliceStateVec();
  }

  if (!read_complete_egroup_) {
    // Resize 'slice_state_vec_' to the first work unit chunk that we need to
    // read from the disk.
    int32 work_unit_bytes = 0;
    int32 work_unit_count = 0;
    for (size_t ii = 0; ii < slice_state_vec_.size() &&
           work_unit_bytes < FLAGS_estore_replication_work_unit_bytes; ++ii) {
      TAGGED_CHECK(slice_state_vec_[ii]->has_transformed_length());
      work_unit_bytes += slice_state_vec_[ii]->transformed_length();
      ++work_unit_count;
    }
    slice_state_vec_.resize(work_unit_count);
  }

  if (eg_state_->IsErasureCodedParityAESEgroup()) {
    TAGGED_DCHECK_EQ(eg_state_->ec_info()->global_info_egroup_id_vec.size(),
                     eg_state_->ec_info()->local_info_egroup_id_vec.size());
    StartInfoEgroupHelperOp();
    return;
  }

  if (slice_state_vec_.empty()) {
    // This can happen if we are asked to read a range that is not
    // existent in the egroup file.
    TAGGED_VLOG(3) << "No slices found to read, arg "
                   << rpc_arg_->ShortDebugString();
    FinishWithError(StargateError::kNoError);
    return;
  }

  // Now that we have fetched the extent group metadata and populated
  // 'slice_state_vec_', we can proceed to read the extent group.
  FetchEgroupDescriptor();
}

//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReadReplicaOp::
ComposePartialSlicesToRead() {
  // This is arising due to a decode request for the EC strip of this
  // egroup. Proceed to determine the slices to be read based on the
  // given range.
  TAGGED_DCHECK(extent_based_metadata_format_ ||
                eg_state_->IsErasureCodedAESEgroup())
    << OUTVARS(extent_based_metadata_format_,
               eg_state_->IsErasureCodedAESEgroup());
  if (google::DEBUG_MODE && extent_based_metadata_format_) {
    for (const auto& entry : eg_state_->base_state()->slice_group_map()) {
      if (!entry.second) {
        continue;
      }

      const auto slice_group = GetSliceStateGroupFB(entry.second->data());
      TAGGED_DCHECK(slice_group->extent_physical_state()->is_ec_info_egroup());
    }
  }

  TAGGED_DCHECK(rpc_arg_->read_range().has_offset() &&
                rpc_arg_->read_range().has_length())
    << rpc_arg_->ShortDebugString();
  TAGGED_DCHECK(rpc_ret_->has_extent_group_metadata());

  // Determine the slices belonging to the read range specified in the RPC.
  pair<int64, int64> read_range(
    rpc_arg_->read_range().offset(),
    rpc_arg_->read_range().length());
  MedusaExtentGroupIdMapEntry egid_entry(&rpc_ret_->extent_group_metadata());
  StargateUtil::FindAllocatedSlicesInRange(
    extent_group_id_, egid_entry, &read_range, arena_,
    nullptr /* remote_arg */, rpc_ret_);

  // Compose 'slice_state_vec_' based on the slices belonging to the given
  // range.
  for (int ii = 0; ii < rpc_ret_->slices_size(); ++ii) {
    slice_state_vec_.emplace_back(
      ExtentGroupState::SliceState::Create(
        rpc_ret_->slices().Get(ii), true /* compact */, arena_));

    TAGGED_VLOG(3) << "Include partial read slice "
                   << rpc_ret_->slices().Get(ii).extent_group_offset() << "/"
                   << rpc_ret_->slices().Get(ii).transformed_length() << "/"
                   << rpc_ret_->slices().Get(ii).cushion() << " range "
                   << read_range.first << "," << read_range.second;
  }

  // Sort slice state vector in increasing order of extent group offset.
  sort(slice_state_vec_.begin(), slice_state_vec_.end(),
       ExtentGroupState::SliceState::SliceStateUPtrOrderIncreasingFileOffset);
}


//-----------------------------------------------------------------------------

void
ExtentStore::DiskManager::ExtentGroupReadReplicaOp::StartInfoEgroupHelperOp() {
  vector<int64> local_info_egroup_id_vec(
    eg_state_->ec_info()->local_info_egroup_id_vec);
  TAGGED_DCHECK(!local_info_egroup_id_vec.empty()) << eg_state_->ToString();

  auto progress_cb = BindInFe<void(StargateError::Type)>(
    fe_, &ExtentGroupReadReplicaOp::InfoEgroupLookupDone, this, _1);

  // Instantiate the helper op for looking up the physical state metadata of
  // the info egroups. Note that we will grab a shared lock on the info egroup
  // state so that if this RPC was issued due to a partial decode operation on
  // the same replica, then egroup_read_op would have already grabbed a shared
  // lock on this egroup already, thereby allowing the read replica op to grab
  // the same shared lock again.
  info_egroup_helper_op_ =
    make_shared<ExtentGroupStateHelperOp>(
      disk_manager_, move(local_info_egroup_id_vec), move(progress_cb),
      extent_group_id_, operation_id_,
      qos_priority(), classification_id(), false /* exclusive lock */);
  const string msg = StringJoin(
    "Fetching the extent group states of ",
    eg_state_->ec_info()->local_info_egroup_id_vec.size(), " info egroups");
  TAGGED_VLOG(1) << msg;
  TRACE(at_, 0) << msg;

  info_egroup_helper_op_->Start();
}

//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReadReplicaOp::InfoEgroupLookupDone(
  const StargateError::Type error) {

  if (error != StargateError::kNoError) {
    TAGGED_LOG(ERROR) << "Aborting the read op as the helper op failed to "
                      << "acquire locks for the info egroups, error "
                      << error;
    TRACE(at_, 0) << "Incurred error during info egroup lookup " << error;
    FinishWithError(StargateError::kRetry);
    return;
  }

  const auto& error_vec = info_egroup_helper_op_->error_vec();
  const auto& extent_group_id_vec =
    info_egroup_helper_op_->extent_group_id_vec();
  const auto& eg_state_vec = info_egroup_helper_op_->eg_state_vec();
  TAGGED_DCHECK_EQ(error_vec.size(), extent_group_id_vec.size());
  TAGGED_DCHECK_EQ(error_vec.size(), eg_state_vec.size());

  // Check if we hit any medusa failure during the info egroup physical
  // metadata lookup.
  for (uint ii = 0; ii < error_vec.size(); ++ii) {
    const auto& err = error_vec[ii];
    const int64 local_info_egroup_id = extent_group_id_vec[ii];
    if (err != MedusaError::kNoError) {
      TAGGED_LOG(ERROR)
        << "Received error while processing the info extent group "
        << local_info_egroup_id << " error " << err;
      FinishWithError(StargateError::kMetadataLookupFailure);
      return;
    }

    const auto& eg_state = eg_state_vec[ii];
    TAGGED_CHECK(eg_state) << local_info_egroup_id;
    TAGGED_CHECK(eg_state->IsErasureCodedInfoAESEgroupMetadataReplica())
      << local_info_egroup_id << " eg_state " << eg_state->ToString()
      << " parity eg_state " << eg_state_->ToString();

    ComposeInfoEgroupResponseMetadata(eg_state, local_info_egroup_id, ii);
  }
  TAGGED_DCHECK_EQ(rpc_ret_->info_egroups_size(),
                   eg_state_->ec_info()->local_info_egroup_id_vec.size());

  const string msg = StringJoin(
    "Lookup complete for ", extent_group_id_vec.size(),
    " info egroups");
  TAGGED_VLOG(3) << msg;
  TRACE(at_, 0) << msg;

  // Now that we have copied all the info egroup metadata, proceed to
  // fetch the egroup file descriptor if there are slices to be read.
  if (slice_state_vec_.empty()) {
    TAGGED_VLOG(3) << "No slices found to read, arg "
                   << rpc_arg_->ShortDebugString();
    FinishWithError(StargateError::kNoError);
    return;
  }

  // We have the info egroups' metadata. Proceed to fetch the extent group
  // descriptor to start the read.
  FetchEgroupDescriptor();
}

//-----------------------------------------------------------------------------

void
ExtentStore::DiskManager::ExtentGroupReadReplicaOp::
ComposeInfoEgroupResponseMetadata(
  const ExtentGroupState::PtrConst& info_eg_state,
  const int64 local_info_egroup_id,
  const int32 index) {

  TAGGED_DCHECK(info_eg_state) << local_info_egroup_id;

  // Create the merged entry for this info egroup. Note that the helper op
  // would have already applied any base state from the AESDB for this egroup.
  MedusaExtentGroupPhysicalStateEntry::PtrConst entry =
    info_eg_state->base_state();
  if (info_eg_state->HasUnsyncedControlBlockUpdate(local_info_egroup_id)) {
    TRACE(at_, 0) << "Merging metadata from ephemeral state";
    entry = CreateMergedPhysicalStateEntry(
      extent_group_id_, info_eg_state, true /* merge_all */).first;
  }
  TAGGED_DCHECK(entry);

  // Ensure that the info egroup metadata is added at the right index,
  // based on its position in the parity egroup's EC info.
  const auto& parity_eg_state = eg_state_;
  TAGGED_DCHECK_EQ(
    parity_eg_state->ec_info()->local_info_egroup_id_vec.at(index),
    local_info_egroup_id) << index;

  // Fill the metadata in the RPC response for the global info egroup.
  auto *const info_egroup = rpc_ret_->add_info_egroups();
  const int64 global_info_egroup_id =
    parity_eg_state->ec_info()->global_info_egroup_id_vec.at(index);
  info_egroup->set_extent_group_id(global_info_egroup_id);
  entry->FillExtentGroupIdMapEntry(
    disk_manager_->disk_id_, info_egroup->mutable_extent_group_metadata());
  info_egroup->set_vdisk_incarnation_id(
    info_eg_state->last_mutator_incarnation_id());
  info_egroup->set_highest_committed_intent_sequence(
    info_eg_state->highest_committed_intent_sequence());
  info_egroup->set_largest_seen_intent_sequence(
    info_eg_state->largest_seen_intent_sequence());
  const ChecksumType checksum_type =
    info_eg_state->crc32_checksums() ? ChecksumType::kCrc32c :
                                       ChecksumType::kAdler32;
  info_egroup->set_checksum_type(static_cast<int>(checksum_type));
}

//-----------------------------------------------------------------------------

void
ExtentStore::DiskManager::ExtentGroupReadReplicaOp::FetchEgroupDescriptor() {
  if (FLAGS_estore_aio_enabled) {
    if (TryGetEgroupDescriptor(extent_group_id_)) {
      IssueDiskRead();
      return;
    }
  }
  auto cont_cb = BindInFe<void()>(
    fe_, &ExtentGroupReadReplicaOp::IssueDiskRead, this);
  GetEgroupDescriptor(extent_group_id_, move(cont_cb));
}

//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReadReplicaOp::IssueDiskRead() {
  TAGGED_DCHECK(fe_->IsWorkerThread());
  TAGGED_DCHECK(!slice_state_vec_.empty());

  // Check if we were able to successfully open the extent group file.
  if (!desc_) {
    const StargateError::Type error =
      mark_egroup_corrupt_ ? StargateError::kDataCorrupt :
      StargateError::kRetry;
    TAGGED_LOG(INFO) << "Failed to open the extent group with " << error;
    if (mark_egroup_corrupt_) {
      // The descriptor should always be valid when using extent based metadata
      // format.
      TAGGED_DCHECK(!extent_based_metadata_format_);

      disk_manager_->disk_wal_->LogExtentGroupCorruptAsync(
        extent_group_id_, eg_state_->FormatSpec(), kEgroupOpenFailed);
    }
    FinishWithError(error);
    return;
  }
  if (FLAGS_estore_aio_enabled) {
    LOG(INFO)<<"FLAGS_unreadable_egroup"<<FLAGS_unreadable_egroup;
    LOG(INFO)<<"/"<<extent_group_id_;
    
    if (FLAGS_allow_partial_unreadable_egroups && (FLAGS_unreadable_egroup==extent_group_id_))
      AIODiskReadDataPerSlice();
    else
      AIODiskReadData();

  } else {
    disk_manager_->ExtentGroupDiskThread(extent_group_id_)->Add(
      BoundFunction<void()>::Create<
        ExtentGroupReadReplicaOp,
        &ExtentGroupReadReplicaOp::DiskReadData>(this));
  }
}

//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReadReplicaOp::DiskReadData() {
  TAGGED_DCHECK(
    disk_manager_->ExtentGroupDiskThread(extent_group_id_)->IsWorkerThread());
  TAGGED_CHECK(desc_);

  shared_ptr<vector<pair<int64, int64>>> offset_count_vec =
    make_shared<vector<pair<int64, int64>>>();
  offset_count_vec->reserve(slice_state_vec_.size());
  for (size_t xx = 0; xx < slice_state_vec_.size(); ++xx) {
    // Read data for this slice.
    const ExtentGroupState::SliceState::UPtrConst& slice_state =
      slice_state_vec_[xx];
    TAGGED_CHECK(slice_state->has_extent_group_offset());
    TAGGED_CHECK(slice_state->has_transformed_length());
    offset_count_vec->emplace_back(slice_state->extent_group_offset(),
                                   slice_state->transformed_length());
  }

  const int64 start_time_usecs = WallTime::NowUsecs();
  Descriptor::ReadVecCallback done_cb =
    Bind(&ExtentGroupReadReplicaOp::DiskReadVecDataDone,
         this, _1, _2, start_time_usecs);
  disk_manager_->ExtentGroupDiskThread(extent_group_id_)->Wrap(&done_cb);
  desc_->ReadVec(0 /* source_id */, offset_count_vec, done_cb);
}

//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReadReplicaOp::DiskReadVecDataDone(
  vector<IOBuffer::Ptr>&& iobuf_vec,
  const int read_errno,
  const int64 start_time_usecs) {

  TAGGED_DCHECK_EQ(iobuf_vec.size(), slice_state_vec_.size());
  int32 total_read_bytes = 0;
  for (size_t ii = 0; ii < iobuf_vec.size(); ++ii) {
    const ExtentGroupState::SliceState::UPtrConst& slice_state =
      slice_state_vec_[ii];
    const int egroup_offset = slice_state->extent_group_offset();
    const int32 slice_size = slice_state->transformed_length();
    IOBuffer::Ptr iobuf = move(iobuf_vec[ii]);
    if (!iobuf) {
      TAGGED_LOG(ERROR) << "Unable to read " << slice_size << " bytes of data"
                        << " from offset " << egroup_offset
                        << " of extent group file"
                        << ExtentStore::GetErrorDetails(read_errno);
    } else if (iobuf->size() < slice_size) {
      TAGGED_LOG(ERROR) << "Read only " << iobuf->size() << " bytes when "
                        << "attempting to read " << slice_size << " bytes of "
                        << "data from offset " << egroup_offset
                        << " of extent group file";
    } else {
      TAGGED_CHECK_EQ(iobuf->size(), slice_size);
    }

    if (!iobuf || iobuf->size() != slice_size) {
      const int64 diff_time_usecs = WallTime::NowUsecs() - start_time_usecs;
      ReportDiskOpCompleted(true /* is_read */,
                            true /* error */,
                            total_read_bytes,
                            diff_time_usecs);
      // TODO (myang): consider copy corrupt replica "as is" when
      // rpc_->copy_corrupt_replica() is true.
      Function<void()> func =
        Bind(&ExtentGroupReadReplicaOp::DiskReadDataDone,
             this,
             StargateError::kDataCorrupt);
      FunctionExecutor *const saved_fe = fe_;
      DeactivateFuncDisablerOutermost();
      saved_fe->Add(move(func));
      return;
    }

    total_read_bytes += iobuf->size();
    slice_data_map_[slice_state->slice_id()] = move(iobuf);
  }

  const int64 diff_time_usecs = WallTime::NowUsecs() - start_time_usecs;
  TAGGED_LOG_IF(INFO,
                FLAGS_estore_disk_access_threshold_msecs > 0 &&
                diff_time_usecs >
                  FLAGS_estore_disk_access_threshold_msecs * 1000)
    << "Disk read took " << diff_time_usecs/1000 << " msecs";

  ReportDiskOpCompleted(true /* is_read */,
                        false /* error */,
                        total_read_bytes,
                        diff_time_usecs);

  Function<void()> func =
    Bind(&ExtentGroupReadReplicaOp::DiskReadDataDone,
         this, StargateError::kNoError);
  FunctionExecutor *const saved_fe = fe_;
  DeactivateFuncDisablerOutermost();
  saved_fe->Add(move(func));
}

//-----------------------------------------------------------------------------

bool is_unreadable(interface::ReplicaReadEgroupRet *rpc_ret_ ,unsigned int val){

    for (int ii = 0; ii < rpc_ret_->corrupt_slices_size(); ++ii) {
      LOG(INFO)<<"checking if "<<rpc_ret_->corrupt_slices().Get(ii)<<"=="<<val;
        if(rpc_ret_->corrupt_slices().Get(ii)==val)
          {

          LOG(INFO)<<"slice id"<<val<<" unreadable, skipping checksum";
            return true;
          }
    }
    LOG(INFO)<<"slice id"<<val<<" not found in corrupt_slices";
    return false;

}

//------------------------------------------------------------------------------
void ExtentStore::DiskManager::ExtentGroupReadReplicaOp::DiskReadDataDone(
  const StargateError::Type err) {

  TAGGED_DCHECK(fe_->IsWorkerThread());

  if (err != StargateError::kNoError) {
    TAGGED_LOG(ERROR) << "Finishing read replica op with error "
                      << StargateError::ToString(err);

    // We'll issue a log entry to mark this extent group as corrupt.
    if (eg_state_) {
      if (extent_based_metadata_format_) {
        disk_manager_->shared_extent_write_state_->LogExtentGroupCorrupt(
          extent_group_id_, eg_state_.get(), kDiskReadFailed,
          [this, err]() { FinishWithError(err); }, at());
        return;
      }

      disk_manager_->disk_wal_->LogExtentGroupCorruptAsync(
        extent_group_id_, eg_state_->FormatSpec(), kDiskReadFailed);
    }

    FinishWithError(err);
    return;
  }

  if (RpcHasTimedOut()) {
    // No need to continue doing work if the corresponding RPC has already
    // timed out.
    const char *msg = "Disk read took too much time, failing op";
    TRACE(at_, 0) << msg;
    TAGGED_LOG(ERROR) << msg;
    FinishWithError(StargateError::kRetry);
    return;
  }

  if (skip_checksum_verification_) {
    ComputeResponsePayload();
    return;
  }

  // For every transformed slice buffer we read from the disk, verify its
  // checksum. Since this is a compute intensive operation, we'll hand each of
  // the buffers to the stargate compute pool.

  // Vector to hold work that should be queued as a batch.
  ArenaVector<Function<void()>> offload_vec(arena_);
  offload_vec.reserve(slice_state_vec_.size());

  // Initialize 'converted_slice_state_vec_' if we need to compute the
  // checksums of the specified checksum type.
  if (target_checksum_type_ &&
      *target_checksum_type_ != rpc_ret_->checksum_type()) {
    converted_slice_state_vec_.reserve(slice_state_vec_.size());
    for (uint ii = 0; ii < slice_state_vec_.size(); ++ii) {
      converted_slice_state_vec_.emplace_back(
        ExtentGroupState::SliceState::Create(
          *slice_state_vec_[ii], false /* compact */, arena_));
    }
  }

  for (size_t xx = 0; xx < slice_state_vec_.size(); ++xx) {

    
  LOG(INFO)<<"FLAGS_allow_partial_unreadable_egroups"<<FLAGS_allow_partial_unreadable_egroups;
  if (FLAGS_allow_partial_unreadable_egroups) {
    if (is_unreadable( rpc_ret_ , slice_state_vec_[xx]->slice_id())){
      LOG(INFO)<<"found corrupt"<<slice_state_vec_[xx]->slice_id();
        continue;
    }
  }

    Function<void()> func = 
      Bind(&ExtentGroupReadReplicaOp::VerifyChecksum, this, xx);
    ++outstanding_compute_reqs_;
    offload_vec.emplace_back(move(func));
  }
  LOG(INFO)<< "outstanding_compute_reqs_" << outstanding_compute_reqs_;

  TAGGED_CHECK_GT(outstanding_compute_reqs_, 0);

  globals_->compute_pool->QueueRequest(&offload_vec, qos_priority_);


  if(FLAGS_error){
    if(FLAGS_mark_disc_wal_corrupt){
        LOG(INFO)<<"error found and making disc wal corrupt";
        if (eg_state_) {
          disk_manager_->disk_wal_->LogExtentGroupCorruptAsync(
            extent_group_id_, eg_state_->FormatSpec(), kDiskReadFailed);
        }
    }
    if(FLAGS_set_estore_experimental_inject_egstate_corruption){
        FLAGS_estore_experimental_inject_egstate_corruption=true;
        FLAGS_estore_experimental_inject_non_existent_egroup_id=extent_group_id_;
    }

    FLAGS_error=false;
  }

}

//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReadReplicaOp::AIODiskReadData() {
  disk_io_start_time_usecs_ = WallTime::NowUsecs();

  unique_ptr<vector<pair<int64, int64>>> offset_count_vec =
    make_unique<vector<pair<int64, int64>>>();
  offset_count_vec->reserve(slice_state_vec_.size());
  slice_block_info_vec_.reserve(slice_state_vec_.size());
  int64 total_read_size = 0;
  int32 largest_transformed_length = 0;
  int64 last_block_num = -1;
  for (size_t xx = 0; xx < slice_state_vec_.size(); ++xx) {
    const ExtentGroupState::SliceState::UPtrConst& slice_state =
      slice_state_vec_[xx];

    TAGGED_CHECK(slice_state->has_extent_group_offset());
    TAGGED_CHECK(slice_state->has_transformed_length());
    LOG(INFO) << "slice_id being read:" << slice_state->slice_id()
              << "at offset" << slice_state->extent_group_offset()
              << endl;
     LOG(INFO) << "extent_based_metadata_format_ :"
               << extent_based_metadata_format_ << endl;
    LOG(INFO) << "slice_id being read:" << slice_state->slice_id()
              << "at offset" << slice_state->extent_group_offset()
              << endl;
    if (extent_based_metadata_format_) {
      TAGGED_DCHECK(disk_manager_->shared_extent_write_state_);
      BlockStore::Component *const component =
        disk_manager_->shared_extent_write_state_->egroups_component();
      const int64 block_size = component->block_size_bytes();

      slice_block_info_vec_.emplace_back();
      vector<int64> *const block_info = &slice_block_info_vec_.back();
      const PhysicalSliceFB *const physical_slice =
        eg_state_->base_state()->GetPhysicalSlice(slice_state->slice_id());
      TAGGED_DCHECK(physical_slice) << slice_state->slice_id();

      const int blocks_per_slice =
        (slice_state->transformed_length() + block_size - 1) / block_size;
      TAGGED_DCHECK_GT(blocks_per_slice, 0);
      block_info->reserve(blocks_per_slice);
      for (int ii = 0; ii < blocks_per_slice; ++ii) {
        const int64 block_num =
          disk_manager_->shared_extent_write_state_->GetBlockNum(
            *physical_slice, ii);
        if (block_num != 0) {
          if (block_num == last_block_num + 1) {
            offset_count_vec->back().second += block_size;
          } else {
            offset_count_vec->emplace_back(
              disk_manager_->shared_extent_write_state_->BlockNum2Offset(
                block_num), block_size);
          }
          last_block_num = block_num;
        }
        block_info->push_back(block_num);
      }
    } else {
      offset_count_vec->emplace_back(slice_state->extent_group_offset(),
                                     slice_state->transformed_length());
    }
    total_read_size += slice_state->transformed_length();

    largest_transformed_length = max(largest_transformed_length,
                                     slice_state->transformed_length());
  }

  // We don't expect the read to be bigger than the replication work unit size
  // by more than one slice.
  TAGGED_LOG_IF(INFO, (total_read_size >
                       FLAGS_estore_replication_work_unit_bytes +
                       largest_transformed_length))
    << "Total read size " << total_read_size << " is too large";

  auto done_cb = BindInFe<void(vector<IOBuffer::Ptr>&&, int)>(
    fe_,
    &ExtentStore::DiskManager::ExtentGroupReadReplicaOp::AIODiskReadDataDone,
    this, _1, _2);

  // Asynchronously read the slices for the op using a vector read.
  TAGGED_CHECK(desc_);
  const uint64 source_id =
    rpc_arg_->has_ilm_migration() ? migration_egroup_io_categorizer_id_ :
                                    bgscan_egroup_io_categorizer_id_;
  desc_->ReadVec(
    disk_manager_->io_categorizer_disk_id_ | source_id,
    move(offset_count_vec), done_cb);
}

//-----------------------------------------------------------------------------
// We will read per slice and return call back for each slice


void ExtentStore::DiskManager::ExtentGroupReadReplicaOp::AIODiskReadDataPerSlice() {
  disk_io_start_time_usecs_ = WallTime::NowUsecs();

  // unique_ptr<pair<int64, int64>> offset_count =
  //   make_unique<pair<int64, int64>>();

  // if (slice_state_vec_.size()>0)
  //     offset_count->reserve(slice_state_vec_[0].size());

  //slice_block_info_vec_.reserve(slice_state_vec_.size());







  int64 total_read_size = 0;
  int32 largest_transformed_length = 0;
  //int64 last_block_num = -1;
  int64 slices_passed=0;

  this->slices_callback_pending_=0;
  iobuf_vec_.reserve(slice_state_vec_.size());
  iobuf_vec_.resize(slice_state_vec_.size(),nullptr);
  //iobuf_arr=malloc(slice_state_vec_.size() * sizeof(IOBuffer::Ptr));
  

  for (size_t xx = 0; xx < slice_state_vec_.size(); ++xx) {
    const ExtentGroupState::SliceState::UPtrConst& slice_state =
      slice_state_vec_[xx];

    TAGGED_CHECK(slice_state->has_extent_group_offset());
    TAGGED_CHECK(slice_state->has_transformed_length());


    // if (extent_based_metadata_format_) {
    //   TAGGED_DCHECK(disk_manager_->shared_extent_write_state_);
    //   BlockStore::Component *const component =
    //     disk_manager_->shared_extent_write_state_->egroups_component();
    //   const int64 block_size = component->block_size_bytes();

    //   slice_block_info_vec_.emplace_back();
    //   vector<int64> *const block_info = &slice_block_info_vec_.back();
    //   const PhysicalSliceFB *const physical_slice =
    //     eg_state_->base_state()->GetPhysicalSlice(slice_state->slice_id());
    //   TAGGED_DCHECK(physical_slice) << slice_state->slice_id();

    //   const int blocks_per_slice =
    //     (slice_state->transformed_length() + block_size - 1) / block_size;
    //   TAGGED_DCHECK_GT(blocks_per_slice, 0);
    //   block_info->reserve(blocks_per_slice);
    //   for (int ii = 0; ii < blocks_per_slice; ++ii) {
    //     const int64 block_num =
    //       disk_manager_->shared_extent_write_state_->GetBlockNum(
    //         *physical_slice, ii);
    //     if (block_num != 0) {
    //       if (block_num == last_block_num + 1) {
    //         offset_count_vec->back().second += block_size;
    //       } else {
    //         offset_count_vec->emplace_back(block_num * block_size, block_size);
    //       }
    //       last_block_num = block_num;
    //     }
    //     block_info->push_back(block_num);
    //   }
    // }
    // } else {
    //   offset_count={slice_state->extent_group_offset(),
    //                                  slice_state->transformed_length()};
                                
    // }
    total_read_size += slice_state->transformed_length();

    largest_transformed_length = max(largest_transformed_length,
                                     slice_state->transformed_length());
                                


    // changes





    LOG(INFO)<<"AIODiskReadData______per_slice"<<++slices_passed<< " slice id  -> "<<slice_state->slice_id()<<endl;

    TAGGED_CHECK(desc_);
    const uint64 source_id =
    rpc_arg_->has_ilm_migration() ? migration_egroup_io_categorizer_id_ :
                                    bgscan_egroup_io_categorizer_id_;


    auto done_cb_ps = BindInFe<void(IOBuffer::Ptr&& , int )>(
    fe_,
    &ExtentStore::DiskManager::ExtentGroupReadReplicaOp::AIODiskReadDataDonePerSlice,
    this, _1, _2, xx );


    this->slices_callback_pending_++;


    desc_->Read(
    disk_manager_->io_categorizer_disk_id_ | source_id,
    slice_state->extent_group_offset(),  slice_state->transformed_length(), move(done_cb_ps) , nullptr );
  }

  // We don't expect the read to be bigger than the replication work unit size
  // by more than one slice.
  TAGGED_LOG_IF(INFO, (total_read_size >
                       FLAGS_estore_replication_work_unit_bytes +
                       largest_transformed_length))
    << "Total read size " << total_read_size << " is too large";

  
}

//-----------------------------------------------------------------------------
//
void ExtentStore::DiskManager::ExtentGroupReadReplicaOp::AIODiskReadDataDonePerSlice(
  IOBuffer::Ptr&& iobuf,
  const int read_errno,
  size_t xx
  ) {

  TAGGED_DCHECK(fe_->IsWorkerThread());



  vector<string> corrupt_slices_vec_s;
  vector<int> corrupt_slices_vec;
  LOG(INFO)<<"disk_manager_->disk_id_:"<<disk_manager_->disk_id_;
  //LOG(INFO)<<"disk_id:"<<disk_id;
    if(FLAGS_make_the_slices_corrupt && 
      (disk_manager_->disk_id_==FLAGS_disk_1 ||  disk_manager_->disk_id_==FLAGS_disk_2) )
  {
   //LOG(INFO)<<FLAGS_list_of_corrupt_slice_ids<<"$$$$";
    
    // stringstream sks(FLAGS_list_of_corrupt_slice_ids);

    // for (int i; sks >> i;) {
    //   LOG(INFO)<<"###########"<<i;
    //     corrupt_slices_vec.push_back(i);    
    //     if (sks.peek() == ',')
    //         sks.ignore();

    //   }
    const string corrupt_slice_ids=FLAGS_list_of_corrupt_slice_ids;
    BoostUtil::SplitOrDie(corrupt_slices_vec_s, corrupt_slice_ids , boost::is_any_of(","));
    for(string item: corrupt_slices_vec_s){
      LOG(INFO)<<"applying STOI on "<<item;
      corrupt_slices_vec.push_back(stoi(item));
    }
    
  }



  bool is_in_corrupt_slices= find(corrupt_slices_vec.begin(), corrupt_slices_vec.end(),
   slice_state_vec_[xx]->slice_id()) != corrupt_slices_vec.end();
  
  if (read_errno != 0 || is_in_corrupt_slices){
    
      TAGGED_LOG(ERROR) << "Error reading data from extent group file for slice number "<< xx
                      << ExtentStore::GetErrorDetails(read_errno);
      is_error_ = true;


    //code to populate the ret proto
    rpc_ret_->add_corrupt_slices(slice_state_vec_[xx]->slice_id());
  }
  else{
    iobuf_vec_[xx]=iobuf;
  }

  this->slices_callback_pending_--;

  if (this->slices_callback_pending_==0)
    {
       // iobuf_vec.insert(iobuf_vec.begin(), iobuf_arr, iobuf_arr + N);
        AIODiskReadDataDone(
        move(iobuf_vec_),
        is_error_);
    }

  }



//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReadReplicaOp::AIODiskReadDataDone(
  vector<IOBuffer::Ptr>&& iobuf_vec,
  int error) {

  TAGGED_DCHECK(fe_->IsWorkerThread());


  if(error) 
    LOG(INFO)<<"Error found while reading";


  int32 total_read_bytes = 0;
  if (!extent_based_metadata_format_) {
    TAGGED_CHECK_EQ(slice_state_vec_.size(), iobuf_vec.size());
  }
  size_t iobuf_vec_idx = 0;
  for (size_t xx = 0; xx < slice_state_vec_.size(); ++xx) {
    const ExtentGroupState::SliceState::UPtrConst& slice_state =
      slice_state_vec_[xx];

    if (extent_based_metadata_format_) {
      BlockStore::Component *const component =
        disk_manager_->shared_extent_write_state_->egroups_component();
      const int64 block_size = component->block_size_bytes();
      IOBuffer::Ptr iobuf = make_shared<IOBuffer>();
      const size_t num_blocks =
        (slice_state->transformed_length() + block_size - 1) / block_size;
      TAGGED_DCHECK_LE(num_blocks, slice_block_info_vec_[xx].size());
      bool block_read_failed = false;
      for (size_t ii = 0; ii < num_blocks; ++ii) {
        const int64 block_num = slice_block_info_vec_[xx][ii];
        if (block_num != 0) {
          TAGGED_DCHECK_GT(iobuf_vec.size(), iobuf_vec_idx);
          IOBuffer::Ptr block_iobuf;
          if (!iobuf_vec[iobuf_vec_idx]) {
            TAGGED_LOG(ERROR) << "Unable to read block number " << block_num;
            error = true;
            block_read_failed = true;
            break;
          }
          if (iobuf_vec[iobuf_vec_idx]->size() == block_size) {
            block_iobuf = move(iobuf_vec[iobuf_vec_idx++]);
          } else {
            TAGGED_DCHECK_GT(iobuf_vec[iobuf_vec_idx]->size(), block_size);
            block_iobuf = make_shared<IOBuffer>();
            iobuf_vec[iobuf_vec_idx]->Split(block_size, block_iobuf.get());
          }
          iobuf->AppendIOBuffer(move(block_iobuf));
        } else {
          IOBuffer::Ptr zeros = IOBufferUtil::CreateZerosIOBuffer(block_size);

          // Encrypt the buffer if needed.
          if (!encryption_type_vec_.empty()) {
            int padding_bytes = -1;
            zeros = StargateCryptoUtil::Encrypt(
              *slice_state, zeros.get(), encryption_type_vec_, *cipher_key_,
              extent_group_id_, block_size /* subregion_size */,
              1 /* num_subregions */, false /* slice_has_subregions */,
              ii * block_size /* write_data_offset */,
              false /* erasure_code_egroup */, &padding_bytes);
          }
          iobuf->AppendIOBuffer(move(zeros));
        }
      }
      if (block_read_failed) {
        break;
      }

      TAGGED_CHECK_GE(iobuf->size(), slice_state->transformed_length());
      if (iobuf->size() > slice_state->transformed_length()) {
        iobuf->Erase(slice_state->transformed_length());  
      }
      total_read_bytes += iobuf->size();
      slice_data_map_[slice_state->slice_id()] = move(iobuf);
      continue;
    }

    const int32 slice_size = slice_state->transformed_length();
    IOBuffer::Ptr& iobuf = iobuf_vec[xx];
    if (!iobuf) {
      TAGGED_LOG(ERROR) << "Unable to read " << slice_size << " bytes of data "
                        << "from offset " << slice_state->extent_group_offset()
                        << " of extent group file";
    } else if (iobuf->size() < slice_size) {
      TAGGED_LOG(ERROR) << "Read only " << iobuf->size()
                        << " bytes when attempting to read " << slice_size
                        << " bytes of data from offset "
                        << slice_state->extent_group_offset()
                        << " of extent group file";
      total_read_bytes += iobuf->size();
    } else {
      TAGGED_CHECK_EQ(iobuf->size(), slice_size);
      total_read_bytes += iobuf->size();
    }
    if (!iobuf || iobuf->size() != slice_size) {
      error = true;
    }
    slice_data_map_[slice_state->slice_id()] = move(iobuf);
  }

  const int64 diff_time_usecs =
    WallTime::NowUsecs() - disk_io_start_time_usecs_;
  TAGGED_LOG_IF(INFO,
                FLAGS_estore_disk_access_threshold_msecs > 0 &&
                diff_time_usecs >
                  FLAGS_estore_disk_access_threshold_msecs * 1000)
    << "AIO Disk read took " << diff_time_usecs/1000 << " msecs";

  ReportDiskOpCompleted(true /* is_read */,
                        error,
                        total_read_bytes,
                        diff_time_usecs);

  if (error && FLAGS_allow_partial_unreadable_egroups) FLAGS_error=true;


  if (!error || FLAGS_allow_partial_unreadable_egroups) {
    DiskReadDataDone(StargateError::kNoError);
  } else {
    DiskReadDataDone(StargateError::kDataCorrupt);
  }
}

//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReadReplicaOp::VerifyChecksum(
  const int slice_vec_index) {

  // This method is executed by a thread in Stargate's compute pool.
  TAGGED_DCHECK(globals_->compute_pool->pool()->IsWorkerThread());

  TAGGED_CHECK_LT(slice_vec_index, slice_state_vec_.size());
  const ExtentGroupState::SliceState::UPtrConst& slice_state =
    slice_state_vec_[slice_vec_index];

  SliceIdMap::const_iterator siter =
    slice_data_map_.find(slice_state->slice_id());
  TAGGED_CHECK(siter != slice_data_map_.end());

  IOBuffer::PtrConst data_buf = siter->second;

  // Compute the checksum for each subregion in a slice. If the data is
  // old/compressed, then the subregion size is equal to the slice length.
  const int32 num_subregions = slice_state->checksum_size();

  // When using extent based metadata format, we store only the untransformed
  // checksums for encrypted but uncompressed slices. Decrypt the buffer if the
  // slice is encrypted but uncompressed for checksum verification.
  const bool is_uncompressed =
    slice_state->transformed_length() == slice_state->untransformed_length();
  const bool is_encrypted = !encryption_type_vec_.empty();

  bool untransform_needed = untransform_needed_;
  if (!converted_slice_state_vec_.empty() &&
      slice_state->logical_checksum_size() == 0) {
    // If we are converting the checksum type of this slice and the slice
    // doesn't have logical checksums set, then we can skip untransformation as
    // we don't need to update the logical checksums.
    untransform_needed = false;
  }

  if (extent_based_metadata_format_ && is_uncompressed && is_encrypted) {
    vector<DataTransformation::Type> compression_type_vec;
    data_buf =
      UntransformSlice(*slice_state,
                       data_buf.get(),
                       compression_type_vec,
                       encryption_type_vec_,
                       cipher_key_,
                       extent_group_id_,
                       num_subregions,
                       0 /* read_start_offset */,
                       true /* has_prepended_compressed_size */);
    untransform_needed = false;
  }

  IOBuffer::PtrConst untransformed_data;
  const bool cksum_error =
    !FLAGS_estore_experimental_read_replica_skip_checksum &&
    (!data_buf ||
       StargateUtil::ComputeAndVerifySliceChecksum(
         "ExtentGroupReadReplicaOp",
         disk_manager_->disk_id_,
         extent_group_id_,
         data_buf.get(),
         slice_state.get(),
         num_subregions,
         0 /* subregion_start_offset */,
         false /* zeroed_out_slice */,
         true /* log_critical */,
         static_cast<ChecksumType>(rpc_ret_->checksum_type())));

  // Send the error response back if there is a checksum mismatch.
  if (cksum_error) {
    if (FLAGS_estore_experimental_dump_corrupt_slices &&
        is_retry_on_checksum_mismatch_ && eg_state_ && data_buf) {
      const string tag =
        StringJoin("stargate_checksum_mismatch_sl", slice_state->slice_id(),
          "_eg", extent_group_id_, "_disk", disk_manager_->disk_id_,
          "_ts_", WallTime::NowSecs(), "_is",
          eg_state_->latest_applied_intent_sequence(), "_of",
          slice_state->extent_group_offset(), "_read_replica_op.bin");
      if (data_buf) {
        StargateUtil::DumpIOBufferWithTag(tag, data_buf);
      } else {
        StargateUtil::DumpIOBufferWithTag(tag, siter->second);
      }
    }

    Function<void()> func =
      Bind(&ExtentGroupReadReplicaOp::VerifyChecksumDone, this,
           StargateError::kDataCorrupt);
    FunctionExecutor *const saved_fe = fe_;
    DeactivateFuncDisablerOutermost();
    saved_fe->Add(move(func));
    return;
  } else if (untransform_needed && (!is_uncompressed || is_encrypted)) {
    // Untransform is done only for those slices that are transformed and
    // passed checksum verification.
    TAGGED_VLOG(3) << "Untransforming slice: "
                   << OUTVARS(slice_state->slice_id());
    untransformed_data =
      UntransformSlice(*slice_state,
                       data_buf.get(),
                       compression_type_vec_,
                       encryption_type_vec_,
                       cipher_key_,
                       extent_group_id_,
                       num_subregions,
                       0 /* read_start_offset */,
                       mixed_compression_egroup_);
    if (untransformed_data == nullptr) {
      corrupt_reason_ = kFailedToUntransform;
      Function<void()> func =
        Bind(&ExtentGroupReadReplicaOp::VerifyChecksumDone, this,
             StargateError::kDataCorrupt);
      FunctionExecutor *const saved_fe = fe_;
      DeactivateFuncDisablerOutermost();
      saved_fe->Add(move(func));
      return;
    }

    if (FLAGS_estore_experimental_verify_logical_checksums_on_read &&
        slice_state->logical_checksum_size() > 0) {
      TAGGED_VLOG(3) << "Verifying logical checksums of slice: "
                     << OUTVARS(slice_state->slice_id());
      StargateUtil::ComputeAndVerifySliceLogicalChecksum(
        "ExtentGroupReadReplicaOp",
         disk_manager_->disk_id_,
         extent_group_id_,
         untransformed_data,
         slice_state.get(),
         static_cast<ChecksumType>(rpc_ret_->checksum_type()));
    }
  }

  TAGGED_VLOG(2) << "Successfully matched checksum value(s)"
                 << " for slice id " << slice_state->slice_id();

  if (target_checksum_type_ &&
      *target_checksum_type_ != rpc_ret_->checksum_type()) {
    TAGGED_DCHECK_LT(slice_vec_index, converted_slice_state_vec_.size());
    ExtentGroupState::SliceState *const converted_slice_state =
      converted_slice_state_vec_[slice_vec_index].get();
    const int num_subregions = converted_slice_state->checksum_size();
    const int subregion_size =
      converted_slice_state->untransformed_length() / num_subregions;
    StargateUtil::ComputeAndSetSliceSubregionChecksum(
      data_buf,
      converted_slice_state,
      subregion_size,
      num_subregions,
      num_subregions > 1 /* slice_has_subregion */,
      0 /* begin_offset */,
      false /* reset_existing_checksum */,
      *target_checksum_type_);

    // Compute the logical checksums if needed.
    if (converted_slice_state->logical_checksum_size() > 0) {
      TAGGED_DCHECK(untransformed_data)
        << OUTVARS(converted_slice_state->ShortDebugString());
      TAGGED_DCHECK_EQ(untransformed_data->size(),
                       converted_slice_state->untransformed_length())
        <<  OUTVARS(converted_slice_state->ShortDebugString());
      StargateUtil::ComputeAndSetSliceSubregionLogicalChecksum(
        untransformed_data,
        converted_slice_state,
        converted_slice_state->untransformed_length() /
          converted_slice_state->logical_checksum_size(),
        0 /*  begin_offset */,
        *target_checksum_type_);
    }

    TAGGED_VLOG(2) << "Computed converted checksums for slice: "
                   << OUTVARS(slice_state->slice_id(),
                              static_cast<int>(rpc_ret_->checksum_type()),
                              static_cast<int>(*target_checksum_type_),
                              slice_state->ShortDebugString(),
                              converted_slice_state->ShortDebugString());
  }

  Function<void()> func =
    Bind(&ExtentGroupReadReplicaOp::VerifyChecksumDone, this,
         StargateError::kNoError);
  FunctionExecutor *const saved_fe = fe_;
  DeactivateFuncDisablerOutermost();
  saved_fe->Add(move(func));
}

//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReadReplicaOp::VerifyChecksumDone(
  const StargateError::Type err) {

  TAGGED_DCHECK(fe_->IsWorkerThread());

  --outstanding_compute_reqs_;
  TAGGED_CHECK_GE(outstanding_compute_reqs_, 0);

  if (err != StargateError::kNoError) {
    error_ = err;
  }

  if (outstanding_compute_reqs_ > 0) return;

  // Simulate checksum mismatch for testing.
  if (google::DEBUG_MODE && !is_retry_on_checksum_mismatch_ &&
      FLAGS_estore_aio_enabled &&
      Random::TL()->OneIn(
        FLAGS_estore_experimental_simulate_corrupt_read_frequency)) {
    TAGGED_LOG(INFO) << "Simulating corrupt replica for read replica for "
                     << "egroup ";
    error_ = StargateError::kDataCorrupt;
  }

  if (error_ != StargateError::kNoError) {
    if (error_ == StargateError::kDataCorrupt) {
      if (FLAGS_estore_aio_enabled && !is_retry_on_checksum_mismatch_) {
        // Let us retry this read before marking the egroup as corrupt. This is
        // mitigation for ENG-29727.
        TRACE(at_, 0) << "Retrying read replica on corrupt egroup";
        TAGGED_LOG(INFO) << "Retrying read replica on corrupt egroup ";
        is_retry_on_checksum_mismatch_ = true;
        error_ = StargateError::kNoError;
        // Reset corrupt_reason_ to default.
        corrupt_reason_ = kSliceChecksumMismatch;
        slice_data_map_.clear();
        IssueDiskRead();
        return;
      }

      if (is_retry_on_checksum_mismatch_ && rpc_arg_->copy_corrupt_replica()) {
        // The data read is corrupt even after we retried the read. But the
        // caller is copying corrupt replicas. So we will ignore this and treat
        // the data as correct.
        TAGGED_LOG(WARNING) << "Returning egroup data inspite of corrupt "
                            << "slices, as requested by the caller";
        error_ = StargateError::kNoError;
        ComputeResponsePayload();
        return;
      }

      // We'll issue a log entry to mark this extent group as corrupt.
      if (eg_state_) {
        if (extent_based_metadata_format_) {
          disk_manager_->shared_extent_write_state_->LogExtentGroupCorrupt(
            extent_group_id_, eg_state_.get(), corrupt_reason_,
            [this]() { FinishWithError(error_); }, at());
          return;
        }

        disk_manager_->disk_wal_->LogExtentGroupCorruptAsync(
          extent_group_id_, eg_state_->FormatSpec(), corrupt_reason_);
      }
    }

    FinishWithError(error_);
    return;
  }

  // We've read all the slice buffers and verified their checksums. We're now
  // ready to send a reply back.
  ComputeResponsePayload();
}

//-----------------------------------------------------------------------------

void
ExtentStore::DiskManager::ExtentGroupReadReplicaOp::ComputeResponsePayload() {
  for (size_t xx = 0; xx < slice_state_vec_.size(); ++xx) {
    const int32 slice_id = slice_state_vec_[xx]->slice_id();
    SliceIdMap::const_iterator siter = slice_data_map_.find(slice_id);
    TAGGED_CHECK(siter != slice_data_map_.end());

    if(siter->second.get()==NULL) continue;


    egroup_data_->AppendIOBuffer(siter->second.get());
  }

  TAGGED_CHECK_GT(egroup_data_->size(), 0);
  if (rpc_) {
    rpc_->set_response_payload(move(egroup_data_));
  }

  if (read_complete_egroup_) {
    TAGGED_DCHECK(!rpc_);
    all_slices_state_vec_.reserve(
      rpc_ret_->extent_group_metadata().slices_size());
    all_slices_block_info_vec_.reserve(
      rpc_ret_->extent_group_metadata().slices_size());
    for (auto&& entry : slice_state_vec_) {
      all_slices_state_vec_.push_back(move(entry));
    }
    for (auto&& entry : slice_block_info_vec_) {
      all_slices_block_info_vec_.push_back(move(entry));
    }

    if (error_ == StargateError::kNoError &&
        next_slice_idx_ < rpc_ret_->extent_group_metadata().slices_size()) {
      slice_state_vec_.clear();
      slice_block_info_vec_.clear();
      PopulateSliceStateVec();
      if (!slice_state_vec_.empty()) {
        IssueDiskRead();
        return;
      }
    }
  }

  FinishWithError(error_);
}

//-----------------------------------------------------------------------------

int32 ExtentStore::DiskManager::ExtentGroupReadReplicaOp::Cost() const {
  // This op reads data from a remote extent store. The cost of the op is
  // the number of slices it has to read.
  int min_op_cost = disk_manager_->min_op_cost();
  if (!is_primary_) {
    min_op_cost = disk_manager_->min_secondary_op_cost();
    if (IsHighPriority()) {
      // For high priority secondary ops, we assign minimum cost so that they
      // don't get blocked in the secondary queue.
      return min_op_cost;
    }
  }

  // TODO(harshit): For AES extent groups we don't have the number of slices
  // available in RPC due to which we will get slice size as 0. Therefore, we
  // need to change the following to account for slices in AES managed extent
  // groups.
  return min(max(rpc_arg_->slices_size(), min_op_cost),
             disk_manager_->max_op_cost());
}

//-----------------------------------------------------------------------------

void
ExtentStore::DiskManager::ExtentGroupReadReplicaOp::SetTraceAttributes() {
  TRACE_ADD_ATTRIBUTE(at_, "egroup", extent_group_id_);
  TRACE_ADD_ATTRIBUTE(at_, "disk", disk_manager_->disk_id_);
  TRACE_ADD_ATTRIBUTE(at_, "intent_seq", rpc_arg_->expected_intent_sequence());
  TRACE_ADD_ATTRIBUTE(at_, "is_primary", is_primary_);
  if (aes_conversion_in_progress_) {
    TRACE_ADD_ATTRIBUTE(at_, "aes_conversion_in_progress", "true");
  }
}

//-----------------------------------------------------------------------------

void
ExtentStore::DiskManager::ExtentGroupReadReplicaOp::FinishWithError(
  const StargateError::Type error) {

  if (info_egroup_helper_op_) {
    info_egroup_helper_op_->FinishHelper(
      false /* update_physical_state */, false /* wait_for_completion */);
  }
  Finish(error);
}

//-----------------------------------------------------------------------------

} } } // namespace
