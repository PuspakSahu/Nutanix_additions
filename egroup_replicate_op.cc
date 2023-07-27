/*
 * Copyright (c) 2010 Nutanix Inc. All rights reserved.
 *
 * Author: aron@nutanix.com
 *
 */

#include <gflags/gflags.h>
#include <list>

#include "cdp/client/stargate/stargate_base/stargate_util.h"
#include "medusa/medusa.h"
#include "medusa/medusa_printer_util.h"
#include "stargate/counters/migration_counters.h"
#include "stargate/extent_store/blockstore_egroup_manager.h"
#include "stargate/extent_store/disk_perf_stat.h"
#include "stargate/extent_store/disk_WAL.h"
#include "stargate/extent_store/disk_WAL_record.h"
#include "stargate/extent_store/egroup_replicate_op.h"
#include "stargate/stargate.h"
#include "stargate/stargate_cluster_state.h"
#include "stargate/stargate_compute_pool.h"
#include "stargate/stargate_net.h"
#include "util/base/adler32.h"
#include "util/base/char_array.h"
#include "util/base/iobuffer_util.h"
#include "util/base/scoped_executor.h"
#include "util/base/string_util.h"
#include "util/base/walltime.h"
#include "util/block_store/file_system_vfs.h"
#include "util/block_store/object_page.h"
#include "util/misc/crash_injector.h"
#include "util/storage/aio_executor.h"
#include "util/storage/io_categorizer.h"
#include "zeus/configuration.h"

using namespace nutanix::block_store;
using namespace nutanix::kv_store;
using namespace nutanix::mantle;
using namespace nutanix::medusa;
using namespace nutanix::net;
using namespace nutanix::stargate::counters;
using namespace nutanix::storage;
using namespace nutanix::thread;
using namespace nutanix::zeus;
using namespace std;
using namespace std::placeholders;

DEFINE_int32(estore_replication_work_unit_bytes,
             1024 * 1024,
             "During replication, we'll fetch data over several RPCs, each of "
             "which will fetch approximately this amount of data.");

DEFINE_int32(estore_hibernation_replication_work_unit_bytes,
             4 * 1024 * 1024,
             "During replication triggered by hibernate or resume operation, "
             "we'll fetch data over several RPCs, each of which will fetch "
             "approximately this amount of data.");

DEFINE_int32(estore_replication_max_work_units,
             4,
             "During replication, these are the maximum number of work units "
             "we'll have outstanding at any time.");

DEFINE_bool(estore_replication_flip_active_status,
            false,
            "When the replication op is waiting for a remote RPC, mark it "
            "inactive.");

DEFINE_int64(estore_experimental_replication_delay_before_fetch,
             -1,
             "Unit test flag to delay the replicate op before the egroup "
             "state has been fetched.");

DEFINE_int64(estore_experimental_replication_delay_after_tu,
             -1,
             "Unit test flag to delay the replicate op after the tentative "
             "update has been flushed.");

DEFINE_int64(estore_experimental_replication_delay_finalize,
             -1,
             "Unit test flag to delay the finalization of the specified "
             "extent group id in the WAL at the end of the replication.");

DEFINE_bool(estore_disk_queue_drop_ilm_request_enabled, false,
            "If false ilm requests are not dropped even if disk queue is "
            "experiencing high latency.");

DEFINE_bool(estore_experimental_aggregate_work_units, false,
            "If true we aggregate all the work units read data so that we can "
            "issue a single disk write.");

DEFINE_bool(estore_experimental_cancel_before_finalize_tu, false,
            "If true we cancel the op before finalizing tentative update.");

DEFINE_bool(estore_experimental_crash_before_finalize_tu, false,
            "If true, crash stargate just before the replication TU is "
            "finalized.");

DEFINE_bool(estore_experimental_crash_after_finalize_tu, false,
            "If true, crash stargate once the replication TU is finalized.");

DEFINE_int32(estore_replicateop_crash_injection, -1,
             "Crashes at the corresponding injection point defined in "
             "'CrashInjections'.");

DEFINE_int32(estore_replicateop_error_injection, -1,
             "Simulates error at the corresponding point defined in "
             "'ErrorInjections'.");

DECLARE_int32(estore_ssd_iops);
DECLARE_int32(estore_hdd_iops);
DECLARE_bool(estore_aio_enabled);
DECLARE_bool(estore_fdatasync_enabled);
DECLARE_int32(estore_disk_access_threshold_msecs);
DECLARE_int32(estore_disk_queue_drop_ilm_request_threshold_secs);
DECLARE_int32(estore_regular_slice_size);
DECLARE_bool(estore_experimental_verify_payload_data_checksums);
DECLARE_bool(estore_experimental_sink_data);

namespace nutanix { namespace stargate { namespace extent_store {

using namespace interface;

constexpr const char *
  ExtentStore::DiskManager::ExtentGroupReplicateOp::crash_injections_str_[];
constexpr const char *
  ExtentStore::DiskManager::ExtentGroupReplicateOp::error_injections_str_[];

//-----------------------------------------------------------------------------

ExtentStore::DiskManager::ExtentGroupReplicateOp::ExtentGroupReplicateOp(
  DiskManager *disk_manager,
  Rpc *rpc,
  const ReplicateExtentGroupArg *arg,
  ReplicateExtentGroupRet *ret,
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
         egroup_replicate_op_id)),
  rpc_arg_(arg),
  extent_group_id_(arg->extent_group_id()),
  managed_by_aes_(ManagedByAes(arg).managed_by_aes),
  extent_based_metadata_format_(
    ManagedByAes(arg).extent_based_metadata_format),
  written_tentative_update_(false),
  slice_vec_(arena_),
  slice_allocated_blocks_map_(arena_),
  allocated_logical_offset_vec_(arena_),
  slice_vec_index_(0),
  outstanding_work_units_(0),
  outstanding_rpcs_(0),
  work_unit_disk_write_outstanding_(false),
  work_unit_replica_read_map_(arena_),
  total_write_time_usecs_(0),
  total_bytes_written_(0),
  fallocated_egroup_size_(0),
  intent_sequence_(-1),
  expected_intent_sequence_(
    managed_by_aes_ ? -1 : arg->expected_intent_sequence()),
  global_metadata_intent_sequence_(
    managed_by_aes_ ? arg->intent_sequence() :
                      arg->expected_intent_sequence()),
  block_store_deallocate_finalize_key_(-1),
  deleted_extent_id_set_(arena_),
  deleted_slice_id_set_(arena_),
  blockstore_finalize_issued_(false),
  blockstore_finalize_outstanding_(false),
  waiting_to_finish_(false),
  waiting_to_finalize_(false),
  disk_usage_diff_(rpc_arg_->disk_usage()),
  checkpoint_lock_acquired_(false),
  finalized_tentative_update_(false),
  block_store_component_(nullptr),
  shared_state_(disk_manager->shared_extent_write_state_.get()),
  egid_vec_(arena_),
  mvalue_vec_(arena_),
  checksum_type_(ChecksumType::kAdler32),
  verify_checksums_(
    FLAGS_estore_experimental_verify_payload_data_checksums &&
    globals_->cluster_state->cluster_stargate_version() >=
      zeus::ConfigurationProto::kVerifyPayloadChecksumVersion &&
    !rpc_arg_->copy_corrupt_replica()),
  extent_based_egroup_applied_is_(-1),
  egroup_file_may_exist_(false) {

  SetLogMessagePrefix(
    StringJoin("opid=", operation_id_,
               " egroup_id=", extent_group_id_,
               " managed_by_aes=", managed_by_aes_ ? "true" : "false",
               " extent_based=", extent_based_metadata_format_ ?
                   "true" : "false",
               " disk=", disk_manager_->disk_id_, ": "));

  TAGGED_DCHECK(CheckPrioritySetAfterUpgrade(
                  arg->has_qos_priority(),
                  arg->qos_principal_name())) << arg->ShortDebugString();
  TAGGED_DCHECK(!extent_based_metadata_format_ || managed_by_aes_)
    << arg->ShortDebugString();

  max_work_unit_bytes_ = FLAGS_estore_replication_work_unit_bytes;
  const Configuration::PtrConst& config = globals_->stargate->Config();
  if (config->IsClusterInHibernateTransition() &&
      StargateUtil::S3KVStoreEnabled(config, rpc_arg_->remote_disk_id())) {
    max_work_unit_bytes_ =
      FLAGS_estore_hibernation_replication_work_unit_bytes;
  }
  ++disk_manager_->num_outstanding_replicateops_;
}

ExtentStore::DiskManager::ExtentGroupReplicateOp::~ExtentGroupReplicateOp() {
  --disk_manager_->num_outstanding_replicateops_;

  if (written_tentative_update_ && error_ != StargateError::kNoError &&
      !extent_based_metadata_format_) {
    // Issue an abort and also delete the extent group replica.
    ScopedArenaMark am;
    DiskWALRecord::Ptr record = DiskWALRecord::Create(am.arena());
    auto egroup_update = record->add_egroup_update();
    egroup_update->set_extent_group_id(extent_group_id_);

    TAGGED_CHECK_GE(intent_sequence_, 0);
    egroup_update->set_delete_egroup(true);
    if (managed_by_aes_) {
      egroup_update->set_managed_by_aes(true);
      egroup_update->set_global_metadata_intent_sequence(
        rpc_arg_->intent_sequence());
    }

    Function<void()> abort_cb;
    if (!finalized_tentative_update_) {
      egroup_update->set_abort_tentative_intent_sequence(intent_sequence_);

      if (disk_manager_->block_store_) {
        egroup_update->set_finalize_deletion_truncation(true);
      }
      MaybeFinalizeOrAbortInfoEgroupTU(record, false /* finalize */);
    } else {
      // If we reach here, we have already finalized the tentative update and
      // then failed while flushing the egroup metadata to AESDB.
      TAGGED_LOG(WARNING) << "Deleting finalized egroup due to AESDB update "
                          << "failure";

      ExtentGroupState::Ptr eg_state =
        disk_manager_->TryFetchExtentGroupState(extent_group_id_);

      if (disk_manager_->block_store_) {
        TAGGED_DCHECK(blockstore_finalize_issued_);
        TAGGED_DCHECK(!allocated_block_id_list_);
        // When using the block store, add the physical segments of the egroup
        // to be deleted to the egroup update record. The disk WAL will delete
        // the physical segments after the record is written to the WAL, then
        // finalize the WAL record and finalize the deletion of the physical
        // segments in the block store.
        AddPhysicalSegmentsToEgroupUpdate(eg_state.get(), egroup_update.get());

        // Provide a dummy callback to be passed to LogDeltaRecord below as
        // required by the disk WAL when using the block store. Since the
        // deallocation will be handled by the disk WAL itself as mentioned
        // above, the callback will be a no-op as we don't need any additional
        // actions in this op related to this egroup.
        abort_cb = bind([egid = extent_group_id_,
                         disk_id = disk_manager_->disk_id_]() {
            LOG(INFO) << "Successfully deleted egroup " << egid
                      << " from disk "
                      << disk_id << " on replicate op error";
          });
      }

      // If this an erasure coded parity egroup, log a delete for the
      // associated info replicas.
      if (eg_state->IsErasureCodedParityAESEgroup()) {
        for (const int64 local_info_egroup_id :
               eg_state->ec_info()->local_info_egroup_id_vec) {
          TAGGED_VLOG(3) << "Deleting the local info egroup metadata "
                         << local_info_egroup_id;
          auto egroup_update = record->add_egroup_update();
          egroup_update->set_extent_group_id(local_info_egroup_id);
          egroup_update->set_delete_egroup(true);
          if (disk_manager_->block_store_) {
            // When using block store, info egroups for a given AES parity
            // egroup will not have physical segments. As there are no block to
            // be deallocated in block store, we can directly finalize the
            // deletion of info egroups.
            egroup_update->set_finalize_deletion_truncation(true);
          }
          egroup_update->set_managed_by_aes(true);
        }
      }
    }
    record->Finish();

    if (block_store_deallocate_finalize_key_ != -1) {
      // We don't need to use BindInFe() here because the function object does
      // not hold any reference to this object.
      abort_cb = fe_->Bind(
        &BlockStore::Component::FinalizeDeallocation, block_store_component_,
        block_store_deallocate_finalize_key_, [](){});
    }
    disk_manager_->disk_wal_->LogDeltaRecord(record.get(), move(abort_cb));
  }

  if (extent_based_metadata_format_ && allocated_block_id_list_) {
    // Abort the allocation of 'allocated_block_id_list_'.
    TAGGED_DCHECK_NE(error_, StargateError::kNoError);
    TAGGED_DCHECK(!allocated_block_id_list_->Empty());
    if (!shared_state_->data_on_ext4_files()) {
      block_store_component_->FinalizeAllocation(
        allocated_block_id_list_->ReleaseCtr(), true /* abort */, [](){},
        nullptr /* at */);
    }
    allocated_block_id_list_.Reset();
  }

  // We should have finalized or aborted the block allocation and
  // 'allocated_block_id_list_' should have been reset.
  TAGGED_DCHECK(!allocated_block_id_list_);

  // Free any extent group locks we might be holding.
  if (egid_locks_held_) {
    UnlockAllExtentGroups();
  }
  if (checkpoint_lock_acquired_) {
    disk_manager_->checkpoint_egroup_locker_.Release(extent_group_id_);
  }

  ExtentGroupState::Ptr eg_state =
    disk_manager_->TryFetchExtentGroupState(extent_group_id_);
  MaybeDeleteExtentGroupState(extent_group_id_, eg_state);
}

//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReplicateOp::StartImpl() {
  TAGGED_DCHECK(fe_->IsWorkerThread());

  // Report the queueing delay seen by this op.
  ReportQueueingDelay();

  // Check if the average queueing delay observed for the local disk is larger
  // that the specified threshold. If so, it indicates that the local disk is
  // very busy. In this case, we will defer any replication triggered for the
  // purpose of ILM, to avoid adding more pressure on the disk.
  if (FLAGS_estore_disk_queue_drop_ilm_request_enabled &&
      rpc_arg_->ilm_migration()) {
    disk_manager_->rwlock_.Lock();
    const int32 avg_q_delay_usecs =
      disk_manager_->perf_stat_->AvgQueueingDelayUsecs();
    disk_manager_->rwlock_.Unlock();
    if (avg_q_delay_usecs >
          FLAGS_estore_disk_queue_drop_ilm_request_threshold_secs *
            1000000LL) {
      TAGGED_VLOG(0) << "Dropping ILM replicate egroup for extent group "
                     << extent_group_id_ << "(expected_intent_seq: "
                     << expected_intent_sequence_
                     << ", global_metadata_intent_seq: "
                     << global_metadata_intent_sequence_
                     << ") from remote disk " << rpc_arg_->remote_disk_id()
                     << " to local disk " << disk_manager_->disk_id_
                     << " due to high avg queueing delay of "
                     << avg_q_delay_usecs << " usecs in the local disk queue";

      MaybeFinish(StargateError::kRetry);
      return;
    }
  }

  TAGGED_VLOG(0) << "Starting replication of extent group from remote disk "
                 << rpc_arg_->remote_disk_id()
                 << ", expected_intent_sequence " << expected_intent_sequence_
                 << ", global_metadata_intent_sequence "
                 << global_metadata_intent_sequence_
                 << ", latest_intent_sequence " << rpc_arg_->intent_sequence();

  if (extent_based_metadata_format_) {
    // Check if there is any pending commit on this egroup and finish the op
    // with kAlreadyExists after resolving the pending commits.
    // This can happen in the rare case where we had the egroup as a secondary
    // replica on this disk which was later orphaned, but not yet deleted.
    TAGGED_DCHECK(shared_state_);
    if (shared_state_->HasPendingCommit(extent_group_id_) ||
        MaybeInjectError(ErrorInjections::kPendingCommit, __LINE__)) {
      shared_state_->ResolvePendingCommit(extent_group_id_);
      TAGGED_LOG(WARNING) << "Replication failed as replica already exists "
                          << "with pending commits";
      MaybeFinish(StargateError::kAlreadyExists);
      return;
    }
  }

  // Grab an exclusive lock on the extent group id.
  RegisterExtentGroup(extent_group_id_, true);
  AcquireExtentGroupLocks();
}

//-----------------------------------------------------------------------------

void
ExtentStore::DiskManager::ExtentGroupReplicateOp::ExtentGroupLocksAcquired() {
  TAGGED_DCHECK(fe_->IsWorkerThread());

  if (FLAGS_estore_experimental_replication_delay_before_fetch ==
        extent_group_id_) {
    TRACE(at_, 0) << "Delaying before fetching egroup state";
    TAGGED_LOG(INFO) << "Delaying before fetching egroup state";
    globals_->delay_ed->Add(BindInFe<void()>(
      fe_, &ExtentGroupReplicateOp::ExtentGroupLocksAcquired, this), 1000);
    return;
  }

  if (disk_manager_->overfull_) {
    TAGGED_LOG(WARNING) << "Replication failed as disk is full";
    MaybeFinish(StargateError::kDiskSpaceUnavailable);
    return;
  }

  // Lookup the extent group state. If we don't find it in memory, we'll lookup
  // the physical state map in Medusa, unless the RPC arg has explicitly set
  // managed_by_aes to false for non-AES egroups. We also set
  // include_to_remove_egstate as true to fetch eg_state for the AES extent
  // group even if it is marked for removal.
  FetchExtentGroupState(extent_group_id_, false /* create_if_needed */,
                        ManagedByAes(rpc_arg_),
                        true /* include_to_remove_egstate */);
}

void ExtentStore::DiskManager::ExtentGroupReplicateOp::ExtentGroupStateFetched(
  const StargateError::Type err,
  const ExtentGroupState::Ptr& egroup_state) {

  TAGGED_DCHECK(fe_->IsWorkerThread());

  if (err != StargateError::kNoError) {
    TAGGED_LOG(WARNING) << "Fetching extent group state failed with error "
                        << StargateError::ToString(err);
    MaybeFinish(StargateError::kRetry);
    return;
  }

  ExtentGroupState::Ptr eg_state = egroup_state;
  if (eg_state && eg_state->extent_based_metadata_format()) {
    shared_state_->PopulateEgState(eg_state.get());
  }

  // If eg_state can be discarded, do so. Otherwise if the extent group already
  // exists, send an error back.
  if (eg_state && eg_state->IsZombie()) {
    TAGGED_LOG(INFO) << "Deleting Zombie state";
    disk_manager_->DeleteExtentGroupState(extent_group_id_);
    eg_state.reset();
  } else if (eg_state && !eg_state->extent_based_metadata_format() &&
             eg_state->largest_seen_intent_sequence() < 0 &&
             !eg_state->to_remove() &&
             eg_state->needs_tentative_resolution()) {
    // This is a zombie egroup state and the only reason we didn't delete it
    // yet was because it had the needs_tentative_resolution flag set, to
    // ensure that no new writes are admitted until the fixer op is run. We
    // have now received a replicate op on this replica, which requires a fixer
    // op to run before replication. It's possible that this replica was not
    // available when the fixer op issued a delete and so we still have this
    // needs_tentative_resolution zombie left behind. We can delete it now and
    // proceed with the replication. Even if we didn't, we would have sent
    // kAlreadyExists below which would issue a delete RPC to get rid of this
    // state, but that would add an unnecessary delay in the replication.
    TAGGED_LOG(INFO)
      << "Deleting zombie state with needs_tentative_resolution set";
    disk_manager_->DeleteExtentGroupState(extent_group_id_);
    eg_state.reset();
  }

  if (eg_state && !eg_state->to_remove()) {
    TAGGED_DCHECK(extent_based_metadata_format_ ||
                  !eg_state->IsErasureCodedInfoAESEgroupMetadataReplica())
      << OUTVARS(eg_state->ToString());

    TAGGED_LOG(WARNING) << "Replication failed as replica already exists";
    MaybeFinish(StargateError::kAlreadyExists);
    return;
  }

  if (rpc_arg_->slices_size() > 0) {
    // We already have slice information available to us. Let's populate
    // 'slice_vec_'.
    slice_vec_.reserve(rpc_arg_->slices_size());
    for (int xx = 0; xx < rpc_arg_->slices_size(); ++xx) {
      // Only those slices that need to be written to disk go in 'slice_vec_'.
      if (rpc_arg_->slices(xx).has_extent_group_offset()) {
        slice_vec_.emplace_back(
          ExtentGroupState::SliceState::Create(&rpc_arg_->slices(xx),
                                               arena_));
      }
    }
    // Sort slice vector in increasing order of extent group offsets.
    sort(
      slice_vec_.begin(), slice_vec_.end(),
      ExtentGroupState::SliceState::SliceStateUPtrOrderIncreasingFileOffset);
  }

  if (rpc_arg_->remote_disk_id() < 0) {
    TAGGED_DCHECK(rpc_arg_->has_read_replica_ret());
    ++outstanding_work_units_;
    if (!managed_by_aes_) {
      TAGGED_CHECK_EQ(slice_vec_index_, 0);
      int32 expected_payload_size = 0;
      const auto& remote_payload = rpc_->request_payload();
      for (size_t xx = 0; xx < slice_vec_.size(); ++xx) {
        expected_payload_size += slice_vec_[xx]->transformed_length();
        if (expected_payload_size < remote_payload->size()) {
          continue;
        }

        slice_vec_index_ = xx + 1;
        break;
      }
      // We should have data exactly till one of the slice's boundary.
      if (expected_payload_size != remote_payload->size()) {
        TAGGED_LOG(DFATAL)
          << "Unexpected size of data fetched from remote disk "
          << rpc_arg_->remote_disk_id() << ", "
          << OUTVARS(expected_payload_size, remote_payload->size());

        MaybeFinish(StargateError::kRetry);
        return;
      }
      TAGGED_CHECK_EQ(slice_vec_index_, slice_vec_.size());
      WriteTentativeWALRecord();
      return;
    }

    // TAGGED_DCHECK(extent_based_metadata_format_);
    ExtentGroupPhysicalStateFetched(
      StargateError::kNoError,
      make_shared<string>(),
      make_shared<ReplicaReadEgroupRet>(rpc_arg_->read_replica_ret()),
      rpc_->request_payload()->Clone());
    return;
  }

  // Set 'remote_ifc_'.
  remote_stargate_handle_ =
    globals_->cluster_state->FetchStargateHandle(rpc_arg_->remote_disk_id());

  // If we are running the ExtentWriteOpTest, fetch the stargate handle using
  // the 'unit_test_svm_handle_map_' in the disk manager that will be populated
  // by the test.
  if (remote_stargate_handle_.empty() &&
      !disk_manager_->unit_test_svm_handle_map_.empty()) {
    const Configuration::PtrConst& config = globals_->stargate->Config();
    const ConfigurationProto::Disk *const disk =
      config->LookupDisk(rpc_arg_->remote_disk_id());
    if (disk && disk->has_service_vm_id()) {
      auto iter = disk_manager_->unit_test_svm_handle_map_.find(
        disk->service_vm_id());
      if (iter != disk_manager_->unit_test_svm_handle_map_.end()) {
        remote_stargate_handle_ = iter->second;
      }
    }
  }

  if (remote_stargate_handle_.empty() ||
      MaybeInjectError(ErrorInjections::kReplicaHandleUnavailable, __LINE__)) {
    TAGGED_LOG(WARNING) << "Unable to fetch the stargate handle for remote "
                        << "disk id " << rpc_arg_->remote_disk_id();
    MaybeFinish(StargateError::kReplicaUnavailable);
    return;
  }
  remote_ifc_ = globals_->netsub->FetchInterface(
    remote_stargate_handle_, disk_manager_->disk_id_);

  if (RetrieveDeletedExtentsAndSlices(
        extent_group_id_, eg_state, &deleted_extent_id_set_,
        &deleted_slice_id_set_)) {
    TAGGED_DCHECK(managed_by_aes_);
    TAGGED_DCHECK(!extent_based_metadata_format_);
    TAGGED_DCHECK(!checkpoint_lock_acquired_);
    // Egroup is marked for deletion with metedata in AES DB. We acquire the
    // checkpoint lock on this egroup to ensure that there is no concurrent
    // UpdatePhysicalStateOp going on while we are working on this egroup.
    TRACE(at_, 0) << "Acquiring checkpoint lock";
    TAGGED_VLOG(3) << "Acquiring checkpoint lock";
    checkpoint_lock_acquired_ =
      disk_manager_->checkpoint_egroup_locker_.TryAcquire(
        extent_group_id_, true /* is_writer */);

    if (!checkpoint_lock_acquired_) {
      // Acquire lock asynchronously.
      auto done_cb = BindInFe<void()>(
        fe_, &ExtentGroupReplicateOp::MaybeFetchExtentGroupPhysicalState,
        this);
      checkpoint_lock_acquired_ = true;
      disk_manager_->checkpoint_egroup_locker_.Acquire(
        extent_group_id_, true /* is_writer */, move(done_cb));
      return;
    }
  }

  MaybeFetchExtentGroupPhysicalState();
}

//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReplicateOp::
MaybeFetchExtentGroupPhysicalState() {

  TAGGED_DCHECK(fe_->IsWorkerThread());

  if (rpc_arg_->slices_size() > 0) {
    WriteTentativeWALRecord();
    return;
  }

  // For extent groups managed by AES, slice information will be read from
  // remote.
  TAGGED_DCHECK(managed_by_aes_);
  TAGGED_DCHECK_GT(rpc_arg_->remote_disk_id(), -1);

  shared_ptr<ReplicaReadEgroupArg> remote_arg =
    make_shared<ReplicaReadEgroupArg>();
  remote_arg->set_extent_group_id(extent_group_id_);
  remote_arg->set_disk_id(rpc_arg_->remote_disk_id());

  // Setting qos priority.
  const Configuration::PtrConst& config = globals_->stargate->Config();
  if (StargateUtil::UseQosPriority(config)) {
    // 'qos_principal_name' is not used. It needs to be set due to legacy
    // reasons as it is a required proto field.
    remote_arg->set_qos_principal_name(string());
    remote_arg->set_qos_priority(qos_priority_);
    remote_arg->set_is_secondary(true);
  } else {
    remote_arg->set_qos_principal_name(
      StargateUtil::GetQosPrincipalName(qos_priority_, config));
  }

  remote_arg->set_managed_by_aes(true);
  remote_arg->set_extent_based_metadata_format(extent_based_metadata_format_);
  remote_arg->set_global_metadata_intent_sequence(
    global_metadata_intent_sequence_);

  // For AES egroups, we do not know the current applied intent sequence
  // at this point. Therefore, we will set this required proto field to -1.
  // The request will be served as long as the remote is at the expected
  // global metadata intent sequence in this case. We will rely on the
  // response of this RPC to know the current applied intent sequence,
  // incase further read requests need to be issued at that intent sequence.
  remote_arg->set_expected_intent_sequence(-1);

  if (rpc_arg_->ignore_corrupt_flag()) {
    remote_arg->set_ignore_corrupt_flag(true);
  }
  if (rpc_arg_->copy_corrupt_replica()) {
    remote_arg->set_copy_corrupt_replica(true);
  }
  if (rpc_arg_->untransform_slices()) {
    remote_arg->set_untransform_slices(true);
    remote_arg->set_transformation_type(rpc_arg_->transformation_type());
    for (int ii = 0; ii < rpc_arg_->transformation_type_list_size(); ++ii) {
      remote_arg->add_transformation_type_list(
        rpc_arg_->transformation_type_list(ii));
    }
    if (rpc_arg_->has_cipher_key_id()) {
      remote_arg->set_cipher_key_id(rpc_arg_->cipher_key_id());
    }
  }

  if (rpc_arg_->ilm_migration()) {
    // This replication is being done due to ILM.
    remote_arg->set_ilm_migration(true);
  }
  ++outstanding_work_units_;

  // TODO(Hema): If we are replicating an AES parity egroup, ensure to read
  // their info egroup metadata also.

  TAGGED_VLOG(1) << "Sending read replica for egroup with intent_seq "
                 << rpc_arg_->intent_sequence()
                 << " to remote replica " << rpc_arg_->remote_disk_id()
                 << " for reading physical state";

  Function<void(StargateError::Type,
                shared_ptr<string>,
                shared_ptr<ReplicaReadEgroupRet>,
                IOBuffer::Ptr&&)> done_cb =
    fe_->Bind(&ExtentGroupReplicateOp::ExtentGroupPhysicalStateFetched,
              this, _1, _2, _3, _4);
  remote_ifc_->ReplicaReadEgroup(move(remote_arg), move(done_cb));
}

//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReplicateOp::
ExtentGroupPhysicalStateFetched(
  StargateError::Type err,
  const shared_ptr<string>& err_detail,
  const shared_ptr<ReplicaReadEgroupRet>& remote_ret,
  IOBuffer::Ptr&& remote_payload) {

  TAGGED_DCHECK(fe_->IsWorkerThread());
  TAGGED_DCHECK(managed_by_aes_);

  // Record any activity traces returned in the RPC response into this op's
  // activity tracer.
  RecordActivityTraces(remote_ret->activity_traces());

  TRACE(at_, 0) << "Completed rpc to fetch physical state";

  if (MaybeInjectError(ErrorInjections::kReadReplicaRpcFailed, __LINE__)) {
    err = StargateError::kTimeout;
  }

  if (err != StargateError::kNoError) {
    TAGGED_LOG(WARNING) << "Attempt to fetch physical state from remote disk "
                        << rpc_arg_->remote_disk_id() << " failed with error "
                        << StargateError::ToString(err);
    error_ = err;
    MaybeFinish();
    return;
  }

  TAGGED_DCHECK(remote_ret->has_extent_group_metadata());
  MedusaExtentGroupIdMapEntry egid_entry(&remote_ret->extent_group_metadata());

  checksum_type_ = static_cast<ChecksumType>(remote_ret->checksum_type());

  if (!VerifyExtentGroupPhysicalState(egid_entry) ||
      MaybeInjectError(ErrorInjections::kChecksFailed, __LINE__)) {
    TAGGED_LOG(WARNING)
      << "Verification of physical state fetched from remote disk "
      << rpc_arg_->remote_disk_id() << " failed";

    MaybeFinish(StargateError::kRetry);
    return;
  }

  // Compute the disk usage diff that will be applied to this disk after this
  // replication using the egroup metadata fetched from the source.
  disk_usage_diff_.Clear();
  if (!extent_based_metadata_format_) {
    StargateUtil::FetchExtentGroupDiskUsage(
      &disk_usage_diff_,
      extent_group_id_,
      &egid_entry,
      globals_->stargate->Config());
    TAGGED_VLOG(3) << "Computed disk usage diff: "
                   << disk_usage_diff_.ShortDebugString();
  }

  // Populate 'slice_vec_' and calculate the transformed egroup size.
  const int num_slices = remote_ret->extent_group_metadata().slices_size();
  slice_vec_.reserve(num_slices);
  if (extent_based_metadata_format_) {
    slice_allocated_blocks_map_.reserve(num_slices);
    TAGGED_DCHECK_EQ(num_slices, remote_ret->allocated_block_bitmaps_size());
  }
  int transformed_egroup_size = 0;
  for (int xx = 0; xx < num_slices; ++xx) {
    // Only those slices that need to be written to disk go in 'slice_vec_'.
    const auto& slice_state = remote_ret->extent_group_metadata().slices(xx);
    if (slice_state.has_extent_group_offset()) {
      slice_vec_.emplace_back(
        ExtentGroupState::SliceState::Create(
          slice_state, true /* copy */, arena_));
      const int slice_end =
        slice_state.extent_group_offset() + slice_state.transformed_length() +
        slice_state.cushion();
      if (slice_end > transformed_egroup_size) {
        transformed_egroup_size = slice_end;
      }
      if (extent_based_metadata_format_) {
        slice_allocated_blocks_map_.emplace(
          slice_state.slice_id(),
          remote_ret->allocated_block_bitmaps(xx));
      }
    }
  }
  TAGGED_CHECK_GT(slice_vec_.size(), 0) << remote_ret->ShortDebugString();
  // Sort slice vector in increasing order of extent group offsets.
  sort(slice_vec_.begin(), slice_vec_.end(),
       ExtentGroupState::SliceState::SliceStateUPtrOrderIncreasingFileOffset);

  TAGGED_CHECK_EQ(slice_vec_index_, 0);
  int32 expected_payload_size = 0;
  for (size_t xx = 0; xx < slice_vec_.size(); ++xx) {
    expected_payload_size += slice_vec_[xx]->transformed_length();
    if (expected_payload_size < remote_payload->size()) {
      continue;
    }

    slice_vec_index_ = xx + 1;
    break;
  }
  // We should have data exactly till one of the slice's boundary.
  if (expected_payload_size != remote_payload->size()) {
    TAGGED_LOG(DFATAL)
      << "Unexpected size of data fetched from remote disk "
      << rpc_arg_->remote_disk_id() << ", "
      << OUTVARS(expected_payload_size, remote_payload->size());

    MaybeFinish(StargateError::kRetry);
    return;
  }
  TAGGED_CHECK_GT(slice_vec_index_, 0);
  TAGGED_CHECK_LE(slice_vec_index_, slice_vec_.size());

  if (disk_manager_->block_store_ || extent_based_metadata_format_) {
    TAGGED_DCHECK(!block_store_component_);
    int allocation_size = 0;
    if (extent_based_metadata_format_) {
      block_store_component_ = shared_state_->egroups_component_;
      const int block_size_log2 = block_store_component_->block_size_log_2();
      const int block_size_bytes =
        block_store_component_->block_size_bytes();
      // Let's go over all the slices and their corresponding allocated block
      // bitmap to calculate the allocation size.
      for (size_t ii = 0; ii < slice_vec_.size(); ++ii) {
        const auto& slice_state = slice_vec_[ii];
        if (!slice_state->has_extent_group_offset()) {
           continue;
        }

        const uint32 allocated_block_bitmap =
          slice_allocated_blocks_map_[slice_state->slice_id()];
        const int num_blocks =
          (slice_state->transformed_length() + block_size_bytes - 1) >>
          block_size_log2;
        if (slice_state->transformed_length() <
            slice_state->untransformed_length()) {
          // Compressed slice.
          allocation_size += (num_blocks << block_size_log2);
        } else {
          for (int jj = 0; jj < num_blocks; ++jj) {
            if ((1 << jj) & allocated_block_bitmap) {
              allocation_size += block_size_bytes;
            }
          }
        }
      }
      TAGGED_DCHECK_GT(allocation_size, 0);
      if (shared_state_->data_on_ext4_files()) {
        fallocated_egroup_size_ = ExtentGroupState::CalculateFallocateSize(
          allocation_size, block_size_bytes, disk_manager_);
      }
    } else {
      block_store_component_ = disk_manager_->block_store_component_;
      allocation_size = ExtentGroupState::CalculateFallocateSize(
        transformed_egroup_size,
        block_store_component_->block_size_bytes(),
        disk_manager_);
    }
    MaybeFetchCipherKey(
      allocation_size, move(egid_entry), remote_ret, move(remote_payload));
    return;
  }

  WriteTentativeWALRecordForAESExtentGroup(remote_ret, move(remote_payload));
}

//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReplicateOp::MaybeFetchCipherKey(
  const int allocation_size,
  MedusaExtentGroupIdMapEntry&& egid_entry,
  const shared_ptr<ReplicaReadEgroupRet>& remote_ret,
  IOBuffer::Ptr&& remote_payload) {

  if (!extent_based_metadata_format_ ||
      !verify_checksums_ || !egid_entry.control_block()->has_cipher_key_id()) {
    // We can skip fetching the cipher key if don't have to untransform to
    // verify the checksums or we don't have the cipher key id.
    AllocateBlocks(allocation_size, remote_ret, move(remote_payload));
    return;
  }

  cipher_key_id_ = egid_entry.control_block()->cipher_key_id();
  // Extract the encryption information from the metadata.
  for (int ii = 0;
       ii < egid_entry.control_block()->transformation_type_list_size();
       ++ii) {
    const DataTransformation::Type transform_type =
      egid_entry.control_block()->transformation_type_list(ii);
    if (DataTransformationType::IsEncryptionType(transform_type)) {
      encryption_type_vec_.emplace_back(transform_type);
    }
  }
  TAGGED_DCHECK(!encryption_type_vec_.empty());
  FetchCipherKey(
    Bind(&ExtentGroupReplicateOp::FetchCipherKeyDone, this, _1,
         allocation_size, remote_ret, bind_move(remote_payload)));
}

//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReplicateOp::AllocateBlocks(
  const int allocation_size,
  const shared_ptr<ReplicaReadEgroupRet>& remote_ret,
  IOBuffer::Ptr&& remote_payload) {

  if (shared_state_->data_on_ext4_files()) {
    TAGGED_DCHECK_EQ(
      allocation_size % block_store_component_->block_size_bytes(), 0)
      << OUTVARS(allocation_size);
    const int num_blocks =
      allocation_size >> block_store_component_->block_size_log_2();
    allocated_block_id_list_.Emplace();
    allocated_block_id_list_->EmplaceBack(shared_state_->Offset2BlockId(0),
                                          num_blocks);
    AllocateFromBlockStoreDone(remote_ret, move(remote_payload));
    return;
  }

  AllocateFromBlockStore<ExtentGroupReplicateOp>(
    0 /* logical_offset */, allocation_size,
    Bind(&ExtentGroupReplicateOp::AllocateFromBlockStoreDone, this,
         remote_ret, bind_move(remote_payload)),
    block_store_component_);
}

//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReplicateOp::FetchCipherKeyDone(
  const MantleErrorProto::Type error,
  const int allocation_size,
  const shared_ptr<ReplicaReadEgroupRet>& remote_ret,
  IOBuffer::Ptr&& remote_payload) {

  TAGGED_DCHECK(fe_->IsWorkerThread());
  TAGGED_DCHECK(extent_based_metadata_format_);
  TAGGED_DCHECK(verify_checksums_);

  // Unable to fetch the cipher key, abort the op.
  if (error != MantleErrorProto::kNoError) {
    error_ = StargateError::kCipherKeyFetchError;
    TAGGED_LOG(ERROR) << "Finishing op with error " << error_
                      << " due to cipher key fetch failure with error "
                      << error_ << " for key id: " << cipher_key_id_;
    MaybeFinish();
    return;
  }

  AllocateBlocks(allocation_size, remote_ret, move(remote_payload));
}

//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReplicateOp::
AllocateFromBlockStoreDone(
  const shared_ptr<ReplicaReadEgroupRet>& remote_ret,
  IOBuffer::Ptr&& remote_payload) {

  if (MaybeInjectError(ErrorInjections::kBlockAllocationFailed, __LINE__)) {
    MaybeFinish(StargateError::kDiskSpaceUnavailable);
    return;
  }

  if (!allocated_block_id_list_) {
    TAGGED_DCHECK_NE(error_, StargateError::kNoError);
    MaybeFinish();
    return;
  }

  if (extent_based_metadata_format_) {
    BlocksAllocationDone(remote_ret, move(remote_payload));
    return;
  }

  WriteTentativeWALRecordForAESExtentGroup(remote_ret, move(remote_payload));
}

//-----------------------------------------------------------------------------

bool ExtentStore::DiskManager::ExtentGroupReplicateOp::
VerifyExtentGroupPhysicalState(
  const MedusaExtentGroupIdMapEntry& egid_entry) const {

  ScopedArenaMark am;
  ArenaAllocator<void *> arena_alloc(am.arena());

  const Configuration::PtrConst& config = globals_->stargate->Config();

  TAGGED_DCHECK(rpc_arg_->has_owner_container_id());
  const ConfigurationProto::Container *container =
    config->LookupContainer(rpc_arg_->owner_container_id());
  TAGGED_CHECK(container) << rpc_arg_->owner_container_id();
  const int vdisk_block_size = container->vdisk_block_size();

  // Let's go over all the extent ids that are to be replicated and verify the
  // received physical state.
  bool physical_state_valid = true;

  LOG(INFO) << "RPC arg extent id list size:"
            << rpc_arg_->extent_id_list_size();
  LOG(INFO) << rpc_arg_->ShortDebugString();


  for (int ii = 0; ii < rpc_arg_->extent_id_list_size(); ++ii) {
    const ExtentId::PtrConst& extent_id =
      arena_alloc.MakeShared<ExtentId>(&rpc_arg_->extent_id_list(ii));

    ExtentState::ArenaUPtrConst extent_state =
      egid_entry.LookupExtent(extent_id, am.arena());
    if (!extent_state) {
      TAGGED_LOG(WARNING)
        << "Extent id " << extent_id->ToString()
        << " not found in physical state received from remote disk "
        << rpc_arg_->remote_disk_id();
      physical_state_valid = false;
      continue;
    }

    TAGGED_DCHECK(egid_entry.control_block()->has_default_slice_size());
    const int expected_num_slices = StargateUtil::ComputeNumSlicesInExtent(
      extent_id.get(),
      egid_entry.control_block()->default_slice_size(),
      vdisk_block_size);

    if (extent_state->slice_ids_size() != expected_num_slices) {
      TAGGED_LOG(WARNING) << "Fewer number of slices found in physical state "
                          << "received from remote disk "
                          << rpc_arg_->remote_disk_id() << " than expected; "
                          << "expected_num_slices:" << expected_num_slices
                          << " num_slices:" << extent_state->slice_ids_size();
      physical_state_valid = false;
      continue;
    }
  }
  if (!physical_state_valid) {
    ostringstream ss;
    ss << "Physical state validation failed for remote disk "
       << rpc_arg_->remote_disk_id() << " response: ";
    MedusaPrinterUtil::PrintExtentGroupIdMapEntry(&ss, &egid_entry);
    TAGGED_LOG(INFO) << ss.str();
  }

  return physical_state_valid;
}

//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReplicateOp::BlocksAllocationDone(
  const shared_ptr<ReplicaReadEgroupRet>& remote_ret,
  IOBuffer::Ptr&& remote_payload) {

  TAGGED_DCHECK(fe_->IsWorkerThread());

  TAGGED_DCHECK(allocated_block_id_list_);
  if (VLOG_IS_ON(1)) {
    for (const auto& extent : *allocated_block_id_list_) {
      TAGGED_VLOG(1) << "Allocated blocks (" << extent.first << ","
                     << extent.second << ")";
    }
  }

  // Prepare the metadata based on RPC response.
  CreateExtentBasedMetadataEntry(remote_ret);

  MaybeCrashForInjection(CrashInjections::kBeforeWritingTU, __LINE__);

  // Start the tentative update write.
  shared_state_->WriteTUEntry(
    arena_alloc_.MakeUnique<SharedExtentWriteState::TUEntry>(
      extent_group_id_, SharedExtentWriteState::kReplicateTUExtentIndex,
      intent_sequence_),
    nullptr /* tu_page */, arena_, at(),
    Bind(&ExtentGroupReplicateOp::WriteTUDone, this, _1, _2,
         bind_move(remote_payload), false /* removed */));
}

//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReplicateOp::
CreateExtentBasedMetadataEntry(
  const shared_ptr<ReplicaReadEgroupRet>& remote_ret) {

  TAGGED_DCHECK(fe_->IsWorkerThread());

  ScopedArenaMark am;

  MedusaExtentGroupIdMapEntry egid_entry(&remote_ret->extent_group_metadata());

  TAGGED_DCHECK_EQ(egid_entry.control_block()->replicas_size(), 1);
  extent_based_egroup_applied_is_ =
    egid_entry.control_block()->replicas(0).intent_sequence();
  const int num_transformations =
    egid_entry.control_block()->transformation_type_list_size();
  int num_slices = 0;

  const auto& block_id_list = *allocated_block_id_list_;
  int block_list_idx = -1;
  int64 next_block_id = -1;
  int num_blocks_remaining = 0;
  const int block_size_log2 = block_store_component_->block_size_log_2();
  const int block_size_bytes = block_store_component_->block_size_bytes();
  const int untransformed_slice_length =
    egid_entry.control_block()->default_slice_size();

  int transformed_usage_bytes = 0;
  int untransformed_usage_bytes = 0;
  int transformed_garbage_bytes = 0;

  // Prepare the slice group metadata based on the metadata received in the RPC
  // response.
  MedusaExtentGroupPhysicalStateEntry::SliceGroupMap slice_group_map;
  for (int ii = 0; ii < remote_ret->extent_physical_states_size(); ++ii) {
    auto extent_physical_state = remote_ret->extent_physical_states(ii);
    const ExtentId::PtrConst& extent_id =
      arena_alloc_.MakeShared<ExtentId>(
        &extent_physical_state.extent_id(), true /* copy */);
    extent_id_set_.emplace(extent_id);
    ExtentState::ArenaUPtrConst extent_state =
      egid_entry.LookupExtent(extent_id, am.arena());
    TAGGED_DCHECK(extent_state) << extent_id;

    SharedExtentWriteState::EgroupMetadataHolder metadata;
    num_slices = extent_state->slice_ids_size();
    metadata.InitSliceGroup(
      shared_state_->CreateSliceGroup(
        num_slices,
        untransformed_slice_length,
        num_transformations,
        arena_));

    const int first_slice_id = extent_state->slice_ids(0);
    TAGGED_DCHECK(extent_state->has_extent_index())
      << OUTVARS(extent_state->ShortDebugString());
    TAGGED_DCHECK_EQ(extent_state->extent_index(), first_slice_id / num_slices)
      << OUTVARS(extent_state->ShortDebugString(), first_slice_id, num_slices);
    FB_MUTATE(metadata.sg_fb, extent_index, extent_state->extent_index());

    FB_MUTATE(metadata.pstate_fb, global_metadata_intent_sequence,
              global_metadata_intent_sequence_);
    FB_MUTATE(metadata.pstate_fb, applied_intent_sequence,
              extent_physical_state.latest_applied_intent_sequence());
    const int64 latest_intent_sequence =
      extent_physical_state.largest_seen_intent_sequence();
    intent_sequence_ = latest_intent_sequence > intent_sequence_ ?
      latest_intent_sequence : intent_sequence_;
    FB_MUTATE(metadata.pstate_fb, latest_intent_sequence,
              latest_intent_sequence);
    FB_MUTATE(metadata.pstate_fb, last_mutator_incarnation_id,
              extent_physical_state.last_mutator_incarnation_id());
    FB_MUTATE(metadata.pstate_fb, mtime_secs,
              extent_physical_state.mtime_secs());
    FB_MUTATE(metadata.pstate_fb, curator_scan_execution_id,
              globals_->cluster_state->curator_scan_execution_id());
    FB_MUTATE(metadata.pstate_fb, is_ec_info_egroup,
              remote_ret->is_erasure_coded());

    // Set transformation types.
    bool compression_enabled = false;
    for (int jj = 0; jj < num_transformations; ++jj) {
      const DataTransformation::Type transform_type =
        egid_entry.control_block()->transformation_type_list(jj);
      metadata.pstate_fb->mutable_transformation_type_vec()->Mutate(
        jj, transform_type);
      if (DataTransformationType::IsCompressionType(transform_type)) {
        compression_enabled = true;
      }
    }
    // Fill the slice metadata.
    for (int jj = 0; jj < num_slices; ++jj) {
      const int slice_id = extent_state->slice_ids(jj);
      const auto& slice_state = egid_entry.LookupSlice(slice_id, am.arena());
      if (!slice_state->has_extent_group_offset()) {
        continue;
      }

      PhysicalSliceFB *const slice_state_fb =
        metadata.pstate_fb->mutable_slice_states()->GetMutableObject(jj);
      if (compression_enabled) {
        FB_MUTATE(slice_state_fb, transformed_length,
                  slice_state->transformed_length());
      }
      bool compressed_slice = false;
      if (slice_state->transformed_length() != untransformed_slice_length) {
        // Compressed slice.
        compressed_slice = true;
        TAGGED_DCHECK_EQ(slice_state->checksum_size(), 1)
          << OUTVARS(extent_id->ToString(), slice_id);
        FB_MUTATE(slice_state_fb, compressed_checksum,
                  slice_state->checksum(0));
        TAGGED_DCHECK_EQ(slice_state->logical_checksum_size(),
                         slice_state_fb->checksums_size())
          << OUTVARS(extent_id->ToString(), slice_id);
        for (int kk = 0; kk < slice_state->logical_checksum_size(); ++kk) {
          slice_state_fb->mutable_checksums()->Mutate(
            kk, slice_state->logical_checksum(kk));
        }
      } else {
        TAGGED_DCHECK_EQ(
          slice_state->checksum_size(), slice_state_fb->checksums_size())
          << OUTVARS(extent_id->ToString(), slice_id);
        for (int kk = 0; kk < slice_state->checksum_size(); ++kk) {
          slice_state_fb->mutable_checksums()->Mutate(
            kk, slice_state->checksum(kk));
        }
      }
      const int num_blocks =
        (slice_state->transformed_length() + block_size_bytes - 1) >>
        block_size_log2;
      const uint32 allocated_block_bitmap =
        slice_allocated_blocks_map_[slice_state->slice_id()];
      for (int kk = 0; kk < num_blocks; ++kk) {
        if (!compressed_slice && ((1 << kk) & allocated_block_bitmap) == 0) {
          continue;
        }

        if (num_blocks_remaining == 0) {
          ++block_list_idx;
          TAGGED_DCHECK_LT(block_list_idx, block_id_list.size())
            << OUTVARS(extent_id->ToString(), slice_id, num_blocks);
          next_block_id = block_id_list[block_list_idx].first;
          num_blocks_remaining = block_id_list[block_list_idx].second;
        }
        const int64 block_num = next_block_id >> block_size_log2;
        shared_state_->GetAndReplaceBlockNum(slice_state_fb, kk, block_num);
        next_block_id =
          BlockStoreUtil::NextBlockId(next_block_id, block_size_bytes);
        --num_blocks_remaining;
        transformed_usage_bytes += block_size_bytes;
      }
      untransformed_usage_bytes +=
        num_blocks > 0 ? untransformed_slice_length : 0;
      const int aligned_transformed_length =
        AlignUp(slice_state->transformed_length(), block_size_bytes);
      transformed_garbage_bytes +=
        aligned_transformed_length - slice_state->transformed_length();
    }

    slice_group_map.emplace(extent_id.get(), move(metadata.sg_fb_buf));
  }
  TAGGED_DCHECK_EQ(num_blocks_remaining, 0);

  SharedExtentWriteState::EgroupMetadataHolder metadata;
  metadata.InitControlBlock(shared_state_->CreateControlBlock(arena_));
  FB_MUTATE(metadata.cb_fb, slice_group_size, num_slices);
  FB_MUTATE(metadata.cb_fb, owner_vdisk_id,
            egid_entry.control_block()->owner_vdisk_id());
  FB_MUTATE(metadata.cb_fb, owner_container_id,
            egid_entry.control_block()->owner_container_id());
  if (egid_entry.control_block()->has_cipher_key_id()) {
    memcpy(metadata.cb_fb->mutable_cipher_key_id()->data(),
           egid_entry.control_block()->cipher_key_id().c_str(),
           egid_entry.control_block()->cipher_key_id().size());
    FB_MUTATE(metadata.cb_fb, cipher_key_id_valid, true);
  }
  FB_MUTATE(metadata.cb_fb, global_metadata_intent_sequence,
            egid_entry.control_block()->latest_intent_sequence());
  FB_MUTATE(metadata.cb_fb->mutable_erasure_coding_info(), is_ec_egroup,
            remote_ret->is_erasure_coded());
  if (shared_state_->data_on_ext4_files()) {
    const int block_size = metadata.cb_fb->block_size();
    TAGGED_DCHECK_GT(block_size, 0);
    TAGGED_DCHECK_EQ(transformed_usage_bytes % block_size, 0)
      << OUTVARS(transformed_usage_bytes, block_size);
    TAGGED_DCHECK_GT(fallocated_egroup_size_, 0);
    TAGGED_DCHECK_EQ(fallocated_egroup_size_ % block_size, 0)
      << OUTVARS(fallocated_egroup_size_, block_size);
    FB_MUTATE(metadata.cb_fb, transformed_size_blocks,
              transformed_usage_bytes / block_size);
    FB_MUTATE(metadata.cb_fb, fallocate_size_blocks,
              fallocated_egroup_size_ / block_size);
  }

  // We will cache the prepared mvalue while egroup data is being replicated
  // from remote. Once the data is replicated we will push this mvalue to the
  // B-Tree AESDB.
  MedusaValue::Ptr mvalue = make_shared<MedusaValue>();
  mvalue->epoch = Medusa::MedusaValue::kInvalidEpoch;
  mvalue->timestamp = 0;
  mvalue->extent_group_physical_state_entry =
    arena_alloc_.MakeShared<MedusaExtentGroupPhysicalStateEntry>(
      extent_group_id_,
      move(metadata.cb_fb_buf),
      &extent_id_set_,
      nullptr /* extent_map */,
      &slice_group_map);
  TAGGED_VLOG(3)
    << "Prepared metadata entry: "
    << mvalue->extent_group_physical_state_entry->ShortDebugString();

  // Compute the disk usage diff that will be applied to this disk after this
  // replication using the egroup metadata fetched from the source.
  disk_usage_diff_.Clear();
  disk_usage_diff_.add_containers(
    egid_entry.control_block()->owner_container_id());
  disk_usage_diff_.add_container_unshared_usage();
  disk_usage_diff_.add_container_deduped_usage();
  disk_usage_diff_.add_container_garbage();
  disk_usage_diff_.add_container_ec_parity();
  disk_usage_diff_.mutable_garbage()->set_transformed(
    transformed_garbage_bytes);
  disk_usage_diff_.mutable_container_garbage(0)->set_transformed(
    transformed_garbage_bytes);
  disk_usage_diff_.mutable_total_usage()->set_transformed(
    transformed_usage_bytes);
  disk_usage_diff_.mutable_container_unshared_usage(0)->set_transformed(
    transformed_usage_bytes);
  disk_usage_diff_.mutable_total_usage()->set_untransformed(
    untransformed_usage_bytes);
  disk_usage_diff_.mutable_container_unshared_usage(0)->set_untransformed(
    untransformed_usage_bytes);
  TAGGED_VLOG(3) << "Computed disk usage diff: "
                 << disk_usage_diff_.ShortDebugString();

  egid_vec_.emplace_back(extent_group_id_);
  mvalue_vec_.emplace_back(move(mvalue));
}

//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReplicateOp::WriteTUDone(
  TUPage::Ptr tu_page,
  const StargateError::Type error,
  IOBuffer::Ptr&& remote_payload,
  const bool removed) {

  TAGGED_DCHECK(fe_->IsWorkerThread());

  if (error != StargateError::kNoError) {
    TAGGED_DCHECK_EQ(error, StargateError::kDiskSpaceUnavailable);
    TAGGED_DCHECK(!removed);
    MaybeFinish(error);
    return;
  }

  if (removed) {
    TAGGED_DCHECK(!remote_payload);
    MaybeCrashForInjection(CrashInjections::kAfterFinalization, __LINE__);
    MaybeFinish();
    return;
  }

  tu_page_ = move(tu_page);

  TRACE(at_, 0) << "Tentative update flushed";
  TAGGED_VLOG(1) << "Tentative update flushed";

  written_tentative_update_ = true;

  MaybeCrashForInjection(CrashInjections::kAfterWritingTU, __LINE__);

  if (shared_state_->data_on_ext4_files()) {
    auto cont_cb = BindInFe<void()>(
      fe_, &ExtentGroupReplicateOp::EgroupDescriptorFetchComplete, this,
      bind_move(remote_payload));
    const uint64 source_id =
      disk_manager_->io_categorizer_disk_id_ |
        migration_egroup_io_categorizer_id_;
    IOCategorizer::Categorize(source_id, IOCategorizer::Op::kFallocate, 0,
                              fallocated_egroup_size_);
    GetEgroupDescriptor(extent_group_id_, move(cont_cb),
                        fallocated_egroup_size_);
    return;
  }

  // Start writing the data as fetched from the remote.
  block_store_descriptor_ =
    arena_alloc_.MakeUnique<
      BlockStoreEgroupManager::BlockStoreExtentDescriptor>(
        shared_state_->egroup_manager_.get(), extent_group_id_);
  desc_ = block_store_descriptor_.get();
  EgroupDescriptorFetchComplete(move(remote_payload));
}

//-----------------------------------------------------------------------------

void
ExtentStore::DiskManager::ExtentGroupReplicateOp::WriteTentativeWALRecord() {
  TAGGED_DCHECK(!managed_by_aes_);
  TAGGED_DCHECK(deleted_extent_id_set_.empty());
  TAGGED_DCHECK(deleted_slice_id_set_.empty());
  TAGGED_DCHECK(!disk_manager_->block_store_);

  // Write a tentative WAL record containing all the slices in this extent
  // group - this also includes those slices that don't have an extent group
  // offset. The purpose of logging such slices is so we correctly record the
  // max used slice id in the WAL log.
  ScopedArenaMark am;
  DiskWALRecord::Ptr record = DiskWALRecord::Create(am.arena());

  auto egroup_update = record->add_egroup_update();
  egroup_update->set_extent_group_id(extent_group_id_);

  // We will log this write at the latest intent sequence received in the RPC.
  // This will ensure that if this extent group was present on this node in the
  // past such that the disk WAL still contains some of the older tentative
  // updates, then those can be identified as stale and not incorrectly applied
  // to this new extent group state. All those older tentative updates will
  // have an intent sequence that is smaller than the latest intent sequence
  // received in this RPC. At the end of the replication when the extent group
  // is finalized on this node, the applied intent sequence will be set to the
  // expected intent sequence which is the one present on the other replicas of
  // this extent group in Medusa.
  intent_sequence_ = rpc_arg_->intent_sequence();

  auto tentative = egroup_update->mutable_add_tentative();
  tentative->set_intent_sequence(intent_sequence_);

  TAGGED_DCHECK_EQ(expected_intent_sequence_,
                   rpc_arg_->expected_intent_sequence());
  tentative->set_post_replication_intent_sequence(
    expected_intent_sequence_);

  tentative->reserve_end_slice_state(rpc_arg_->slices_size());
  // Add slices to tentative WAL record.
  for (int xx = 0; xx < rpc_arg_->slices_size(); ++xx) {
    ExtentGroupState::SliceStatePtr ss =
      ExtentGroupState::SliceState::Create(&rpc_arg_->slices(xx),
                                           am.arena());
    ss->CopyTo(tentative->add_end_slice_state());
  }

  // Set the up alignment hint for egroup file tentative allocation size.
  // For egroup replication, we allocate the size of entire egroup upfront
  // hence we don't need to do up align it any further.
  egroup_update->set_fallocate_up_alignment(0 /* no up alignment */);

  record->Finish();

  TRACE(at_, 0) << "Writing tentative delta record";

  IOBuffer::Ptr remote_payload = rpc_arg_->remote_disk_id() < 0 ?
    rpc_->request_payload()->Clone() : shared_ptr<IOBuffer>();
  auto flush_cb = BindInFe<void()>(
    fe_, arena_, &ExtentGroupReplicateOp::TentativeUpdateFlushed, this,
    bind_move(remote_payload));
  disk_manager_->disk_wal_->LogDeltaRecord(record.get(), move(flush_cb));
}

//-----------------------------------------------------------------------------

void
ExtentStore::DiskManager::ExtentGroupReplicateOp::
WriteTentativeWALRecordForAESExtentGroup(
  const shared_ptr<interface::ReplicaReadEgroupRet>& remote_ret,
  IOBuffer::Ptr&& remote_payload) {

  TAGGED_DCHECK(managed_by_aes_);

  // Check if we are replicating an AES parity egroup from the source. If so,
  // create new egroup ids for storing the metadata of the info egroups
  // associated to the parity egroup.
  const int32 num_info_egroups = remote_ret->info_egroups_size();
  if (num_info_egroups && local_info_egroup_id_vec_.empty()) {
    // Verify sanity of INFO egroup metadata. This is a guardrail added in the
    // context of ENG-411813 where we see that the INFO metadata replica is
    // logged without any slice information, even when the source metadata was
    // found valid later. Adding debug logging and preventing such an entry
    // from getting silently persisted.
    for (int ii = 0; ii < num_info_egroups; ++ii) {
      const auto& info_egroup = remote_ret->info_egroups().Get(ii);
      if (!info_egroup.has_extent_group_metadata() ||
          !info_egroup.extent_group_metadata().has_control_block() ||
          info_egroup.extent_group_metadata().extents_size() == 0U ||
          info_egroup.extent_group_metadata().slices_size() == 0U) {
        TAGGED_LOG(DFATAL)
          << "Invalid info replica metadata received: "
          << OUTVARS(ii, extent_group_id_,
                     info_egroup.ShortDebugString());

        error_ = StargateError::kRetry;
        MaybeFinish();
        return;
      }
    }


    Function<void()> cont_cb =
      bind(&ExtentGroupReplicateOp::WriteTentativeWALRecordForAESExtentGroup,
           this, remote_ret, bind_move(remote_payload));
    CreateInfoEgroupIds(num_info_egroups, move(cont_cb));
    return;
  }

  // Write a tentative WAL record containing all the extents and slices in this
  // extent group - this also includes those slices that don't have an extent
  // group offset. The purpose of logging such slices is so we correctly record
  // the max used slice id in the WAL log.
  ScopedArenaMark am;
  DiskWALRecord::Ptr record = DiskWALRecord::Create(am.arena());

  // We will log this write at the largest seen intent sequence with post
  // replication intent sequence as the latest applied intent sequence. We get
  // both largest seen and latest applied intent sequences from remote.
  TAGGED_CHECK(remote_ret->has_largest_seen_intent_sequence())
    << remote_ret->ShortDebugString();
  intent_sequence_ = remote_ret->largest_seen_intent_sequence();
  TAGGED_CHECK(remote_ret->has_last_mutator_incarnation_id());

  // For AES egroups, we will rely on the replica intent sequence retrieved
  // from the RPC response for issuing any further read requests.
  TAGGED_DCHECK_EQ(expected_intent_sequence_, -1)
    << rpc_arg_->ShortDebugString();
  expected_intent_sequence_ =
    remote_ret->extent_group_metadata().control_block().replicas(0).
      intent_sequence();

  PrepareWALRecordForAESExtentGroup(
    extent_group_id_,
    remote_ret.get(),
    nullptr /* info_egroup */,
    record,
    am.arena());

  // Include the updates for the info egroups also, in the same WAL record.
  if (remote_ret->info_egroups_size() > 0) {

    TAGGED_DCHECK_EQ(local_info_egroup_id_vec_.size(),
                     remote_ret->info_egroups_size());
    for (int ii = 0; ii < remote_ret->info_egroups_size(); ++ii) {
      const auto& info_egroup = remote_ret->info_egroups().Get(ii);

      // Since we are creating new egroup ids to store the associated info
      // egroup metadata, we do not have any pre-existing AESDB keys that
      // need to be cleaned up. Any older version of the parity egroup in
      // this replica would have been marked for deletion prior to this
      // replication, which would have marked its previous associated info
      // egroups for deletion as well. Eventual flushing of map4 entries
      // will take care of deleting the old info egroup's keys.
      PrepareWALRecordForAESExtentGroup(
        local_info_egroup_id_vec_[ii],
        nullptr /* remote_ret */,
        &info_egroup,
        record,
        am.arena());
    }
  }

  record->Finish();

  TRACE(at_, 0) << "Writing tentative delta record";

  auto flush_cb = BindInFe<void()>(
    fe_, arena_, &ExtentGroupReplicateOp::TentativeUpdateFlushed, this,
    bind_move(remote_payload));
  disk_manager_->disk_wal_->LogDeltaRecord(record.get(), move(flush_cb));
}

//-----------------------------------------------------------------------------

void
ExtentStore::DiskManager::ExtentGroupReplicateOp::
PrepareWALRecordForAESExtentGroup(
  const int64 extent_group_id,
  const ReplicaReadEgroupRet *remote_ret,
  const ReplicaReadEgroupRet::InfoEgroupMetadata *info_egroup,
  const DiskWALRecord::Ptr& record,
  Arena *const arena) {

  TAGGED_DCHECK(managed_by_aes_);

  // Determine if this WAL record updation is for the egroup primarily owned by
  // the op.
  TAGGED_DCHECK(remote_ret || info_egroup) << extent_group_id;

  AESPhysicalMetadata egroup_metadata(
    extent_group_id, remote_ret, info_egroup, rpc_arg_->intent_sequence());
  const bool owner_egroup_write = egroup_metadata.owner_egroup_write;
  TAGGED_DCHECK(owner_egroup_write || extent_group_id_ != extent_group_id)
    << extent_group_id;

  // Clear any pre-existing tombstone as we are re-creating the egroup on this
  // disk, and have already recorded any keys that need to be deleted in
  // deleted_extent_id_set_ and deleted_slice_id_set_.
  //
  // For child AES info egroups that are also included as part of the parity
  // egroup WAL record, we have already created fresh egroup ids for storing
  // their metadata and therefore, must not have any pre-existing tombstone
  // that needs to be cleared.
  if (owner_egroup_write) {
    auto egroup_update = record->add_egroup_update();
    egroup_update->set_extent_group_id(extent_group_id);
    egroup_update->set_managed_by_aes(true);
    egroup_update->set_delete_egroup(false);
  }

  auto egroup_update = record->add_egroup_update();
  egroup_update->set_extent_group_id(extent_group_id);
  egroup_update->set_managed_by_aes(true);
  egroup_update->set_global_metadata_intent_sequence(
    egroup_metadata.global_metadata_intent_sequence);
  egroup_update->set_highest_committed_intent_sequence(
    egroup_metadata.highest_committed_intent_sequence);
  egroup_update->set_checksum_type(static_cast<int>(checksum_type_));

  const MedusaExtentGroupIdMapEntryProto& egid_entry_proto =
    *egroup_metadata.egid_entry_proto;
  const MedusaExtentGroupIdMapEntryProto::ControlBlock &cb =
    egid_entry_proto.control_block();

  TAGGED_CHECK(cb.has_slice_group_size());
  TAGGED_CHECK(cb.has_slices_stored_by_id());

  egroup_update->set_last_mutator_incarnation_id(
    egroup_metadata.last_mutator_incarnation_id);

  TAGGED_CHECK_EQ(cb.replicas_size(), 1U);
  TAGGED_CHECK(cb.replicas(0).has_intent_sequence())
    << egid_entry_proto.ShortDebugString();

  // Include EC specific information for the egroup.
  AddErasureCodeInfo(egroup_metadata, egroup_update.get());

  TAGGED_DCHECK(cb.has_write_time_usecs());
  TAGGED_DCHECK_GE(cb.write_time_usecs(), 0);
  if (cb.has_write_time_usecs()) {
    egroup_update->set_mtime_secs(cb.write_time_usecs() / 1000000);
  }

  TAGGED_DCHECK(!owner_egroup_write ||
                intent_sequence_ ==
                  egroup_metadata.largest_seen_intent_sequence)
    << "owner_egroup_write " << owner_egroup_write
    << " intent_sequence " << intent_sequence_
    << " largest_seen_intent_sequence "
    << egroup_metadata.largest_seen_intent_sequence;

  auto tentative = egroup_update->mutable_add_tentative();
  tentative->set_intent_sequence(
    egroup_metadata.largest_seen_intent_sequence);
  tentative->set_post_replication_intent_sequence(
    cb.replicas(0).intent_sequence());

  // Add commont control block fields to tentative WAL record.
  AddControlBlockFieldsToAESExtentGroupUpdate(egroup_update.get(), cb);

  // Add extents to tentative WAL record.
  ArenaUnorderedSet<int> live_slices(arena);
  AddExtentsToAESExtentGroupTentative(
    tentative.get(), egid_entry_proto,
    owner_egroup_write, &deleted_extent_id_set_,
    &live_slices);

  // Add slices to tentative WAL record.
  AddSlicesToAESExtentGroupTentative(
    tentative.get(), egid_entry_proto, live_slices,
    owner_egroup_write, &deleted_slice_id_set_, arena);

  if (owner_egroup_write) {
    AddOwnerEgroupMetadata(egroup_metadata.is_parity_egroup, remote_ret,
                           tentative.get(), egroup_update.get(), arena);
  }

  TAGGED_VLOG(2) << "Logging TU " << egroup_update->ShortDebugString();
}

//-----------------------------------------------------------------------------

ExtentStore::DiskManager::ExtentGroupReplicateOp::AESPhysicalMetadata::
AESPhysicalMetadata(
  const int64 extent_group_id,
  const ReplicaReadEgroupRet *remote_ret,
  const ReplicaReadEgroupRet::InfoEgroupMetadata *info_egroup,
  const int64 rpc_arg_intent_sequence) :
  egroup_id(extent_group_id),
  owner_egroup_write(remote_ret ? true : false),
  is_erasure_coded(owner_egroup_write ? remote_ret->is_erasure_coded() :
    true),
  is_parity_egroup(owner_egroup_write ? remote_ret->info_egroups_size() > 0 :
    false),
  egid_entry_proto(owner_egroup_write ? &remote_ret->extent_group_metadata() :
                   &info_egroup->extent_group_metadata()),
  highest_committed_intent_sequence(
    owner_egroup_write ? remote_ret->highest_committed_intent_sequence() :
      info_egroup->highest_committed_intent_sequence()),
  largest_seen_intent_sequence(
    owner_egroup_write ? remote_ret->largest_seen_intent_sequence() :
      info_egroup->largest_seen_intent_sequence()),
  last_mutator_incarnation_id(
    owner_egroup_write ? remote_ret->last_mutator_incarnation_id() :
      info_egroup->vdisk_incarnation_id()),
  global_metadata_intent_sequence(
    owner_egroup_write ? rpc_arg_intent_sequence :
      egid_entry_proto->control_block().latest_intent_sequence()) {
}

//-----------------------------------------------------------------------------

void
ExtentStore::DiskManager::ExtentGroupReplicateOp::AddErasureCodeInfo(
  const AESPhysicalMetadata& egroup_metadata,
  ExtentGroupUpdateInterface *const egroup_update) {

  if (egroup_metadata.is_erasure_coded) {
    TAGGED_VLOG(2) << "Adding EC info for the egroup "
                   << egroup_metadata.egroup_id
                   << " is_parity " << egroup_metadata.is_parity_egroup
                   << " is_owner " << egroup_metadata.owner_egroup_write;
    auto ec_info = egroup_update->mutable_ec_info();
    ec_info->set_is_parity_egroup(egroup_metadata.is_parity_egroup);
    ec_info->set_parity_egroup_id(
      egroup_metadata.owner_egroup_write ? -1 : extent_group_id_);
  }
}

//-----------------------------------------------------------------------------

void
ExtentStore::DiskManager::ExtentGroupReplicateOp::AddOwnerEgroupMetadata(
  const bool is_parity,
  const ReplicaReadEgroupRet *const remote_ret,
  AddTentativeInterface *const tentative,
  ExtentGroupUpdateInterface *const egroup_update,
  Arena *const arena) {

  // Add all those extent ids and slice ids to the tentative WAL record that
  // were deleted.
  tentative->reserve_delete_extent_id(deleted_extent_id_set_.size());
  for (const ExtentId::PtrConst& extent_id : deleted_extent_id_set_) {
    auto deleted_eid = tentative->add_delete_extent_id();
    deleted_eid->CopyFrom(*extent_id->extent_id_proto());
  }

  tentative->reserve_delete_slice_id(deleted_slice_id_set_.size());
  for (const int32 slice_id : deleted_slice_id_set_) {
    tentative->add_delete_slice_id(slice_id);
  }

  if (allocated_block_id_list_) {
    AddPhysicalSegmentsToTentativeRecord<ExtentGroupReplicateOp>(
      tentative, kExtentGroupPhysicalStateDefaultBlockSize, arena);

    // When using the block store, the egroup is always aligned at the
    // component's block boundary.
    egroup_update->set_fallocate_up_alignment(
      block_store_component_->block_size_bytes());
  } else {
    // Set the up alignment hint for egroup file tentative allocation size.
    // For egroup replication, we allocate the size of entire egroup upfront
    // hence we don't need to do up align it any further.
    egroup_update->set_fallocate_up_alignment(0 /* no up alignment */);
  }

  egroup_update->set_is_primary(rpc_arg_->is_primary());

  if (is_parity) {
    TAGGED_DCHECK_EQ(local_info_egroup_id_vec_.size(),
                     remote_ret->info_egroups_size());
    auto aes_ec_tu = tentative->mutable_aes_ec_tentative_info();
    for (int ii = 0; ii < remote_ret->info_egroups_size(); ++ii) {
      const auto& info_egroup = remote_ret->info_egroups().Get(ii);
      auto info_egroup_tu = aes_ec_tu->add_info_egroups();
      info_egroup_tu->set_global_extent_group_id(
        info_egroup.extent_group_id());
      info_egroup_tu->set_local_extent_group_id(
        local_info_egroup_id_vec_[ii]);
      info_egroup_tu->set_intent_sequence(
        info_egroup.largest_seen_intent_sequence());
    }
    aes_ec_tu->set_update_type(static_cast<int>(
      ExtentGroupState::ErasureCodeParityUpdateType::kEncode));
  }
}

//-----------------------------------------------------------------------------

void
ExtentStore::DiskManager::ExtentGroupReplicateOp::TentativeUpdateFlushed(
  IOBuffer::Ptr&& remote_payload) {

  TAGGED_DCHECK(fe_->IsWorkerThread());

  if (FLAGS_estore_experimental_replication_delay_after_tu ==
        extent_group_id_) {
    TRACE(at_, 0) << "Delaying after writing TU";
    TAGGED_LOG(INFO) << "Delaying after writing TU";
    globals_->delay_ed->Add(BindInFe<void()>(
      fe_, &ExtentGroupReplicateOp::TentativeUpdateFlushed, this,
      bind_move(remote_payload)), 1000);
    return;
  }

  written_tentative_update_ = true;

  TRACE(at_, 0) << "Tentative delta record written";

  if (allocated_block_id_list_) {
    // We can now finalize the physical block allocations in the block store.
    TAGGED_VLOG(1) << "Finalizing physical block allocations";
    blockstore_finalize_issued_ = true;
    blockstore_finalize_outstanding_ = true;
    BlockIdList block_id_list = allocated_block_id_list_->GetCtr();
    Function<void()> finalize_alloc_done_cb =
      FuncDisabled<void()>(
        [this]() {
          TAGGED_DCHECK(fe_->IsWorkerThread());
          TAGGED_DCHECK(blockstore_finalize_outstanding_);
          TAGGED_DCHECK(blockstore_finalize_issued_);
          blockstore_finalize_outstanding_ = false;
          if (waiting_to_finalize_) {
            FinalizeTentativeUpdate();
          } else if (waiting_to_finish_) {
            MaybeFinish();
          }
        });

    fe_->Wrap(&finalize_alloc_done_cb);

    if (block_id_list.size() == 1 && block_id_list[0].second == 1) {
      block_store_component_->FinalizeAllocation(
        block_id_list[0].first, false /* abort */,
        move(finalize_alloc_done_cb), at());
    } else {
      block_store_component_->FinalizeAllocation(
        move(block_id_list), false /* abort */,
        move(finalize_alloc_done_cb), at());
    }
  }

  // Fetch the descriptor for the extent group. This will take care of creating
  // the file and fallocating it if required.
  ExtentGroupState::Ptr eg_state =
    disk_manager_->TryFetchExtentGroupState(extent_group_id_);
  TAGGED_DCHECK(eg_state);

  // The tentative update will set the tentative_fallocate_size if the
  // transformed size of the egroup is going to exceed the previously
  // fallocated egroup size. We will pass this value to the GetEgroupDescriptor
  // so that the egroup file is appropriately extended.
  fallocated_egroup_size_ = eg_state->tentative_fallocate_size();

  if (disk_manager_->block_store_ && !FLAGS_estore_experimental_sink_data) {
    // The extent group state did not exist (or was zombie or represented a
    // previous to_remove egroup) when this op started, so the block store
    // descriptor needs to be initialized with the newly created extent group
    // state's physical segment vector.
    TAGGED_DCHECK(block_store_descriptor_.get());
    auto desc = down_cast<BlockStoreEgroupManager::BlockStoreDescriptor *>(
      block_store_descriptor_.get());
    desc->set_physical_segment_vec(&eg_state->physical_segment_vec());
    if (!disk_manager_->CanFallocate(fallocated_egroup_size_)) {
      fallocate_failed_ = true;
      desc_ = nullptr;
    } else {
      desc_ = desc;
    }
    EgroupDescriptorFetchComplete(move(remote_payload));
    return;
  }

  auto cont_cb = BindInFe<void()>(
    fe_, &ExtentGroupReplicateOp::EgroupDescriptorFetchComplete, this,
    bind_move(remote_payload));
  const uint64 source_id =
    disk_manager_->io_categorizer_disk_id_ |
      migration_egroup_io_categorizer_id_;
  IOCategorizer::Categorize(source_id, IOCategorizer::Op::kFallocate, 0,
                            fallocated_egroup_size_);
  GetEgroupDescriptor(extent_group_id_, move(cont_cb),
                      fallocated_egroup_size_);

  MAYBE_INJECT_CRASH();
}

//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReplicateOp::
EgroupDescriptorFetchComplete(IOBuffer::Ptr&& remote_payload) {

  TAGGED_DCHECK(fe_->IsWorkerThread());

  egroup_file_may_exist_ = true;
  if (MaybeInjectError(ErrorInjections::kFallocateFailed, __LINE__)) {
    ReleaseEgroupDescriptor();
    fallocate_failed_ = true;
  }
  // Check if we were able to successfully create the extent group file.
  if (!desc_) {
    TAGGED_DCHECK(!disk_manager_->block_store_ || fallocate_failed_);
    // If we have run out of disk space, return kDiskSpaceUnavailable. The top
    // level egroup can handle this failure gracefully by marking the disk as
    // full and will try to select some other replica disk.
    error_ = fallocate_failed_ ? StargateError::kDiskSpaceUnavailable :
               StargateError::kDataCorrupt;
    MaybeFinish();
    return;
  }

  // Initially if we have received any data, we do so for first batch of slices
  // in the increasing order of offset in extent group file. Since it's the
  // very first payload, we get data starting from slice 0 for
  // 'slice_vec_index_' number of slices.
  SliceRange slice_range = make_pair(0, slice_vec_index_);
  MaybeStartWorkUnit();
  if (!remote_payload) {
    return;
  }

  // We already have some data with us, let's write it.
  TAGGED_DCHECK_GT(remote_payload->size(), 0);
  WorkUnitIssueDiskWrite(slice_range, move(remote_payload));
}

//-----------------------------------------------------------------------------

bool ExtentStore::DiskManager::ExtentGroupReplicateOp::
ShouldAggregateWorkUnits() const {
  return FLAGS_estore_experimental_aggregate_work_units ||
         !disk_manager_->EgroupDataUpdateSupported();
}

//-----------------------------------------------------------------------------

void
ExtentStore::DiskManager::ExtentGroupReplicateOp::MaybeStartWorkUnit() {
  TAGGED_DCHECK(fe_->IsWorkerThread());

  if (error_ == StargateError::kNoError && RpcHasTimedOut()) {
    // No need to continue doing work if the corresponding RPC has already
    // timed out.
    error_ = StargateError::kRetry;
  }

  if (error_ != StargateError::kNoError) {
    // No need to fetch more.
    if (outstanding_work_units_ == 0) {
      TAGGED_VLOG(0)
        << "Replication failed with error " << StargateError::ToString(error_);

      MaybeFinish();
    }
    return;
  }

  if (slice_vec_index_ * 1ULL >= slice_vec_.size()) {
    // Nothing more to fetch.
    if (outstanding_work_units_ == 0) {
      // Make sure we're marked as active again.
      if (inactive_ && FLAGS_estore_replication_flip_active_status) {
        FlipActiveStatus();
      }
      if (!work_unit_replica_read_map_.empty()) {
        TAGGED_DCHECK(ShouldAggregateWorkUnits());
        IssueAggregatedDiskWrite();
        return;
      }

      // Issue a Flush.
      IssueFlush();
    }
    return;
  }

  while (outstanding_work_units_ < FLAGS_estore_replication_max_work_units &&
         slice_vec_index_ * 1ULL < slice_vec_.size()) {
    // Start a work unit. This work unit will first fetch some slices from the
    // remote replica.

    shared_ptr<ReplicaReadEgroupArg> remote_arg =
      make_shared<ReplicaReadEgroupArg>();
    remote_arg->set_extent_group_id(extent_group_id_);
    remote_arg->set_disk_id(rpc_arg_->remote_disk_id());

    // Setting qos priority.
    const Configuration::PtrConst& config = globals_->stargate->Config();
    if (StargateUtil::UseQosPriority(config)) {
      // 'qos_principal_name' is not used. It needs to be set due to legacy
      // reasons as it is a required proto field.
      remote_arg->set_qos_principal_name(string());
      remote_arg->set_qos_priority(qos_priority_);
      remote_arg->set_is_secondary(true);
    } else {
      remote_arg->set_qos_principal_name(
        StargateUtil::GetQosPrincipalName(qos_priority_, config));
    }

    if (managed_by_aes_) {
      remote_arg->set_managed_by_aes(managed_by_aes_);
      remote_arg->set_extent_based_metadata_format(
        extent_based_metadata_format_);
    }

    // Only send the classification name to the remote node.
    if (remote_stargate_handle_ != globals_->netsub->local_stargate_handle()) {
      remote_arg->clear_classification_id();
    }

    remote_arg->set_global_metadata_intent_sequence(
      global_metadata_intent_sequence_);
    remote_arg->set_expected_intent_sequence(expected_intent_sequence_);
    if (rpc_arg_->ilm_migration()) {
      // This replication is being done due to ILM.
      remote_arg->set_ilm_migration(true);
    }

    if (rpc_arg_->ignore_corrupt_flag()) {
      remote_arg->set_ignore_corrupt_flag(true);
    }
    if (rpc_arg_->copy_corrupt_replica()) {
      remote_arg->set_copy_corrupt_replica(true);
    }
    if (rpc_arg_->untransform_slices()) {
      remote_arg->set_untransform_slices(true);
      remote_arg->set_transformation_type(rpc_arg_->transformation_type());
      for (int ii = 0; ii < rpc_arg_->transformation_type_list_size(); ++ii) {
        remote_arg->add_transformation_type_list(
          rpc_arg_->transformation_type_list(ii));
      }
      if (rpc_arg_->has_cipher_key_id()) {
        remote_arg->set_cipher_key_id(rpc_arg_->cipher_key_id());
      }
    }

    const int32 start_slice_vec_index = slice_vec_index_;
    int32 work_unit_bytes = 0;
    while (slice_vec_index_ * 1ULL < slice_vec_.size() &&
           work_unit_bytes < max_work_unit_bytes_) {

      slice_vec_[slice_vec_index_]->CopyTo(remote_arg->add_slices());
      TAGGED_CHECK(slice_vec_[slice_vec_index_]->has_transformed_length());
      work_unit_bytes += slice_vec_[slice_vec_index_]->transformed_length();
      ++slice_vec_index_;
    }
    ++outstanding_work_units_;
    ++outstanding_rpcs_;

    if (VLOG_IS_ON(1)) {
      ostringstream msg;
      msg << "Sending read replica for egroup " << extent_group_id_
          << " intent_seq " << expected_intent_sequence_
          << " global_metadata_intent_seq " << global_metadata_intent_sequence_
          << " to remote replica " << rpc_arg_->remote_disk_id()
          << " with slices";
      for (int xx = 0; xx < remote_arg->slices_size(); ++xx) {
        msg << " " << remote_arg->slices(xx).slice_id();
      }

      TAGGED_LOG(INFO) << msg.str();
    }

    Function<void(StargateError::Type,
                  shared_ptr<string>,
                  shared_ptr<ReplicaReadEgroupRet>,
                  IOBuffer::Ptr&&)> done_cb =
      Bind(&ExtentGroupReplicateOp::WorkUnitReplicaReadDone,
           this, _1, _2, _3, _4,
           make_pair(start_slice_vec_index, remote_arg->slices_size()));
    fe_->Wrap(&done_cb);
    remote_ifc_->ReplicaReadEgroup(move(remote_arg), move(done_cb));
  }

  if (outstanding_rpcs_ == outstanding_work_units_ && !inactive_ &&
      FLAGS_estore_replication_flip_active_status) {
    // We're doing no local work - we're only waiting on remote RPCs. Mark
    // this op inactive if not so already.
    FlipActiveStatus();
  }

  TRACE(at_, 0) << "Outstanding work_units=" << outstanding_work_units_
                << " rpcs=" << outstanding_rpcs_;
}

//-----------------------------------------------------------------------------

void
ExtentStore::DiskManager::ExtentGroupReplicateOp::
IssueAggregatedDiskWrite() {

  TAGGED_DCHECK(fe_->IsWorkerThread());
  TAGGED_DCHECK_EQ(outstanding_work_units_, 0);
  TAGGED_CHECK_EQ(outstanding_rpcs_, 0);
  TAGGED_DCHECK_EQ(slice_vec_index_, slice_vec_.size());

  // Go through all the buffered chunks and build up a single buffer.
  int total_num_slices = 0;
  IOBuffer::Ptr aggregate_iobuffer = make_shared<IOBuffer>();
  for (auto& it : work_unit_replica_read_map_) {
    const SliceRange& slice_range = it.first;
    IOBuffer::PtrConst& remote_payload = it.second;
    const int32 start_slice = slice_range.first;
    const int32 num_slices = slice_range.second;
    // For egroup replication, we must read all the slices, therefore the
    // slice ranges must be consecutive.
    TAGGED_CHECK_EQ(start_slice, total_num_slices);
    total_num_slices += num_slices;
    aggregate_iobuffer->AppendIOBuffer(move(remote_payload));
  }
  work_unit_replica_read_map_.clear();
  SliceRange aggregate_slice_range = make_pair(0, total_num_slices);
  TAGGED_DCHECK_EQ(total_num_slices, slice_vec_.size());
  ++outstanding_work_units_;
  TAGGED_VLOG(1) << "Issue cloud disk write, slices: " << total_num_slices
                 << " size: " << aggregate_iobuffer->size();
  WorkUnitIssueDiskWriteContinue(
    aggregate_slice_range, move(aggregate_iobuffer),
    false /* should_aggregate_work_units */);
}

//-----------------------------------------------------------------------------

void
ExtentStore::DiskManager::ExtentGroupReplicateOp::WorkUnitReplicaReadDone(
  const StargateError::Type err,
  const shared_ptr<string>& err_detail,
  const shared_ptr<ReplicaReadEgroupRet>& remote_ret,
  IOBuffer::Ptr&& remote_payload,
  const SliceRange& slice_range) {

  TAGGED_DCHECK(fe_->IsWorkerThread());

  --outstanding_rpcs_;
  TAGGED_CHECK_GE(outstanding_rpcs_, 0);

  // Record any activity traces returned in the RPC response into this op's
  // activity tracer.
  RecordActivityTraces(remote_ret->activity_traces());

  TRACE(at_, 0) << "Completed rpc. Outstanding rpcs=" << outstanding_rpcs_;

  if (err != StargateError::kNoError) {
    TAGGED_LOG(WARNING) << "Attempt to fetch data for egroup "
                        << extent_group_id_ << " from remote disk "
                        << rpc_arg_->remote_disk_id() << " failed with error "
                        << StargateError::ToString(err);
    error_ = err;

    --outstanding_work_units_;
    TAGGED_CHECK_GE(outstanding_work_units_, 0);

    TRACE(at_, 0) << "Completed work unit. Outstanding work_units="
                  << outstanding_work_units_;

    if (outstanding_work_units_ == 0) {
     TAGGED_VLOG(0) << "Replication failed for extent group "
                    << extent_group_id_ << " with error "
                    << StargateError::ToString(error_);

      MaybeFinish();
    }
    return;
  }

  // We have some local work to do. Mark us active again if not already so.
  if (inactive_ && FLAGS_estore_replication_flip_active_status) {
    FlipActiveStatus();
  }

  // We successfully fetched the data. Now write it to the disk.
  WorkUnitIssueDiskWrite(slice_range, move(remote_payload));
}

//-----------------------------------------------------------------------------

void
ExtentStore::DiskManager::ExtentGroupReplicateOp::WorkUnitIssueDiskWrite(
  const SliceRange& slice_range,
  IOBuffer::Ptr&& remote_payload) {

  TAGGED_DCHECK(fe_->IsWorkerThread());
  if (verify_checksums_) {
    VerifySliceChecksums(slice_range, move(remote_payload));
    return;
  }

  WorkUnitIssueDiskWriteContinue(slice_range, move(remote_payload),
                                 ShouldAggregateWorkUnits());
}

//-----------------------------------------------------------------------------

void
ExtentStore::DiskManager::ExtentGroupReplicateOp::VerifySliceChecksums(
  const SliceRange& slice_range,
  IOBuffer::Ptr&& remote_payload) {

  shared_ptr<int> outstanding_compute_reqs = make_shared<int>(0);
  shared_ptr<bool> cksum_error = make_shared<bool>(false);
  const int64 remote_payload_size = remote_payload->size();
  // 'slice_data_offset' is the offset within 'remote_payload' of the
  // current slice, it is computed accumulatively.
  int64 slice_data_offset = 0;

  // Vector to hold work that should be queued as a batch.
  ArenaVector<Function<void()>> offload_vec(arena_);
  offload_vec.reserve(slice_range.second);

  for (int xx  = slice_range.first;
       xx < slice_range.first + slice_range.second; ++xx) {
    const ExtentGroupState::SliceState::UPtrConst& slice_state =
      slice_vec_[xx];
    TAGGED_CHECK(slice_state->has_extent_group_offset());
    const int64 slice_length = slice_state->transformed_length();
    TAGGED_CHECK_GE(remote_payload_size, slice_data_offset + slice_length);

    // Extract the buffer corresponding with the current slice.
    IOBuffer::Ptr iobuf =
      remote_payload->Clone(slice_data_offset, slice_length);
    slice_data_offset += slice_length;
    ++(*outstanding_compute_reqs);
    // We cannot use move semantics on remote_payload here.
    Function<void()> func =
      Bind(&ExtentGroupReplicateOp::VerifyOneSliceChecksums,
           this, bind_move(iobuf), slice_state.get(), outstanding_compute_reqs,
           cksum_error, slice_range, remote_payload);
    offload_vec.emplace_back(move(func));
  }

  globals_->compute_pool->QueueRequest(&offload_vec, qos_priority_);

  TAGGED_CHECK_EQ(slice_data_offset, remote_payload_size);
}

//-----------------------------------------------------------------------------

void
ExtentStore::DiskManager::ExtentGroupReplicateOp::VerifyOneSliceChecksums(
  IOBuffer::Ptr&& iobuf,
  const ExtentGroupState::SliceState* const slice_state,
  shared_ptr<int> outstanding_compute_reqs,
  shared_ptr<bool> cksum_error,
  const SliceRange& slice_range,
  IOBuffer::Ptr remote_payload) {

  const int32 num_subregions = slice_state->checksum_size();
  // When using extent based metadata format, we store only the untransformed
  // checksums for encrypted but uncompressed slices. Decrypt the buffer if the
  // slice is encrypted but uncompressed for checksum verification.
  const bool is_uncompressed =
    slice_state->transformed_length() == slice_state->untransformed_length();
  const bool is_encrypted = !encryption_type_vec_.empty();
  if (extent_based_metadata_format_ && is_uncompressed && is_encrypted) {
    vector<DataTransformation::Type> compression_type_vec;
    iobuf = UntransformSlice(*slice_state,
                             iobuf.get(),
                             compression_type_vec,
                             encryption_type_vec_,
                             cipher_key_,
                             extent_group_id_,
                             num_subregions,
                             0 /* read_start_offset */,
                             true /* has_prepended_compressed_size */);
  }

  if (StargateUtil::ComputeAndVerifySliceChecksum(
        "ExtentGroupReplicateOp",
        disk_manager_->disk_id_,
        extent_group_id_,
        iobuf.get(),
        slice_state,
        slice_state->checksum_size(),
        0 /* subregion_start_offset */,
        false /* zeroed_out_slice */,
        true /* log_critical */,
        checksum_type_)) {
    *cksum_error = true;
    int64 applied_intent_sequence = -1;
    if (extent_based_metadata_format_) {
      TAGGED_DCHECK_GE(extent_based_egroup_applied_is_, 0);
      applied_intent_sequence = extent_based_egroup_applied_is_;
    } else {
      ExtentGroupState::Ptr eg_state =
        disk_manager_->TryFetchExtentGroupState(extent_group_id_);
      TAGGED_CHECK(eg_state) << extent_group_id_;
      applied_intent_sequence = eg_state->latest_applied_intent_sequence();
    }
    const string tag =
      StringJoin("checksum_mismatch_sl",
        slice_state->slice_id(), "_eg",
        extent_group_id_, "_disk", disk_manager_->disk_id_,
        "_ts_", WallTime::NowSecs(), "_is", applied_intent_sequence, "_of",
        slice_state->extent_group_offset(), "_replicate_op.bin");
    StargateUtil::DumpIOBufferWithTag(tag, iobuf);
  }

  Function<void()> func =
    Bind(&ExtentGroupReplicateOp::VerifySliceChecksumsDone, this,
         slice_range, bind_move(remote_payload),
         outstanding_compute_reqs, cksum_error);
  FunctionExecutor *const saved_fe = fe_;
  DeactivateFuncDisablerOutermost();
  saved_fe->Add(move(func));
}

//-----------------------------------------------------------------------------

void
ExtentStore::DiskManager::ExtentGroupReplicateOp::
VerifySliceChecksumsDone(
  const SliceRange& slice_range,
  IOBuffer::Ptr&& remote_payload,
  shared_ptr<int> outstanding_compute_reqs,
  shared_ptr<bool> cksum_error) {

  TAGGED_DCHECK(fe_->IsWorkerThread());

  --(*outstanding_compute_reqs);
  TAGGED_CHECK_GE(*outstanding_compute_reqs, 0);

  if (*outstanding_compute_reqs > 0) {
    return;
  }

  TAGGED_CHECK(remote_ifc_ || rpc_arg_->remote_disk_id() < 0)
    << OUTVARS(rpc_arg_->remote_disk_id());
  if (*cksum_error) {
    TAGGED_LOG(DFATAL) << "Replicate payload checksums failed"
                       << ", peer: "
                       << (remote_ifc_ ? remote_ifc_->peer_handle() : "");
    WorkUnitDiskWriteDone(StargateError::kDataCorrupt);
    return;
  }

  TAGGED_VLOG(2) << "Replicate payload checksums passed"
                 << ", peer: "
                 << (remote_ifc_ ? remote_ifc_->peer_handle() : "");

  WorkUnitIssueDiskWriteContinue(slice_range, move(remote_payload),
                                 ShouldAggregateWorkUnits());
}

//-----------------------------------------------------------------------------

void
ExtentStore::DiskManager::ExtentGroupReplicateOp::
WorkUnitIssueDiskWriteContinue(
  const SliceRange& slice_range,
  IOBuffer::Ptr&& remote_payload,
  const bool should_aggregate_work_units) {

  TAGGED_DCHECK(fe_->IsWorkerThread());
  if (should_aggregate_work_units) {
    // Save the result until all work units are read in an ordered map.
    work_unit_replica_read_map_.emplace(slice_range, move(remote_payload));
    // Imagine work_unit_replica_read_map_ as a "in-memory" disk, then we have
    // finished writing this slice_range so invoke WorkUnitDiskWriteDone with
    // StargateError::kNoError.
    WorkUnitDiskWriteDone(StargateError::kNoError);
    return;
  }

  if (FLAGS_estore_aio_enabled) {
    if (work_unit_disk_write_outstanding_) {
      work_unit_disk_write_queue_.push(make_pair(
        slice_range, move(remote_payload)));
    } else {
      AIOWorkUnitDiskWriteData(slice_range, move(remote_payload));
    }
  } else {
    disk_manager_->ExtentGroupDiskThread(extent_group_id_)->Add(
      Bind(&ExtentGroupReplicateOp::WorkUnitDiskWriteData,
           this, slice_range, bind_move(remote_payload)));
  }
}

//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReplicateOp::WorkUnitDiskWriteData(
  const SliceRange& slice_range,
  IOBuffer::PtrConst&& remote_payload) {

  TAGGED_DCHECK(
    disk_manager_->ExtentGroupDiskThread(extent_group_id_)->IsWorkerThread());
  shared_ptr<vector<pair<int64, IOBuffer::PtrConst>>>
    offset_iobuf_vec =
      make_shared<vector<pair<int64, IOBuffer::PtrConst>>>();
  offset_iobuf_vec->reserve(slice_range.second);

  // Iterate through all the slices, writing the data.
  for (int xx  = slice_range.first;
       xx < slice_range.first + slice_range.second; ++xx) {
    const ExtentGroupState::SliceState::UPtrConst& slice_state =
      slice_vec_[xx];

    TAGGED_CHECK_GE(remote_payload->size(), slice_state->transformed_length());

    // Extract the buffer to be written.
    IOBuffer::Ptr iobuf =
      remote_payload->Clone(0, slice_state->transformed_length());

    // Remove the above prefix from remote_payload.
    remote_payload = remote_payload->Clone(slice_state->transformed_length());

    if (slice_state->cushion()) {
      // If we are writing compressed data, the slice data may not be in
      // multiple of disk block size. Before issuing disk write, we should
      // write zero data to the cushion space to make it multiple of block
      // size. Since cushion space is not expected to be large and while
      // replicating, we should be writing all the slices, we shall write zero
      // to entire cushion space so that kernel can merge with the
      // sebusequent writes.
      IOBuffer::Ptr zero_buf =
        IOBufferUtil::CreateZerosIOBuffer(slice_state->cushion());
      iobuf->AppendIOBuffer(zero_buf.get());
      TAGGED_DCHECK_EQ(
        iobuf->size(),
        slice_state->transformed_length() + slice_state->cushion());
      TAGGED_VLOG(2) << "Padding " << slice_state->cushion()
                     << " bytes of zero data at offset "
                     << slice_state->extent_group_offset() +
                        slice_state->transformed_length();
    }

    TAGGED_DCHECK(slice_state->has_extent_group_offset());
    offset_iobuf_vec->emplace_back(slice_state->extent_group_offset(), iobuf);
  }

  Descriptor::WriteVecCallback done_cb =
    Bind(&ExtentGroupReplicateOp::DiskWriteVecDataDone,
         this, _1, _2, _3, offset_iobuf_vec);
  disk_manager_->ExtentGroupDiskThread(extent_group_id_)->Wrap(&done_cb);
  TAGGED_CHECK(desc_);
  desc_->WriteVec(0 /* source_id */, move(offset_iobuf_vec), move(done_cb));
}

void ExtentStore::DiskManager::ExtentGroupReplicateOp::DiskWriteVecDataDone(
  vector<int64>&& bytes_written_vec,
  int write_errno,
  const int64 io_time,
  const shared_ptr<vector<pair<int64, IOBuffer::PtrConst>>>&
    offset_iobuf_vec ) {

  TAGGED_DCHECK(!FLAGS_estore_aio_enabled);
  TAGGED_DCHECK_EQ(bytes_written_vec.size(), offset_iobuf_vec->size());

  for (uint ii = 0; ii < offset_iobuf_vec->size(); ++ii) {
    const int64 bytes_written = bytes_written_vec[ii];
    const int64 iobuf_len = (*offset_iobuf_vec)[ii].second->size();
    if (bytes_written != iobuf_len) {
      if (write_errno > 0) {
        TAGGED_LOG(ERROR) << "Error writing " << iobuf_len << " bytes at "
                          << "offset " << (*offset_iobuf_vec)[ii].first
                          << " of extent group file "
                          << "for " << extent_group_id_
                          << ExtentStore::GetErrorDetails(write_errno);
      } else {
        TAGGED_LOG(ERROR) << "Attempted to write " << iobuf_len
                          << " bytes, but only managed to write "
                          << bytes_written
                          << " bytes to extent group file for "
                          << extent_group_id_;
        total_bytes_written_ += bytes_written;
      }
      Function<void()> func =
        Bind(&ExtentGroupReplicateOp::WorkUnitDiskWriteDone,
             this, StargateError::kDataCorrupt);
      FunctionExecutor *const saved_fe = fe_;
      DeactivateFuncDisablerOutermost();
      saved_fe->Add(move(func));
      return;
    }

    total_bytes_written_ += bytes_written;
  }

  TAGGED_LOG_IF(INFO,
                FLAGS_estore_disk_access_threshold_msecs > 0 &&
                io_time > FLAGS_estore_disk_access_threshold_msecs * 1000)
    << "Disk " << disk_manager_->disk_id_ << ": write took "
    << io_time/1000 << " msecs";
  total_write_time_usecs_ += io_time;

  // We wrote all the data successfully. We don't call Flush yet as
  // other work units still might need to do their writes.
  Function<void()> func =
    Bind(&ExtentGroupReplicateOp::WorkUnitDiskWriteDone, this,
         StargateError::kNoError);
  FunctionExecutor *const saved_fe = fe_;
  DeactivateFuncDisablerOutermost();
  saved_fe->Add(move(func));
}

//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReplicateOp::
WorkUnitDiskWriteDone(StargateError::Type err) {
  TAGGED_DCHECK(fe_->IsWorkerThread());

  if (err != StargateError::kNoError) {
    error_ = err;
  }

  --outstanding_work_units_;
  TAGGED_CHECK_GE(outstanding_work_units_, 0);

  TRACE(at_, 0) << "Completed work unit. Outstanding work_units="
                << outstanding_work_units_;

  if (outstanding_rpcs_ == outstanding_work_units_ && !inactive_ &&
      FLAGS_estore_replication_flip_active_status) {
    // We're doing no local work - we're only waiting on remote RPCs. Mark
    // this op inactive.
    FlipActiveStatus();
  }

  // Start another work unit if needed.
  MaybeStartWorkUnit();
}

//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReplicateOp::
AIOWorkUnitDiskWriteData(const SliceRange& slice_range,
                         IOBuffer::Ptr&& remote_payload) {

  TAGGED_DCHECK(fe_->IsWorkerThread());
  TAGGED_DCHECK(desc_);
  TAGGED_CHECK(!work_unit_disk_write_outstanding_);
  work_unit_disk_write_outstanding_ = true;

  // Iterate through all the slices and asynchronously write the data using a
  // vector write.
  offset_iobuf_vec_ = make_shared<vector<pair<int64, IOBuffer::PtrConst>>>();
  offset_iobuf_vec_->reserve(slice_range.second);

  int64 last_block_num = 0;
  int64 start_block_num = 0;
  IOBuffer::Ptr iobuf = make_shared<IOBuffer>();
  for (int xx = slice_range.first;
       xx < slice_range.first + slice_range.second; ++xx) {
    const ExtentGroupState::SliceState::UPtrConst& slice_state =
      slice_vec_[xx];

    TAGGED_CHECK_GE(remote_payload->size(), slice_state->transformed_length());
    TAGGED_DCHECK(slice_state->has_extent_group_offset());

    if (extent_based_metadata_format_) {
      const int block_size = block_store_component_->block_size_bytes();
      TAGGED_DCHECK_GE(block_size, AIOExecutor::kBlockSize);
      TAGGED_DCHECK_EQ(mvalue_vec_.size(), 1);
      const PhysicalSliceFB *const physical_slice =
        mvalue_vec_[0]->extent_group_physical_state_entry->
          GetPhysicalSlice(slice_state->slice_id());
      TAGGED_DCHECK(physical_slice);
      const int blocks_per_slice =
        (slice_state->transformed_length() + block_size - 1) / block_size;
      TAGGED_DCHECK_GT(blocks_per_slice, 0);
      const int last_block_data_len =
        slice_state->transformed_length() -
          (blocks_per_slice - 1) * block_size;
      TAGGED_DCHECK_LE(last_block_data_len, block_size);
      TAGGED_DCHECK_GT(last_block_data_len, 0);
      const uint32 allocated_block_bitmap =
        slice_allocated_blocks_map_[slice_state->slice_id()];
      for (int ii = 0; ii < blocks_per_slice; ++ii) {
        if (((1 << ii) & allocated_block_bitmap) == 0) {
          DCHECK_GE(remote_payload->size(), block_size);
          // The read replica op would fill zeros for the blocks which are
          // not allocated. Erase those regions from the buffer.
          remote_payload->Erase(0, block_size);
          if (remote_payload->size() == 0) {
            // We have reached end of buffer, copy whatever ranges we have
            // gathered into write vector.
            TAGGED_DCHECK_EQ(ii, blocks_per_slice - 1);
            TAGGED_DCHECK_EQ(xx, slice_range.first + slice_range.second - 1);
            offset_iobuf_vec_->emplace_back(
              shared_state_->BlockNum2Offset(start_block_num), move(iobuf));
            break;
          }

          continue;
        }

        const int64 block_num =
          disk_manager_->shared_extent_write_state_->GetBlockNum(
            *physical_slice, ii);
        TAGGED_DCHECK_GT(block_num, 0) << slice_state->ShortDebugString();
        start_block_num = start_block_num > 0 ? start_block_num : block_num;

        if (last_block_num > 0 && last_block_num + 1 != block_num) {
          // We don't have contiguous block range therefore we need to move the
          // offset and data gathered so far into the write vector.
          TAGGED_DCHECK_EQ((last_block_num - start_block_num + 1) * block_size,
                           iobuf->size())
            << OUTVARS(start_block_num, last_block_num, block_size);
          offset_iobuf_vec_->emplace_back(
            shared_state_->BlockNum2Offset(start_block_num), move(iobuf));
          // Reset the iobuf.
          iobuf = make_shared<IOBuffer>();
          start_block_num = block_num;
        }

        // Prepare the iobuf for this block. In case the blocks are contiguous
        // we will keep appending to the same iobuf.
        const int64 data_size =
          (ii == blocks_per_slice - 1 ? last_block_data_len : block_size);
        remote_payload->Split(data_size, iobuf.get());
        if (data_size < block_size) {
          iobuf->AppendIOBuffer(
            IOBufferUtil::CreateZerosIOBuffer(block_size - data_size));
        }

        if (remote_payload->size() == 0) {
          // We have reached end of buffer, copy whatever ranges we have
          // gathered into write vector.
          TAGGED_DCHECK_EQ(ii, blocks_per_slice - 1);
          TAGGED_DCHECK_EQ(xx, slice_range.first + slice_range.second - 1);
          offset_iobuf_vec_->emplace_back(
            shared_state_->BlockNum2Offset(start_block_num), move(iobuf));
          break;
        }

        last_block_num = block_num;
      }
    } else {
      // Extract the buffer to be written.
      remote_payload->Split(slice_state->transformed_length(), iobuf.get());
      // If needed, pad the data with enough zero bytes from the slice's
      // cushion so the write is aligned.
      const int num_unaligned_bytes = iobuf->size() % AIOExecutor::kBlockSize;
      if (num_unaligned_bytes != 0) {
        const int zero_buf_len = AIOExecutor::kBlockSize - num_unaligned_bytes;
        TAGGED_CHECK(slice_state->has_cushion());
        TAGGED_CHECK_LE(zero_buf_len, slice_state->cushion());
        CharArray::Ptr zero_buf = make_shared<CharArray>(zero_buf_len);
        memset(zero_buf->data(), 0, zero_buf->size());
        iobuf->Append(&zero_buf);
      }
      offset_iobuf_vec_->emplace_back(slice_state->extent_group_offset(),
                                      move(iobuf));
      iobuf = make_shared<IOBuffer>();
    }
  }
  TAGGED_DCHECK_EQ(remote_payload->size(), 0);

  Descriptor::WriteVecCallback done_cb =
    Bind(&ExtentStore::DiskManager::ExtentGroupReplicateOp::
         AIOWorkUnitDiskWriteDone, this, _1, _2, _3);
  fe_->Wrap(&done_cb);
  // Asynchronously write the data for the op using a vector write.
  auto vec = offset_iobuf_vec_;
  const uint64 source_id =
    disk_manager_->io_categorizer_disk_id_ |
      migration_egroup_io_categorizer_id_;
  desc_->WriteVec(source_id, move(vec), move(done_cb));
}

//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReplicateOp::
AIOWorkUnitDiskWriteDone(
  vector<int64>&& bytes_written_vec,
  int write_errno,
  const int64 io_time) {

  TAGGED_DCHECK(fe_->IsWorkerThread());

  if (MaybeInjectError(ErrorInjections::kIOError, __LINE__)) {
    write_errno = EIO;
  }

  bool error = false;
  if (write_errno != 0) {
    TAGGED_LOG(ERROR)
      << "Error writing data to the extent group file corresponding "
      << "to " << extent_group_id_
      << ExtentStore::GetErrorDetails(write_errno);
    error = true;
  }

  for (uint xx = 0; xx < offset_iobuf_vec_->size(); ++xx) {
    const int64 offset = (*offset_iobuf_vec_)[xx].first;
    const int64 count = (*offset_iobuf_vec_)[xx].second->size();
    const int64 bytes_written = bytes_written_vec[xx];

    if (bytes_written == count) {
      total_bytes_written_ += bytes_written;
    } else {
      if (bytes_written < 0) {
        TAGGED_LOG(ERROR) << "Error writing " << count << " bytes at "
                          << "offset " << offset
                          << " of extent group file "
                          << "for " << extent_group_id_;
      } else {
        TAGGED_LOG(ERROR) << "Attempted to write " << count
                          << " bytes, but only managed to write "
                          << bytes_written
                          << " bytes to extent group file for "
                          << extent_group_id_;
        total_bytes_written_ += bytes_written;
      }
      error = true;
    }
  }

  TAGGED_LOG_IF(INFO,
                FLAGS_estore_disk_access_threshold_msecs > 0 &&
                io_time > FLAGS_estore_disk_access_threshold_msecs * 1000)
    << "AIO Disk " << disk_manager_->disk_id_ << ": write took "
    << io_time/1000 << " msecs";
  total_write_time_usecs_ += io_time;

  TAGGED_CHECK(work_unit_disk_write_outstanding_);
  work_unit_disk_write_outstanding_ = false;

  if (!error) {
    // Check if we have more pending work unit disk writes. If we do, start the
    // next work unit disk write in the queue.
    if (!work_unit_disk_write_queue_.empty()) {
      pair<SliceRange, IOBuffer::Ptr> slice_data =
        work_unit_disk_write_queue_.front();
      work_unit_disk_write_queue_.pop();
      AIOWorkUnitDiskWriteData(slice_data.first, move(slice_data.second));
    }
    WorkUnitDiskWriteDone(StargateError::kNoError);
  } else {
    // Clear any pending work units in work_unit_disk_write_queue_ as we will
    // not be starting them now, since we have encountered a write error.
    if (!work_unit_disk_write_queue_.empty()) {
      TAGGED_LOG(INFO) << "Cancelling pending writes "
                       << OUTVARS(outstanding_work_units_,
                                  work_unit_disk_write_queue_.size());

      TAGGED_DCHECK_GE(static_cast<size_t>(outstanding_work_units_),
                       work_unit_disk_write_queue_.size());
      outstanding_work_units_ -= work_unit_disk_write_queue_.size();
      work_unit_disk_write_queue_ = {};
    }
    WorkUnitDiskWriteDone(StargateError::kDataCorrupt);
  }
}

//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReplicateOp::IssueFlush() {
  TAGGED_DCHECK(fe_->IsWorkerThread());

  TAGGED_CHECK_EQ(error_, StargateError::kNoError);

  if (FLAGS_estore_aio_enabled || !FLAGS_estore_fdatasync_enabled) {
    FinalizeTentativeUpdate();
    return;
  }

  TRACE(at_, 0) << "Issuing Flush";

  disk_manager_->ExtentGroupDiskThread(extent_group_id_)->Add(
    BoundFunction<void()>::Create<
      ExtentGroupReplicateOp, &ExtentGroupReplicateOp::DiskFlush>(this));
}

//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReplicateOp::DiskFlush() {
  TAGGED_DCHECK(
    !FLAGS_estore_aio_enabled &&
    disk_manager_->ExtentGroupDiskThread(extent_group_id_)->IsWorkerThread());

  TAGGED_DCHECK(desc_);
  const int64 start_time_usecs = WallTime::NowUsecs();
  TAGGED_DCHECK(FLAGS_estore_fdatasync_enabled);

  Descriptor::FlushCallback done_cb =
    Bind(&ExtentGroupReplicateOp::FlushDone, this, _1, start_time_usecs);
  fe_->Wrap(&done_cb);
  desc_->Flush(done_cb);
}

void ExtentStore::DiskManager::ExtentGroupReplicateOp::FlushDone(
  bool success, int64 start_time_usecs) {

  TAGGED_DCHECK(fe_->IsWorkerThread());

  const int64 diff_time_usecs = WallTime::NowUsecs() - start_time_usecs;
  total_write_time_usecs_ += diff_time_usecs;

  if (!success) {
    DiskFlushDone(StargateError::kDataCorrupt);
    return;
  }

  TAGGED_LOG_IF(INFO,
                FLAGS_estore_disk_access_threshold_msecs > 0 &&
                diff_time_usecs >
                FLAGS_estore_disk_access_threshold_msecs * 1000)
    << "Disk " << disk_manager_->disk_id_ << ": Flush took "
    << diff_time_usecs/1000 << " msecs";

  DiskFlushDone(StargateError::kNoError);
}

void
ExtentStore::DiskManager::ExtentGroupReplicateOp::DiskFlushDone(
  StargateError::Type err) {
  TAGGED_DCHECK(fe_->IsWorkerThread());

  TRACE(at_, 0) << "Flush completed with error "
                << StargateError::ToString(err);

  if (err != StargateError::kNoError) {

    ReportDiskOpCompleted(false /* is_read */,
                          true /* error */,
                          total_bytes_written_,
                          total_write_time_usecs_);

    TAGGED_VLOG(0) << "Replication failed for extent group "
                   << extent_group_id_ << " with error "
                   << StargateError::ToString(err);

    MaybeFinish(err);
    return;
  }

  FinalizeTentativeUpdate();
}

//-----------------------------------------------------------------------------

void
ExtentStore::DiskManager::ExtentGroupReplicateOp::FinalizeTentativeUpdate() {
  TAGGED_DCHECK(fe_->IsWorkerThread());

  if (FLAGS_estore_experimental_replication_delay_finalize ==
        extent_group_id_) {
    TRACE(at_, 0) << "Delaying finalization";
    TAGGED_LOG(INFO) << "Delaying finalization of replication of extent group "
                     << extent_group_id_ << " from remote disk "
                     << rpc_arg_->remote_disk_id()
                     << " to local disk " << disk_manager_->disk_id_
                     << ", expected_intent_sequence "
                     << expected_intent_sequence_
                     << ", latest_intent_sequence " << intent_sequence_;
    globals_->delay_ed->Add(BindInFe<void()>(
      fe_, &ExtentGroupReplicateOp::FinalizeTentativeUpdate, this), 1000);
    return;
  }

  if (FLAGS_estore_experimental_cancel_before_finalize_tu) {
    TAGGED_LOG(INFO) << "Abort finalization of replication of extent group "
                     << extent_group_id_ << " from remote disk "
                     << rpc_arg_->remote_disk_id()
                     << " to local disk " << disk_manager_->disk_id_
                     << ", expected_intent_sequence "
                     << expected_intent_sequence_
                     << ", latest_intent_sequence " << intent_sequence_;
    MaybeFinish(StargateError::kRetry);
    return;
  }

  MAYBE_INJECT_CRASH_ALL_BUILDS(
    FLAGS_estore_experimental_crash_before_finalize_tu);

  if (blockstore_finalize_outstanding_) {
    waiting_to_finalize_ = true;
    return;
  }

  TAGGED_CHECK_EQ(error_, StargateError::kNoError);

  // Update the disk usage stats. disk_usage_diff_ does not account for
  // any fallocated space beyond the end of the egroup file. We will handle
  // that here and update the garbage appropriately.

  TAGGED_DCHECK_GT(disk_usage_diff_.container_unshared_usage_size(), 0);
  TAGGED_DCHECK_GT(disk_usage_diff_.container_deduped_usage_size(), 0);
  TAGGED_DCHECK_GT(disk_usage_diff_.container_ec_parity_size(), 0);
  TAGGED_DCHECK_GT(disk_usage_diff_.container_garbage_size(), 0);

  // We might have written over some previously fallocated region on the egroup
  // file. Adjust the garbage accordingly.
  const int32 new_garbage =
    fallocated_egroup_size_ - disk_usage_diff_.total_usage().transformed();
  if (new_garbage > 0) {
    TAGGED_DCHECK(!extent_based_metadata_format_);
    disk_usage_diff_.mutable_total_usage()->set_transformed(
      disk_usage_diff_.total_usage().transformed() + new_garbage);
    disk_usage_diff_.mutable_garbage()->set_transformed(
      disk_usage_diff_.garbage().transformed() + new_garbage);
    disk_usage_diff_.mutable_container_garbage(0)->set_transformed(
      disk_usage_diff_.container_garbage(0).transformed() + new_garbage);
  }

  if (!extent_based_metadata_format_) {
    disk_manager_->UpdateDiskUsage(extent_group_id_,
                                   disk_usage_diff_,
                                   true /* add */);
  }

  ReportDiskOpCompleted(false /* is_read */,
                        false /* error */,
                        total_bytes_written_,
                        total_write_time_usecs_);

  const MigrationIOStatsEntry migration_io_stats(
    1 /* add_operations */,
    total_bytes_written_,
    0 /* delete_operations */,
    0 /* deleted_bytes */);
  const MigrationIOStatsReason reason = rpc_arg_->has_migration_reason() ?
    MigrationCounters::MigrationReasonProtoToEnum(
      rpc_arg_->migration_reason()) :
    MigrationIOStatsReason::kUnclassified;
  disk_manager_->migration_counters_->AddStats(migration_io_stats, reason);

  if (extent_based_metadata_format_) {
    if (shared_state_->data_on_ext4_files()) {
      WriteExtentBasedMetadata();
      return;
    }

    // Create the egroup proxy file. We could have done this right after
    // writing the TU entry but chose to wait until we get to a point where the
    // operation will not abort. This way, we can avoid deleting the egroup
    // proxy file in the abort path.
    TRACE(at_, 0) << "Creating the egroup proxy file";
    string path = disk_manager_->GetEgroupFileSystemPath(extent_group_id_);
    shared_state_->file_system_->CreateWeakLink(
      disk_manager_->kExtentStoreEgroupBaseName, move(path),
      BindInFe<void(Error)>(fe_, &ExtentGroupReplicateOp::ProxyFileLinkDone,
                            this, _1));
    return;
  }

  TRACE(at_, 0) << "Finalizing tentative update";
  ScopedArenaMark am;
  DiskWALRecord::Ptr record = DiskWALRecord::Create(am.arena());
  auto egroup_update = record->add_egroup_update();
  egroup_update->set_extent_group_id(extent_group_id_);

  TAGGED_CHECK_GE(intent_sequence_, 0);
  egroup_update->set_finalize_tentative_intent_sequence(intent_sequence_);
  egroup_update->set_largest_seen_intent_sequence(intent_sequence_);

  // When we replicate an egroup, the egroup is scrubbed consequently. The
  // scrub time should be updated.
  egroup_update->set_last_scrub_time_secs(WallTime::NowSecs());

  if (!managed_by_aes_) {
    // Indicate that the applied intent sequence is already sync'd with
    // Medusa (since the caller who invoked this op must have looked at Medusa
    // to come up with the expected intent sequence).
    egroup_update->set_synced_intent_sequence(
      rpc_arg_->expected_intent_sequence());
  } else {
    egroup_update->set_global_metadata_intent_sequence(
      rpc_arg_->intent_sequence());
    egroup_update->set_managed_by_aes(true);
  }

  // If we fallocated some blocks for the newly created egroup file, we need to
  // log that in the WAL. This effectively indicates that the egroup file is
  // now guaranteed to have at least these many bytes preallocated on disk.
  if (fallocated_egroup_size_) {
    egroup_update->set_fallocate_size(fallocated_egroup_size_);
  }

  TAGGED_VLOG(2) << "Issuing finalize: " << egroup_update->ShortDebugString();

  MaybeFinalizeOrAbortInfoEgroupTU(record, true /* finalize */);
  record->Finish();

  finalized_tentative_update_ = true;

  auto flush_cb = BindInFe<void()>(
    fe_, arena_, &ExtentGroupReplicateOp::ReplicationCompleted, this);
  disk_manager_->disk_wal_->LogDeltaRecord(record.get(), move(flush_cb));
}

//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReplicateOp::ProxyFileLinkDone(
  const Error error) {

  TAGGED_DCHECK(fe_->IsWorkerThread());

  if (error != Error::kExists) {
    TRACE(at_, 0) << "Proxy file linked with error "
                  << static_cast<int>(error);
    TAGGED_VLOG(1) << "Proxy file linked with error " << error;
  } else {
    TRACE(at_, 0) << "Proxy file already present";
    TAGGED_VLOG(1) << "Proxy file already present";
  }

  // TODO: When converting from a non-extent-based ext4 egroup to an
  // extent-based egroup, an ext4 egroup data file may be present. We should
  // delete that file.

  if (error != Error::kNoError && error != Error::kExists) {
    if (error == Error::kNoSpace) {
      TAGGED_LOG(ERROR) << "Proxy egroup file creation failed: " << error;
      error_ = StargateError::kDiskSpaceUnavailable;
    } else {
      TAGGED_LOG(FATAL) << "Proxy egroup file creation failed: " << error;
    }

    MaybeFinish();
    return;
  }

  // Continue to write the metadata before finalizing the TU.
  WriteExtentBasedMetadata();
}

//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReplicateOp::
WriteExtentBasedMetadata() {
  TAGGED_DCHECK_EQ(egid_vec_.size(), mvalue_vec_.size());
  TAGGED_DCHECK_EQ(egid_vec_.size(), 1);

  MaybeCrashForInjection(CrashInjections::kBeforeWritingMetadata, __LINE__);

  Function<void(MedusaError::Type, int)> done_cb =
    BoundFunction<void(MedusaError::Type, int)>::Create<
      ExtentGroupReplicateOp,
      &ExtentGroupReplicateOp::BTreeAESDBUpdateDone>(this);
  TRACE(at_, 0) << "Updating B-Tree AESDB";
  disk_manager_->globals_->medusa->UpdateExtentGroupPhysicalState(
    disk_manager_->btree_aesdb_,
    &egid_vec_,
    &mvalue_vec_,
    disk_manager_->CreateAESDBCallback(
      move(done_cb), AESDBManager::AESDBOp::kWriteOp,
      KVStore::KVStoreType::kBTreeType),
    false /* skip_commit_log */,
    false /* cache_updated_values */,
    disk_manager_->perf_stat()->medusa_call_stats());
}

//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReplicateOp::
MaybeFinalizeOrAbortInfoEgroupTU(
  const DiskWALRecord::Ptr& record,
  const bool finalize) {

  // Return if this is not an AES parity egroup.
  ExtentGroupState::Ptr eg_state =
    disk_manager_->TryFetchExtentGroupState(extent_group_id_);
  TAGGED_CHECK(eg_state) << extent_group_id_;
  if (!eg_state->IsErasureCodedParityAESEgroup()) {
    return;
  }

  TAGGED_DCHECK(error_ == StargateError::kNoError || !finalize)
    << "Finalize " << finalize << " error " << error_;
  TAGGED_DCHECK_GT(local_info_egroup_id_vec_.size(), 0)
    << "error " << error_ << " finalize " << finalize;

  for (const int64 info_egroup_id : local_info_egroup_id_vec_) {
    ExtentGroupState::Ptr info_eg_state =
      disk_manager_->TryFetchExtentGroupState(info_egroup_id);
    TAGGED_CHECK(info_eg_state) << info_egroup_id;
    TAGGED_DCHECK(info_eg_state->has_global_metadata_intent_sequence())
      << info_egroup_id << " eg_state " << info_eg_state->ToString();

    // Given that the replicate op grabs an exclusive lock on the parity egroup
    // and also creates fresh egroup ids for storing the info egroup metadata,
    // we are ensured that no other racing op will mutate the largest seen
    // intent sequence of the info egroups until the parity egroup's exclusive
    // lock is released.
    const int64 info_tu_intent_sequence =
      info_eg_state->largest_seen_intent_sequence();
    TAGGED_CHECK_GE(info_tu_intent_sequence, 0)
      << info_egroup_id << " eg_state " << info_eg_state->ToString();

    TAGGED_VLOG(1) << "Rolling " << (finalize ? "forward" : "backward")
                   << " info egroup " << info_egroup_id
                   << " intent_sequence " << info_tu_intent_sequence;

    auto egroup_update = record->add_egroup_update();
    egroup_update->set_extent_group_id(info_egroup_id);
    egroup_update->set_managed_by_aes(true);
    egroup_update->set_global_metadata_intent_sequence(
      info_eg_state->global_metadata_intent_sequence());
    if (!finalize) {
      egroup_update->set_delete_egroup(true);
      if (disk_manager_->block_store_) {
        // When using block store, info egroups for a given AES parity egroup
        // will not have physical segments. As there are no block to be
        // deallocated in block store, we can directly finalize the deletion of
        // info egroups.
        egroup_update->set_finalize_deletion_truncation(true);
      }
      egroup_update->set_abort_tentative_intent_sequence(
        info_tu_intent_sequence);
      continue;
    }
    egroup_update->set_largest_seen_intent_sequence(info_tu_intent_sequence);
    egroup_update->set_finalize_tentative_intent_sequence(
      info_tu_intent_sequence);
  }
}


//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReplicateOp::ReplicationCompleted() {
  TAGGED_DCHECK_EQ(error_, StargateError::kNoError);
  TAGGED_DCHECK(fe_->IsWorkerThread());

  TAGGED_VLOG(0) << "Successfully completed replication of extent group "
                 << extent_group_id_ << " from remote disk "
                 << rpc_arg_->remote_disk_id()
                 << " to local disk " << disk_manager_->disk_id_;

  MAYBE_INJECT_CRASH_ALL_BUILDS(
    FLAGS_estore_experimental_crash_after_finalize_tu);

  if (checkpoint_lock_acquired_) {
    TAGGED_DCHECK(managed_by_aes_);
    TRACE(at_, 0) << "Releasing checkpoint lock";
    disk_manager_->checkpoint_egroup_locker_.Release(extent_group_id_);
    TAGGED_VLOG(3) << "Released checkpoint lock";
    checkpoint_lock_acquired_ = false;
  }
  allocated_block_id_list_.Reset();
  MaybeFinish();
}

//-----------------------------------------------------------------------------

int32 ExtentStore::DiskManager::ExtentGroupReplicateOp::Cost() const {
  TAGGED_DCHECK(rpc_arg_);
  TAGGED_DCHECK(MaybeAddToQosPrimaryQueue());
  // The read iops cost is divided by the difference between SSD iops and HDD
  // iops if the ops is writing on the hdd. Even if the ilm is due to disk
  // balancing and the read may happen on HDD, this is ok as the read cost is
  // much lower on hdd.
  // some sequential nature and many of them will happen on a SSD disk.
  const int32 read_amplification_factor =
    max(FLAGS_estore_ssd_iops / FLAGS_estore_hdd_iops, 1);
  const int32 num_slices =
    max(rpc_arg_->slices_size() / (disk_manager_->is_ssd() ? 1 :
          read_amplification_factor), 1);
  const int32 num_seq_writes =
    max(1, ((num_slices * FLAGS_estore_regular_slice_size) /
             max_work_unit_bytes_));

  // num_slices is the cost of reading 'num_slices' at remote extent store.
  // The op does sequential writes in chunks of
  // FLAGS_estore_replication_word_unit_bytes. The number of writes on the
  // local extent store will be the total data read divided by the max chunk
  // size. Each sequential write is considered to be of cost 1.
  const int32 cost = std::max(num_slices + num_seq_writes,
                              disk_manager_->min_op_cost());
  if (google::DEBUG_MODE && cost > 40 && !disk_manager_->is_ssd()) {
    TAGGED_VLOG(2)
      << "Replicate op on " << disk_manager_->disk_id_ << ": high op cost "
      << cost << " num_slices " << num_slices << " amplification factor "
      << read_amplification_factor << " num_seq_writes " << num_seq_writes;
  }
  return std::min(cost, disk_manager_->max_op_cost());
}

//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReplicateOp::SetTraceAttributes() {
  TRACE_ADD_ATTRIBUTE(at_, "egroup", extent_group_id_);
  TRACE_ADD_ATTRIBUTE(at_, "source", rpc_arg_->remote_disk_id());
  TRACE_ADD_ATTRIBUTE(at_, "target", disk_manager_->disk_id_);
  TRACE_ADD_ATTRIBUTE(at_, "intent_seq",
                      StringJoin(expected_intent_sequence_));
  if (rpc_arg_->has_ilm_migration()) {
    TRACE_ADD_ATTRIBUTE(at_, "ilm_migration", rpc_arg_->ilm_migration());
  }
}

//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReplicateOp::MaybeFinish(
  const Optional<StargateError::Type> err) {

  TAGGED_DCHECK(fe_->IsWorkerThread());

  if (err) {
    error_ = *err;
  }

  if (blockstore_finalize_outstanding_) {
    TAGGED_DCHECK(!extent_based_metadata_format_);
    waiting_to_finish_ = true;
    return;
  }

  if (extent_based_metadata_format_ && written_tentative_update_ &&
      !finalized_tentative_update_) {
    TAGGED_DCHECK_NE(error_, StargateError::kNoError);
    if (egroup_file_may_exist_ && shared_state_->data_on_ext4_files()) {
      // Delete the egroup file before finalizing the tentative update.
      if (desc_) {
        ReleaseEgroupDescriptor();
      }
      egroup_file_may_exist_ = false;
      disk_manager_->ExtentGroupDiskThread(extent_group_id_)->Add(
        [this, err]() {
        TAGGED_DCHECK(
          disk_manager_->ExtentGroupDiskThread(extent_group_id_)->
             IsWorkerThread());
        disk_manager_->RemoveEgroupFile(extent_group_id_);
        fe_->Add(Bind(&ExtentGroupReplicateOp::MaybeFinish, this,
                      err));
      });
      return;
    }

    shared_state_->RemoveTUEntry(
      arena_alloc_.MakeUnique<SharedExtentWriteState::TUEntry>(
        extent_group_id_,
        SharedExtentWriteState::kReplicateTUExtentIndex),
      tu_page_, at(), Bind(&ExtentGroupReplicateOp::RemoveTUDone, this,
                           _1, true /* abort */));
    return;
  }

  if (allocated_block_id_list_ && !extent_based_metadata_format_) {
    TAGGED_DCHECK_NE(error_, StargateError::kNoError);
    TAGGED_DCHECK(!allocated_block_id_list_->Empty());

    // The op aborted after allocating blocks from the block store. Give the
    // blocks back.
    if (blockstore_finalize_issued_) {
      // Block store finalization has already completed. Deallocate the blocks
      // from the block store before finishing.
      DeallocateFromBlockStore();
    } else {
      // Finalization has not been issued to the block store yet. We can't just
      // abort the allocation now because we don't want to make the ids
      // available for re-allocation until the tentative update is finalized in
      // the WAL, otherwise we may end up finalizing the allocation for the ids
      // during crash recovery even when they have been allocated elsewhere.
      // Instead, we will finalize the allocation, deallocate the ids, finalize
      // the tentative update and then finalize the deallocation.
      TRACE(at_, 0) << "Abort: Finalizing block store allocations";

      blockstore_finalize_issued_ = true;
      BlockIdList block_id_list = allocated_block_id_list_->GetCtr();
      Function<void()> finalize_alloc_done_cb =
        FuncDisabled<void()>(
          [this]() {
            TRACE(at_, 0) << "Block store finalization finished";
            TAGGED_DCHECK(fe_->IsWorkerThread());
            TAGGED_DCHECK(blockstore_finalize_issued_);
            MaybeFinish();
          });

      fe_->Wrap(&finalize_alloc_done_cb);

      if (block_id_list.size() == 1 && block_id_list[0].second == 1) {
        block_store_component_->FinalizeAllocation(
          block_id_list[0].first, false /* abort */,
          move(finalize_alloc_done_cb), at());
      } else {
        block_store_component_->FinalizeAllocation(
          move(block_id_list), false /* abort */, move(finalize_alloc_done_cb),
          at());
      }
    }
    return;
  }

  if (managed_by_aes_ && error_ == StargateError::kNoError &&
      !extent_based_metadata_format_) {
    // Delete the unflushed deleted entry as we have reincarnated this extent
    // group. All those keys that were actually deleted will be persisted by
    // the following physical update op.
    disk_manager_->unflushed_deleted_aes_egroups_->erase(extent_group_id_);

    // We'll persist the update in Medusa immediately if we are deleting any
    // sub-keys as part of this replication. This ensures that we don't lose
    // track of these delete markers in case the egroup is deleted before the
    // deletions are flushed from the memtable.
    //
    // Note: For AES parity egroups, it is sufficient to check just for the
    // deleted keys of the parity egroup, as we would be creating fresh
    // info egroup ids on the target of the replication to store the info
    // egroup metadata.
    const bool persist_update = (!deleted_extent_id_set_.empty() ||
                                 !deleted_slice_id_set_.empty());

    // For AES parity egroups, update the physical state for the
    // associated info egroup metadata.
    ExtentGroupState::Ptr eg_state =
      disk_manager_->TryFetchExtentGroupState(extent_group_id_);
    if (eg_state && eg_state->IsErasureCodedParityAESEgroup()) {
      TAGGED_VLOG(3) << "Updating physical state of the parity and "
                     << "num info egroup metadata "
                     << eg_state->ec_info()->local_info_egroup_id_vec.size();
      TAGGED_DCHECK_GT(
        eg_state->ec_info()->local_info_egroup_id_vec.size(), 0);
      vector<int64> egroup_id_vec(
        eg_state->ec_info()->local_info_egroup_id_vec);
      egroup_id_vec.emplace_back(extent_group_id_);
      TAGGED_DCHECK_EQ(
        egroup_id_vec.size(),
        eg_state->ec_info()->local_info_egroup_id_vec.size() + 1);

      // Issue the request to update the physical state of the egroups.
      // Note: specify the 'unlock_egroups' parameter as 'false', to
      // not make this helper function release the egroup locks, as
      // the UpdatePhysicalStateOp eventually invokes
      // MaybeDeleteExtentGroupState() on the egroup(s). This ASSERTs if it
      // finds an eg state whose lock has been released but has active
      // operations. The onus is on the parent op to release any slice
      // locks held and unregister itself from the list of active ops
      // as part of cleanup.
      UpdatePhysicalStateAndFinish(move(egroup_id_vec),
                                   true /* wait_for_completion */,
                                   persist_update,
                                   false /* unlock_egroups */);
      return;
    }

    UpdatePhysicalStateAndFinish(
      extent_group_id_, true /* wait_for_completion */, persist_update);
    return;
  }

  Finish();
}

//-----------------------------------------------------------------------------

void
ExtentStore::DiskManager::ExtentGroupReplicateOp::DeallocateFromBlockStore() {
  TAGGED_DCHECK(fe_->IsWorkerThread());
  TAGGED_DCHECK(allocated_block_id_list_);
  TAGGED_DCHECK(blockstore_finalize_issued_);
  TAGGED_DCHECK(!blockstore_finalize_outstanding_);

  TRACE(at_, 0) << "Aborting - deallocating block store allocations";

  // Remember the first id that is being deallocated - we will use it to
  // finalize the deallocation in the block store after aborting the tentative
  // update.
  block_store_deallocate_finalize_key_ = (*allocated_block_id_list_)[0].first;
  block_store_component_->Deallocate(
    allocated_block_id_list_->ReleaseCtr(),
    BindInFe<void(Error)>(fe_, &ExtentGroupReplicateOp::DeallocateDone, this,
                          _1), false /* replay */, at());
}

void
ExtentStore::DiskManager::ExtentGroupReplicateOp::DeallocateDone(
  const Error error) {

  TAGGED_DCHECK(fe_->IsWorkerThread());

  // The only reason Deallocate() would fail is if we were deallocating an
  // already deallocated id, which should never happen.
  TAGGED_DCHECK(error == Error::kNoError) << error;
  allocated_block_id_list_.Reset();
  MaybeFinish();
}

//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReplicateOp::BTreeAESDBUpdateDone(
  const MedusaError::Type err,
  const int error_source_index) {

  TAGGED_DCHECK(fe_->IsWorkerThread());

  TRACE(at_, 0) << "B-Tree AESDB update completed with status: " << err;
  TAGGED_VLOG(3) << "B-Tree AESDB update completed with status: " << err;

  TAGGED_CHECK_EQ(err, MedusaError::kNoError)
    << "B-Tree AESDB update failed with status: " << err;

  MaybeCrashForInjection(CrashInjections::kBeforeFinalization, __LINE__);
  MaybeCrashForInjection(
    CrashInjections::kBeforeFinalizationCrashOnRecoveryBeforeMetadata,
    __LINE__);
  MaybeCrashForInjection(
    CrashInjections::kBeforeFinalizationCrashOnRecoveryAfterMetadata,
    __LINE__);

  BlockIdList block_id_list = allocated_block_id_list_->ReleaseCtr();
  allocated_block_id_list_.Reset();

  if (shared_state_->data_on_ext4_files()) {
    Function<void(Error)> cb =
      Bind(&ExtentGroupReplicateOp::RemoveTUDone, this, _1, false /* abort */);
    auto tu_entry =
      arena_alloc_.MakeUnique<SharedExtentWriteState::TUEntry>(
        extent_group_id_, SharedExtentWriteState::kReplicateTUExtentIndex);
    shared_state_->RemoveTUEntry(move(tu_entry), tu_page_, at(), move(cb));
    return;
  }

  // Finalize block allocation.
  TRACE(at_, 0) << "Finalizing tentatively allocated blocks";
  TAGGED_VLOG(1) << "Finalizing tentatively allocated blocks";

  Function<void()> finalize_alloc_done_cb =
    FuncDisabled<void()>(
      [this]() {
        TAGGED_DCHECK(fe_->IsWorkerThread());

        TRACE(at_, 0) << "Finalized tentatively allocated blocks";
        TAGGED_VLOG(1) << "Finalized tentatively allocated blocks";

        MaybeCrashForInjection(CrashInjections::kAfterFinalizeAlloc, __LINE__);
        MaybeCrashForInjection(
          CrashInjections::kAfterFinalizeAllocCrashOnRecoveryBeforeMetadata,
          __LINE__);
        MaybeCrashForInjection(
          CrashInjections::kAfterFinalizeAllocCrashOnRecoveryAfterMetadata,
          __LINE__);

        Function<void(Error)> cb =
          Bind(&ExtentGroupReplicateOp::RemoveTUDone, this, _1,
               false /* abort */);
        auto tu_entry =
          arena_alloc_.MakeUnique<SharedExtentWriteState::TUEntry>(
            extent_group_id_, SharedExtentWriteState::kReplicateTUExtentIndex);
        shared_state_->RemoveTUEntry(move(tu_entry), tu_page_, at(), move(cb));
      });
  fe_->Wrap(&finalize_alloc_done_cb);

  block_store_component_->FinalizeAllocation(
    move(block_id_list), false /* abort */, move(finalize_alloc_done_cb),
    at());
}

//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReplicateOp::RemoveTUDone(
  const Error error,
  const bool abort) {

  TAGGED_DCHECK(fe_->IsWorkerThread());

  disk_manager_->UpdateDiskUsage(
    extent_group_id_, disk_usage_diff_, true /* add */,
    KVStore::KVStoreType::kBTreeType);
  TAGGED_DCHECK_EQ(error, Error::kNoError) << OUTVARS(abort);
  finalized_tentative_update_ = true;
  WriteTUDone(nullptr /* tu_page */, StargateError::kNoError, IOBuffer::Ptr(),
              true /* removed */);
}

//-----------------------------------------------------------------------------

void ExtentStore::DiskManager::ExtentGroupReplicateOp::MaybeCrashForInjection(
  const CrashInjections injection,
  const int line) {

  if (FLAGS_estore_replicateop_crash_injection ==
      static_cast<int>(injection)) {
    TAGGED_LOG(INFO) << "Crash injection "
                     << crash_injections_str_[static_cast<int>(injection)]
                     << " at " << __FILE__ << ":" << line;
    MAYBE_INJECT_CRASH_ALL_BUILDS(1 /* inverse_probability */);
  }
}

//-----------------------------------------------------------------------------

bool ExtentStore::DiskManager::ExtentGroupReplicateOp::MaybeInjectError(
  const ErrorInjections injection,
  const int line) {

  if (FLAGS_estore_replicateop_error_injection ==
      static_cast<int>(injection)) {
    TAGGED_LOG(INFO) << "Error injection "
                     << error_injections_str_[static_cast<int>(injection)]
                     << " at " << __FILE__ << ":" << line;
    return true;
  }
  return false;
}

//-----------------------------------------------------------------------------

} } } // namespace
