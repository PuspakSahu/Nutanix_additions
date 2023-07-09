/*
 * Copyright (c) 2013 Nutanix Inc. All rights reserved.
 *
 * Author: partha@nutanix.com
 *
 */

#include <json/json.h>

#include "cdp/client/stargate/stargate_interface/stargate_interface.h"
#include "kv_store/rocksdb_kv_store.h"
#include "medusa/medusa_printer_util.h"
#include "qa/test/stargate/extent_store/egroup_ops_tester.h"
#include "qa/test/stargate/util/space_usage.h"
#include "util/base/shell_util.h"
#include "util/net/ip_util.h"

DECLARE_int32(egroup_test_chunk_size);
DECLARE_int32(egroup_test_sleep_on_finish_secs);
DECLARE_int32(estore_regular_slice_size);
DECLARE_int32(estore_checksum_subregion_size);
DECLARE_int64(estore_replication_work_unit_bytes);
DECLARE_int32(stargate_manager_verbosity);
DECLARE_string(egroup_encryption_type);
DECLARE_bool(egroup_test_estore_aes_enabled);
DECLARE_bool(egroup_test_estore_aes_hybrid_enabled);
DECLARE_bool(use_block_store);
DECLARE_bool(egroup_test_ec_ops);
DECLARE_bool(egroup_test_use_unified_background_scan);
DECLARE_bool(make_the_slices_corrupt_);



DEFINE_bool(egroup_test_disk_usage, false,
            "If enabled, the test verifies if disk usage computation is "
            "accurate for primary and secondary disks.");

DEFINE_bool(egroup_test_update_last_scrub_time, false,
            "If true, the test checks that the last_scrub_time_secs field "
            "for the egroups are updated on the first write and after "
            "migration.");

using namespace nutanix;
using namespace nutanix::kv_store;
using namespace nutanix::medusa;
using namespace nutanix::misc;
using namespace nutanix::net;
using namespace nutanix::stargate;
using namespace nutanix::stargate::interface;
using namespace nutanix::stargate::test;
using namespace nutanix::stats;
using namespace nutanix::test;
using namespace nutanix::zeus;

using namespace std;

//-----------------------------------------------------------------------------

class EgroupOpsTest : EgroupOpsTester {
 public:
  EgroupOpsTest(const string& config_file_name);
  ~EgroupOpsTest();

  // Test TestImmutableEgroupTruncated scenario. If a WriteExtentGroups RPC
  // comes in and specifies immutable egroups && !further_writes, then
  // Stargate should optimistically truncate extent group.
  void TestImmutableEgroupTruncated();

  // Test write extent group at intent sequence specified as the
  // 'post_write_intent_sequence' in the write rpc arg.
  void TestWriteExtentGroupAtPostWriteIntentSequence();

  // Test overwriting an erasure coded info egroup.
  void TestOverwriteErasureCodedEgroup();

  // Test rollback op.
  void TestRollbackEgroup();

  // Test writing a parity AES egroup.
  void TestAESWriteParity(bool inject_error);

  // Test erasure coding with AES egroups.
  void TestAESMigrateErasureCode();

  void TestAESErasureCoding(
    bool inject_error,
    int64 replication_bytes = -1);

  // Test ReplicaReadEgroup RPC.
  void TestReplicaReadEgroup(bool managed_by_aes);

  // Test ReplicateExtentGroup RPC.
  void TestReplicateExtentGroup(bool managed_by_aes);

  // Test DeleteExtentGroup RPC.
  void TestDeleteExtentGroup(bool managed_by_aes);

  // Test DeleteExtentGroup RPC for AES and Non-AES extent group mix.
  void TestDeleteExtentGroupOnAesAndNonAesEgroups();

  // Test deletion and replication of an AES extent group. This test verifies
  // the case where an extent group replica was deleted and then replicated
  // again on the very same disk before its physical state metadata is deleted
  // from Medusa.
  void TestDeleteAndReplicateForAESExtentGroup();

  // Test SyncEgroupSlices RPC.
  void TestSyncEgroupSlices();

  // Test TruncateEgroups RPC.
  void TestTruncateEgroups();

  // Test partial slice overwrites.
  void TestExtentStoreOverwrites(int chunk_size);

  // Test ExtentStore drop (in) WriteExtentGroup op.
  void TestExtentStoreDropOps();

  // Test slice subregion read op.
  void TestSliceSubregionRead();

  // Test slice subregion write op.
  void TestSliceSubregionWrite();

  // Test writes to multiple slices.
  void TestMultiSliceWrite();

  // Test compressed slice write op.
  void TestCompressedSlice();

  // Test compressed slice replacement and verify cushion sizes.
  void TestCompressedSliceReplacement();

  // Test mixed compression extent group where the extent group can have
  // both compressed and uncompressed slices.
  void TestMixedCompressionEgroup();

  // Tests sync op for Mixed compression egroups.
  void TestEgroupSyncSlicesForMixedCompressionEgroup();

  // Test tentative update abort on recovery.
  void TestAbortOnRecovery(bool first_update_tentative);

  // Test abort on recovery where all updates, starting at intent sequence 0
  // are aborted.
  void TestAbortOnRecoveryAllUpdatesRollback();

  // Test tentative update abort on recovery using a non-default fallocate
  // upalignment.
  void TestAbortOnRecoveryWithCustomUpalignment();

  // Test dependent write handling in the presence of a failure.
  void TestAbortedWrite();

  // Test egroup state rollback with an aborted write.
  void TestEGStateWithAbortedWrite();

  // Test that a read that happens between out of order writes doesn't result
  // in a deadlock
  void TestOutoforderWritesWithInterleavedRead();

  // Test that verifies the disk WAL is correctly recovered when the checkpoint
  // file contains a non-finalized replication tentative update followed by
  // older write tentative updates in the following delta files. (ENG-65008).
  void TestNonFinalizedCheckpointDuringReplication(
    bool crash_before_replica_finalization);

  // Tests whether extent store writes updates the medusa field
  // next_slice_allocation_offset_hint.
  void TestNextSliceAllocationOffsetHintUpdate();

  // Test that reads multiple sparse regions of an extent.
  void TestSparseMultiRegionRead();

  // Test that disk usage is reset when AES egroups are deleted.
  void TestDiskUsageOnDelete();

  // Test that the last_scrub_time_secs field for the egroups are updated on
  // the first write and after migration.
  void TestUpdateLastScrubTime(bool managed_by_aes);

  // Test to ensure that race between write and Medusa check op doens't cause
  // corruption of slice metadata.
  void TestRaceWriteAndMedusaCheckOp();

  // Tests to ensure owner vdisk id changes are reflected in AESDB as well via
  // estore background scan OPs.
  void TestOwnerVDiskChange();

  // Issues a series of IO to a new egroup and verifies if the primary and
  // secondary disk usage matches and is accurate with the on-disk usage.
  void TestPrimaryAndSecondaryDiskUsageCalculation();

  // Test the op which converts a non-AES egroup to an AES egroup. If
  // 'checkpoint_before_recovery' is set to true, then the disk WAL is
  // checkpointed before recovering the WAL.
  void TestConversionOp(bool checkpoint_before_recovery);

  // Test the case where an egroup gets converted while background scan is in
  // progress.
  void TestConversionInteropWithBgScan();

  // Tests the case where a converted egroup gets replicated to a disk where
  // an old non-AES zombie egroup state exists.
  void TestReplicateAESEgroupToNonAESZombie();

  // Reset the bg scan related gflags for all stargate instances.
  void ResetStargateBgScanGFlag();

  // Verify stargate handling on finding unclaimed disks during extent store
  // initialization.
  void TestSpuriousDiskDir();

 private:
  void SetDiskQueueDropOpMode(bool enable);
  void SetSSDDiskQueueAdmissionThreshold(int threshold);

  // Return the next eligible egroup id for test op.
  int64 GetNextEgroupId();

  // Return the timestamp of the last completed estore disk usage recomputation
  // cycle for the specified disk.
  string FetchLastDiskUsageCompleted(int64 disk_id);

  // Starts num_updates writes on different slices and aborts a write on a
  // particular update (count starts from 0 to num_updates - 1). The updates
  // can be specified to fully/partially overwrite existing slices or create
  // newer slices. The writes can be followed by optional truncation of egroup
  // with or without error injection. If 'restart_stargate' is true then
  // stargate is restarted with all the stacked tentative updates.
  // if 'test_same_slice_overwrite' is set to true then write after the
  // aborted update are done to the slices written previously.
  void TestEGStateWithAbortedWriteHelper(
    int32 num_updates,
    int32 update_to_abort,
    int32 new_slices_pct = 0,
    int32 partial_overwrite_pct = 0,
    bool truncate_egroup = false,
    bool fail_truncate = false,
    bool replicate_egroup = false,
    bool restart_stargate = false,
    bool test_same_slice_overwrite = false);

  // Method that creates some erasure coded egroups and overwrites them
  // multiple times.
  void TestOverwriteErasureCodedEgroupInternal();

  // Checkpoints WAL on all disks.
  void CheckpointDiskWALs();

  // Prints the disk usage stats for the primary and secondary disk.
  void PrintDiskUsageStats(
    int64 primary_disk_usage_before_op = -1,
    int64 secondary_disk_usage_before_op = -1,
    int64 primary_disk_usage_after_op = -1,
    int64 secondary_disk_usage_after_op = -1);

  // Internal helper function to perform extent store write on 'egroup_id' of
  // 'write_size_bytes' to 'num_vblocks' each. 'overwrites' mean that the
  // writes will overwrite to the same region. 'compression' sends a
  // transformation type to the request. By the end of the write, both primary
  // and secondary disk usage will be computed and determined if they match
  // with the expected values.
  void WriteAndVerifyDiskUsage(
    int64 egroup_id, int64 write_size_bytes, int64 num_vblocks,
    bool overwrites = false, bool compression = false, int32 num_regions = 1,
    bool replacement_slices = false);

  // Callback function used by a local write function as a done_cb for a
  // WriteExtentGroup RPC.
  void WriteExtentGroupDone(
    StargateError::Type err,
    shared_ptr<string> err_detail,
    shared_ptr<WriteExtentGroupRet> ret,
    Notification *notify = nullptr);

  // Fetches the list of egroups from 'disk_id'. 'egroups_ret' contains the
  // actual list and 'cookie' is set by stargate to continue calling
  // FetchEgroups for more egroups.
  void FetchEgroups(int64 disk_id, FetchEgroupsRet *const egroups_ret,
                    string *cookie);

  // Issue a migration request for the 'egroup_id' to the specified desired
  // replica disk ids.
  void MigrateEgroups(
    int64 vdisk_id,
    int stargate_index,
    int64 egroup_id,
    vector<int64> desired_replica_disk_ids);

  // Callback invoked when the egroup migration request completes.
  void MigrateEgroupsDone(
    nutanix::stargate::StargateError::Type err,
    const shared_ptr<string>& err_detail,
    const shared_ptr<FixExtentGroupsRet>& ret,
    const shared_ptr<FixExtentGroupsArg>& arg,
    Notification *notification);

  // Returns true if the disk is SSD.
  bool IsSSDDisk(int64 disk_id);

 private:
  // Contains the latest egroup_id which can be used for the test.
  int64 egroup_id_;
};

//-----------------------------------------------------------------------------

EgroupOpsTest::EgroupOpsTest(const string& config_file_name) :
  EgroupOpsTester(config_file_name),
  egroup_id_(1000) {

  // There should be at least 2 disks.
  CHECK_GE(disk_ids_.size(), 2);
}

//-----------------------------------------------------------------------------

EgroupOpsTest::~EgroupOpsTest() {
}

//-----------------------------------------------------------------------------

int64 EgroupOpsTest::GetNextEgroupId() {
  ++egroup_id_;
  return egroup_id_;
}

//-----------------------------------------------------------------------------

bool EgroupOpsTest::IsSSDDisk(const int64 disk_id) {
  Configuration::PtrConst config = cluster_mgr_->Config();
  unordered_map<int64, const ConfigurationProto::Disk *>::const_iterator it =
    config->disk_id_map().find(disk_id);
  CHECK(it != config->disk_id_map().end()) << disk_id;
  return (it->second->storage_tier() == "SSD-SATA" ||
          it->second->storage_tier() == "SSD-PCIe");
}

//-----------------------------------------------------------------------------

static void Wait(Notification *notif) {
  notif->Notify();
}

void EgroupOpsTest::PrintDiskUsageStats(
  const int64 primary_disk_usage_before_op,
  const int64 secondary_disk_usage_before_op,
  const int64 primary_disk_usage_after_op,
  const int64 secondary_disk_usage_after_op) {

  ostringstream oss;
  oss << "Disk usage - disk " << disk_ids_[0] << " before="
      << primary_disk_usage_before_op << " after="
      << primary_disk_usage_after_op;
  if (disk_ids_.size() > 1) {
    CHECK_GE(secondary_disk_usage_after_op, 0);
    CHECK_GT(disk_ids_[1], 0);
    oss << " disk " << disk_ids_[1] << " before="
        << secondary_disk_usage_before_op << " after="
        << secondary_disk_usage_after_op;
  }

  LOG(INFO) << oss.str();
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::TestImmutableEgroupTruncated() {
  LOG(INFO) << "Running " << __FUNCTION__;
  const int64 egroup_id = GetNextEgroupId();

  //---------------------------------------------------------------------------
  // Do 2 writes.
  // 1) First write will cause more fallocation than necessary.
  // 2) Second write will set "immutable_extents && !further_writes", which
  //    should cause truncation.
  //---------------------------------------------------------------------------

  //---------------------------------------------------------------------------
  // 1st write.
  //---------------------------------------------------------------------------

  auto arg = WriteEgArg();
  arg->set_extent_group_id(egroup_id);
  arg->set_is_sequential(true);
  arg->add_disk_ids(disk_ids_[0]);
  arg->add_disk_ids(disk_ids_[1]);
  arg->set_managed_by_aes(
    FLAGS_egroup_test_estore_aes_enabled);
  if (FLAGS_egroup_test_estore_aes_enabled) {
    arg->set_intent_sequence(-1);
    arg->set_global_metadata_intent_sequence(0);
    arg->set_slice_group_size(32);
    arg->set_slices_stored_by_id(false);
    arg->set_vdisk_incarnation_id(123);
  } else {
    arg->set_intent_sequence(0 /* current_intent_sequence */);
  }

  // Regions.
  // We're going to write just 8 byte. Then test whether egroup is truncated to
  // 8 byte.
  auto primary = arg->mutable_primary();
  auto ex = primary->add_extents();
  auto eid = ex->mutable_extent_id();
  eid->set_vdisk_block(3);
  eid->set_owner_id(vdisk_id_);
  ex->add_region_offset(0);
  ex->add_region_length(8);

  const StargateInterface::Ptr& ifc =
    cluster_mgr_->stargate_mgr()->GetStargateInterfaceForDisk(disk_ids_[0]);
  CHECK(ifc);

  // Generate 8 bytes ot data.
  IOBuffer::Ptr data = make_shared<IOBuffer>();
  data->Clear();
  auto sub = Random::CreateRandomDataIOBuffer(8 /* bytes */);
  data->AppendIOBuffer(sub.get());

  const uint stargate_index_for_secondary_replica =
    cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(
      disk_ids_[1]);
  CHECK_GE(stargate_index_for_secondary_replica, 0) << disk_ids_[1];
  LOG(INFO) << "Fetching usage and threshold pct from disk " << disk_ids_[1]
            << ", stargate_idx=" << stargate_index_for_secondary_replica;
  const string fallocate_alignment_threshold_pct =
    GetStargateGFlag(
      "stargate_disk_manager_fallocate_alignment_threshold_pct",
      stargate_index_for_secondary_replica);

  SetStargateGFlag(
    "stargate_disk_manager_fallocate_alignment_threshold_pct", "0",
    stargate_index_for_secondary_replica);
  SetStargateGFlag(
    "estore_compute_secondary_disk_usage_from_primary", "false",
    stargate_index_for_secondary_replica);
  SpaceUsage usage(cluster_mgr_);
  CHECK_GT(container_id_, 0);
  usage.RegisterContainerId(container_id_);
  const int64 usage_disk1 = usage.GetUsage(disk_ids_[0]).first;
  const int64 usage_disk2 = usage.GetUsage(disk_ids_[1]).first;
  const int64 disk1_garbage_before_write =
    usage.GetGarbage(disk_ids_[0]).first;
  const int64 disk2_garbage_before_write =
    usage.GetGarbage(disk_ids_[1]).first;
  LOG(INFO) << "Disk garbage before write - primary: " << disk_ids_[0]
            << " garbage: " << disk1_garbage_before_write
            << ", secondary: " << disk_ids_[1]
            << " garbage: " << disk2_garbage_before_write;
  Notification notif;
  const Function<void(StargateError::Type,
                      shared_ptr<string>,
                      shared_ptr<WriteExtentGroupRet>)> done_cb =
    func_disabler_.Bind(&Wait, &notif);
  WriteExtentGroup(arg, data, done_cb);
  notif.Wait();
  // Sleep for couple of seconds before querying the stats so that stargate
  // publishes the stats to arithmos before the test looks it up.
  sleep(5);
  const int64 updated_usage_disk1 = usage.GetUsage(disk_ids_[0]).first;
  const int64 updated_usage_disk2 = usage.GetUsage(disk_ids_[1]).first;
  const int64 disk1_garbage_after_write = usage.GetGarbage(disk_ids_[0]).first;
  PrintDiskUsageStats(
    usage_disk1, usage_disk2, updated_usage_disk1, updated_usage_disk2);
  LOG(INFO) << "Verifying disk usage for primary disk " << disk_ids_[0]
            << " after writing 8 bytes";
  const int64 num_user_write_bytes = max(8, FLAGS_estore_regular_slice_size);
  usage.LookupAndCompare(
    "primary-disk-usage-verification-after-8-byte-write",
    usage_disk1 + num_user_write_bytes /* num_bytes_written */,
    -1 /* total_bytes_untransformed */,
    usage_disk1 + num_user_write_bytes
      /* container_unshared_usage_transformed */,
    -1 /* unshared_bytes_untransformed */,
    -1 /* garbage_bytes_transformed */,
    -1 /* garbage_bytes_untransformed */,
    disk1_garbage_after_write /* acceptable_deviation_bytes */,
    disk_ids_[0]);
  LOG(INFO) << "Verifying disk usage for secondary disk " << disk_ids_[1]
            << " after writing 8 bytes";
  const int64 disk2_garbage_after_write =
    usage.GetGarbage(disk_ids_[1]).first;
  usage.LookupAndCompare(
    "secondary-disk-usage-verification-after-8-byte-write",
    usage_disk2 + num_user_write_bytes /* num_bytes_written */,
    -1 /* total_bytes_untransformed */,
    usage_disk2 + num_user_write_bytes
      /* container_unshared_usage_transformed */,
    -1 /* unshared_bytes_untransformed */,
    -1 /* garbage_bytes_transformed */,
    -1 /* garbage_bytes_untransformed */,
    disk2_garbage_after_write /* acceptable_deviation_bytes */,
    disk_ids_[1]);
  SetStargateGFlag(
    "estore_compute_secondary_disk_usage_from_primary",
    "true",
    stargate_index_for_secondary_replica);
  SetStargateGFlag(
    "stargate_disk_manager_fallocate_alignment_threshold_pct",
    fallocate_alignment_threshold_pct,
    stargate_index_for_secondary_replica);

  //---------------------------------------------------------------------------
  // 2nd write.
  //---------------------------------------------------------------------------

  // These special parameters are needed to trigger truncate of egroup.
  arg->set_writing_immutable_extents(true);
  arg->set_expect_further_writes_hint(false);
  if (FLAGS_egroup_test_estore_aes_enabled) {
    arg->set_intent_sequence(-1);
    arg->set_global_metadata_intent_sequence(0);
    arg->set_slice_group_size(32);
    arg->set_slices_stored_by_id(false);
    arg->set_vdisk_incarnation_id(123);
  } else {
    arg->set_expected_intent_sequence(0);
    arg->set_intent_sequence(1);
  }

  notif.Reset();
  WriteExtentGroup(arg, data, done_cb);
  notif.Wait();

  // Wait for truncate op to finish.
  sleep(30);

  unordered_map<int64, int32> egid_2_falloc_len_map;
  FetchEgroupFallocateLength(disk_ids_[0], &egid_2_falloc_len_map);

  const int64 KiB = 1024;
  CHECK(egid_2_falloc_len_map.find(egroup_id) != egid_2_falloc_len_map.end());
  CHECK_EQ(egid_2_falloc_len_map[egroup_id], 32 * KiB);
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::TestOverwriteErasureCodedEgroup() {
  LOG(INFO) << "Running " << __FUNCTION__;
  // Ensure that we have at least 5 disks available since we will create 3/2
  // strips for this test.
  CHECK_GE(disk_ids_.size(), 5);
  // Perform the overwrite test in a loop a few times.
  for (uint ii = 0; ii < 2; ++ii) {
    TestOverwriteErasureCodedEgroupInternal();
  }
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::TestWriteExtentGroupAtPostWriteIntentSequence() {
  LOG(INFO) << "Running " << __FUNCTION__;

  // Do not run disk usage op while the test is running since the egroup might
  // end up getting deleted while we're trying to fetch the state.
  SetStargateGFlag(
    "disk_manager_experimental_disable_disk_usage_op", "true");
  SetStargateGFlag("estore_experimental_background_scan_pause_processing",
                   "true");
  const string max_outstanding_medusa_check_ops =
    GetStargateGFlag("estore_max_outstanding_medusa_check_ops");
  SetStargateGFlag("estore_max_outstanding_medusa_check_ops", "0");
  const int64 egroup_id = GetNextEgroupId();
  const int64 intent_sequence = 10;
  const int64 post_write_intent_sequence = 5;
  // We will write an extent group at an intent sequence other than that
  // specified as the intent sequence in the 'WriteExtentGroupArg'.
  auto arg = WriteEgArg();
  arg->set_extent_group_id(egroup_id);
  arg->set_intent_sequence(intent_sequence);
  arg->add_disk_ids(disk_ids_[0]);
  arg->add_disk_ids(disk_ids_[1]);
  arg->set_post_write_intent_sequence(post_write_intent_sequence);

  auto primary = arg->mutable_primary();
  auto ex = primary->add_extents();
  auto eid = ex->mutable_extent_id();
  eid->set_vdisk_block(3);
  eid->set_owner_id(vdisk_id_);
  ex->add_region_offset(0);
  ex->add_region_length(1024);

  const StargateInterface::Ptr& ifc =
    cluster_mgr_->stargate_mgr()->GetStargateInterfaceForDisk(disk_ids_[0]);
  CHECK(ifc);

  // Generate 1K bytes ot data.
  IOBuffer::Ptr data = make_shared<IOBuffer>();
  data->Clear();
  auto sub = Random::CreateRandomDataIOBuffer(1024 /* bytes */);
  data->AppendIOBuffer(sub.get());

  LOG(INFO) << "Writing extent group " << egroup_id << " with intent seq "
            << intent_sequence << ", and latest applied intent seq "
            << post_write_intent_sequence;
  Notification notify;
  const Function<void(StargateError::Type,
                      shared_ptr<string>,
                      shared_ptr<WriteExtentGroupRet>)> done_cb =
    func_disabler_.Bind(&Wait, &notify);
  SetStargateGFlag(
    "estore_compute_secondary_disk_usage_from_primary", "false");
  SpaceUsage usage(cluster_mgr_);
  CHECK_GT(container_id_, 0);
  usage.RegisterContainerId(container_id_);
  const int64 usage_disk1 = usage.GetUsage(disk_ids_[0]).first;
  const int64 usage_disk2 = usage.GetUsage(disk_ids_[1]).first;
  WriteExtentGroup(arg, data, done_cb);
  notify.Wait();
  // Sleep for couple of seconds before querying the stats so that stargate
  // publishes the stats to arithmos before the test looks it up.
  sleep(2);
  const int64 updated_usage_disk1 = usage.GetUsage(disk_ids_[0]).first;
  const int64 updated_usage_disk2 = usage.GetUsage(disk_ids_[1]).first;
  const int64 garbage_disk1 = usage.GetGarbage(disk_ids_[0]).first;
  const int64 garbage_disk2 = usage.GetGarbage(disk_ids_[1]).first;
  PrintDiskUsageStats(
    usage_disk1, usage_disk2, updated_usage_disk1, updated_usage_disk2);
  LOG(INFO) << "Verifying disk usage for primary disk " << disk_ids_[0]
            << " after writing 1K bytes";
  const int64 num_user_write_bytes =
    max(1024, FLAGS_estore_regular_slice_size);
  usage.LookupAndCompare(
    "primary-disk-usage-verification-after-1K-write",
    usage_disk1 + num_user_write_bytes /* num_bytes_written */,
    -1 /* total_bytes_untransformed */,
    usage_disk1 + num_user_write_bytes
      /* container_unshared_usage_transformed */,
    -1 /* unshared_bytes_untransformed */,
    -1 /* garbage_bytes_transformed */,
    -1 /* garbage_bytes_untransformed */,
    garbage_disk1,
    disk_ids_[0]);
  LOG(INFO) << "Verifying disk usage for secondary disk " << disk_ids_[1]
            << " after writing 1K bytes";
  usage.LookupAndCompare(
    "secondary-disk-usage-verification-after-1K-byte-write",
    usage_disk2 + num_user_write_bytes /* num_bytes_written */,
    -1 /* total_bytes_untransformed */,
    usage_disk2 + num_user_write_bytes
      /* container_unshared_usage_transformed */,
    -1 /* unshared_bytes_untransformed */,
    -1 /* garbage_bytes_transformed */,
    -1 /* garbage_bytes_untransformed */,
    garbage_disk2,
    disk_ids_[1]);
  SetStargateGFlag(
    "estore_compute_secondary_disk_usage_from_primary", "true");

  // Ensure that the write wrote the egroup at the
  // 'post_write_intent_sequence'.
  for (uint ii = 0; ii <= 1; ++ii) {
    interface::GetEgroupStateRet egroup_state;
    GetEgroupState(egroup_id, disk_ids_[ii], intent_sequence + 1,
                   &egroup_state, false /* managed_by_aes */,
                   false /* extent_based_format */, nullptr /* error_ret */,
                   false /* set_latest_applied_intent_sequence */);
    CHECK_EQ(egroup_state.latest_applied_intent_sequence(),
             post_write_intent_sequence);
    LOG(INFO) << "Found replica " << disk_ids_[ii] << " of egroup "
              << egroup_id << " at post write intent sequence "
              << post_write_intent_sequence;
  }
  SetStargateGFlag("estore_experimental_background_scan_pause_processing",
                   "false");
  SetStargateGFlag(
    "disk_manager_experimental_disable_disk_usage_op", "false");
  SetStargateGFlag(
    "estore_max_outstanding_medusa_check_ops",
    max_outstanding_medusa_check_ops);
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::TestAESWriteParity(const bool inject_error) {
  LOG(INFO) << "Running " << __FUNCTION__ << " inject_error " << inject_error;
  // Ensure that we have at least 5 disks available since we will create 3/2
  // strips for this test.
  CHECK_GE(disk_ids_.size(), 5);

  const vector<int64> info_disk_ids =
    { disk_ids_[0], disk_ids_[1], disk_ids_[2] };
  const vector<int64> info_egroup_ids =
    { GetNextEgroupId(), GetNextEgroupId(), GetNextEgroupId() };
  const int64 primary_parity_egroup_id = GetNextEgroupId();
  const int64 primary_parity_disk_id = disk_ids_[3];
  const int64 secondary_parity_egroup_id = GetNextEgroupId();
  const int64 secondary_parity_disk_id = disk_ids_[4];
  const vector<int64> parity_disk_ids =
    { primary_parity_disk_id, secondary_parity_disk_id };
  const vector<int64> parity_egroup_ids =
    { primary_parity_egroup_id, secondary_parity_egroup_id };

  vector<vector<SliceState::Ptr>> info_slice_state_vecs;
  vector<SliceState::Ptr> primary_parity_slice_state_vec;
  vector<SliceState::Ptr> secondary_parity_slice_state_vec;
  vector<vector<shared_ptr<EgroupOpsTester::Extent>>> info_extent_state_vecs;

  // Write a strip with untransformed info extent group.
  LOG(INFO) << "Writing untrasformed erasure code strip "
            << JoinWithSeparator(info_egroup_ids, ",");

  // Set error injection types if requested by the caller. We will test
  // rolling forward of the primary parity and the associated info egroup
  // metadata residing on the parity replica. We will test the roll back of
  // the secondary parity and associated info egroup metadata residing on
  // the secondary parity replica.
  const int32 primary_parity_inject_error_type =
    inject_error ? 9 /* kNoFinalizeTentativeUpdates */ : -1;
  const int32 secondary_parity_inject_error_type =
    inject_error ? 6 /* kNoSliceWritesAfterTentativeUpdates */ : -1;

  vector<IOBuffer::Ptr> parity_data_vec;
  vector<shared_ptr<ReplicaReadEgroupRet>> remote_ret_vec;
  remote_ret_vec.reserve(info_egroup_ids.size());
  const bool should_contain_marker = !inject_error;
  WriteErasureCodedStripHelper(
    info_egroup_ids, info_disk_ids,
    primary_parity_egroup_id, primary_parity_disk_id,
    secondary_parity_egroup_id, secondary_parity_disk_id,
    &info_slice_state_vecs,
    &primary_parity_slice_state_vec,
    &secondary_parity_slice_state_vec,
    &info_extent_state_vecs,
    false /* compressible_data */,
    0 /* compress_percentage */,
    true /* managed_by_aes */,
    primary_parity_inject_error_type,
    secondary_parity_inject_error_type,
    &remote_ret_vec,
    &parity_data_vec);

  const uint primary_parity_stargate_index =
    cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(
      parity_disk_ids.at(0));
  const uint secondary_parity_stargate_index =
    cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(
      parity_disk_ids.at(1));
  for (uint kk = 0; kk < 2; ++kk) {
    // We verify twice, one before restarting stargates, one after.
    if (kk > 0) {
      // Restart an info egroup replica also. This ensures EC marker
      // placed on an info egroup data replica is carried forward after
      // the restart.
      RestartStargateAtIndex(
        cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(
          info_disk_ids.at(0)));

      // Restart the parity egroup replicas to ensure the mapping between
      // global to local, as well as all the info egroup metadata is carried
      // forward.
      LOG(INFO) << "Restarting stargate " << primary_parity_stargate_index;
      RestartStargateAtIndex(primary_parity_stargate_index);
      LOG(INFO) << "Restarting stargate " << secondary_parity_stargate_index;
      RestartStargateAtIndex(secondary_parity_stargate_index);

      // Ensure to give a little time for WAL recovery to complete.
      sleep(10);
    }

    // Verifies whether the metadata persisted on the parity replicas is
    // consistent with what was written on the original info egroup data
    // replicas.
    VerifyAESECStripMetadata(info_egroup_ids,
                             info_disk_ids,
                             parity_egroup_ids,
                             parity_disk_ids,
                             info_slice_state_vecs,
                             info_extent_state_vecs,
                             primary_parity_inject_error_type,
                             secondary_parity_inject_error_type,
                             should_contain_marker);
  }

  // Checkpoint the Disk WALs.
  LOG(INFO) << "Checkpoint the disk WALs";
  CheckpointDiskWALs();

  // Restart the primary parity replica to check if WAL recovery can rightly
  // capture the parity EC marker (which includes the corresponding local
  // info egroup mapping that must get applied to the in-memory eg state
  // based on the above checkpoint operation).
  RestartStargateAtIndex(
    cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(
      parity_disk_ids.at(0)));

  // Verify the strip metadata again. This should now read the metadata
  // from the actual AESDB itself instead of the WAL state.
  LOG(INFO) << "Read the metadata after checkpointing";
  VerifyAESECStripMetadata(info_egroup_ids,
                           info_disk_ids,
                           parity_egroup_ids,
                           parity_disk_ids,
                           info_slice_state_vecs,
                           info_extent_state_vecs,
                           primary_parity_inject_error_type,
                           secondary_parity_inject_error_type,
                           should_contain_marker);

  // Proceed to delete a parity if we had not injected any error in its
  // creation.
  if (!inject_error) {

    // Next, verify the recreation logic of an AES parity egroup.
    unordered_map<int64, shared_ptr<ReplicaReadEgroupRet>> info_egroup_map;
    DCHECK_EQ(info_egroup_ids.size(), remote_ret_vec.size());
    for (uint ii = 0; ii < info_egroup_ids.size(); ++ii) {
      info_egroup_map.emplace(info_egroup_ids.at(ii),
                              remote_ret_vec.at(ii));
    }

    for (uint kk = 0; kk < 2; ++kk) {
      DeleteExtentGroup(primary_parity_egroup_id, primary_parity_disk_id,
                        true /* managed_by_aes */);

      Notification notification;
      WriteParityExtentGroup(
        primary_parity_egroup_id,
        primary_parity_disk_id,
        true /* is_primary_parity */,
        info_egroup_ids,
        parity_egroup_ids,
        parity_data_vec.at(0),
        &notification,
        &primary_parity_slice_state_vec,
        true /* managed_by_aes */,
        &info_egroup_map,
        true /* decode_request */,
        primary_parity_inject_error_type);

      if (kk != 0) {
        LOG(INFO) << "Restarting primary parity stargate index "
                  << primary_parity_stargate_index;
        RestartStargateAtIndex(primary_parity_stargate_index);
        CheckpointDiskWALs();
      }

      for (uint ii = 0; ii < info_egroup_ids.size(); ++ii) {
        const int64 info_egroup_id = info_egroup_ids.at(ii);

        // Read replica on secondary parity for this info egroup and verify
        // the metadata is consistent with its primary replica.
        LOG(INFO) << "Reading the new primary parity replica for the info "
                  << "egroup " << info_egroup_id;
        IOBuffer::Ptr transformed_data = make_shared<IOBuffer>();
        ReplicaReadEgroupAndVerifyMetadata(
          primary_parity_egroup_id, primary_parity_disk_id,
          info_slice_state_vecs.at(ii),
          info_extent_state_vecs.at(ii), &transformed_data,
          true /* managed_by_aes */,
          should_contain_marker, primary_parity_inject_error_type,
          0 /* expected_global_intent_sequence */, info_egroup_id);
      }
    }

    // Test that deletes an AES parity egroup.
    DeleteExtentGroup(secondary_parity_egroup_id, secondary_parity_disk_id,
                      true /* managed_by_aes */);

    // Next, verify the replication logic of an AES parity egroup.
    for (uint kk = 0; kk < 2; ++kk) {
      // Flush physical states into map4 so that on stargate restart
      // we will not have recovered eg_state in memory.
      CheckpointDiskWALs();

      // We verify twice, one before restarting stargates, one after.
      if (kk > 0) {
        LOG(INFO) << "Restarting stargate " << secondary_parity_stargate_index;
        RestartStargateAtIndex(secondary_parity_stargate_index);
      }

      // Test that replicates an AES parity egroup.
      vector<shared_ptr<EgroupOpsTester::SliceState>> slice_state_vec;
      vector<medusa::ExtentIdProto> extent_id_list;
      // We replicate the primary parity egroup twice:
      // (1) from primary_parity_disk_id to secondary_parity_disk_id.
      // (2) from secondary_parity_disk_id to disk_ids[0]
      ReplicateExtentGroup(primary_parity_egroup_id,
                           kk == 0 ? primary_parity_disk_id
                                  : secondary_parity_disk_id
                           /* source_disk_id */,
                           kk == 0 ? secondary_parity_disk_id
                                   : disk_ids_[0]
                           /* dest_disk_id */,
                           slice_state_vec,
                           0 /* latest_intent_sequence */,
                           true /* managed_by_aes */,
                           false /* extent_based_format */,
                           container_id_,
                           &extent_id_list);
      for (uint ii = 0; ii < info_egroup_ids.size(); ++ii) {
        const int64 info_egroup_id = info_egroup_ids.at(ii);

        // Read replica on secondary parity for this info egroup and verify
        // the metadata is consistent with its primary replica.
        LOG(INFO) << "Reading the new primary parity replica for the info "
                  << "egroup " << info_egroup_id;
        IOBuffer::Ptr transformed_data = make_shared<IOBuffer>();
        ReplicaReadEgroupAndVerifyMetadata(
          primary_parity_egroup_id, secondary_parity_disk_id,
          info_slice_state_vecs.at(ii),
          info_extent_state_vecs.at(ii), &transformed_data,
          true /* managed_by_aes */,
          should_contain_marker,
          secondary_parity_inject_error_type,
          0 /* expected_global_intent_sequence */, info_egroup_id);
      }
    }

    // Now pick an info egroup and delete the same. Then, issue a write
    // request on the same replica similar to how decode op issues the
    // secondary write request. This must successfully re-create the info
    // egroup on the deleted replica.
    const int64 info_egroup_id = info_egroup_ids.at(0);
    const int64 info_disk_id = info_disk_ids.at(0);
    shared_ptr<ReplicaReadEgroupRet> remote_ret;
    IOBuffer::Ptr data_to_write = make_shared<IOBuffer>();
    ReplicaReadEgroup(
      info_egroup_id, info_disk_id,
      vector<shared_ptr<EgroupOpsTester::SliceState>>()
        /* slice_state_vec */,
      &data_to_write,
      StargateError::kNoError /* expected_error */,
      true /* managed_by_aes */, nullptr /* user_notification */,
      &remote_ret);
    CHECK(remote_ret && remote_ret.get())
      << info_egroup_id << " disk_id" << info_disk_id;
    CHECK(remote_ret->has_extent_group_metadata())
      << data_to_write->size()
      << " " << info_egroup_id;

    DeleteExtentGroup(info_egroup_id, info_disk_id,
                      true /* managed_by_aes */);

    RestartStargateAtIndex(
      cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(
        info_disk_id));

    sleep(10);
    SetStargateGFlag("estore_aes_checkpoint_applied_state", "true");
    // Checkpoint the disk WALs here. This ensures all the above physical
    // state is flushed to the rocksdb backend.
    CheckpointDiskWALs();
    SetStargateGFlag("estore_aes_checkpoint_applied_state", "false");

#ifndef NDEBUG
    LOG(INFO) << "Initiating failed recreation of the info egroup "
              << OUTVARS(info_egroup_id, info_disk_id);
    // Issue a write that is injected with the appropriate error that will
    // cause the stargate on that replica to crash after placing the TU.
    WriteInfoExtentGroup(
      info_egroup_id,
      info_disk_id,
      true /* managed_by_aes */,
      true /* decode_request */,
      data_to_write,
      remote_ret,
      6 /* slice_write_inject_error_type */);

    // Wait for the WAL recovery to complete after the injected stargate
    // crash.
    sleep(10);
#endif

    LOG(INFO) << "Initiating successful recreation of the info egroup "
              << OUTVARS(info_egroup_id, info_disk_id);
    // Issue a write that is expected to succeed now. If the WAL recovery hits
    // a crash loop in this scenario, this write will fail.
    WriteInfoExtentGroup(
      info_egroup_id,
      info_disk_id,
      true /* managed_by_aes */,
      true /* decode_request */,
      data_to_write,
      remote_ret);
  }
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::TestAESErasureCoding(
  const bool inject_error,
  const int64 replication_bytes) {

  LOG(INFO) << "Running " << __FUNCTION__ << " " << inject_error
            << " replication bytes " << replication_bytes;
  // Ensure that we have at least 5 disks available since we will create 3/2
  // strips for this test.
  CHECK_GE(disk_ids_.size(), 5);

  const vector<int64> info_disk_ids =
    { disk_ids_[0], disk_ids_[1], disk_ids_[2] };
  const vector<int64> info_egroup_ids =
    { GetNextEgroupId(), GetNextEgroupId(), GetNextEgroupId() };

  vector<vector<SliceState::Ptr>> info_slice_state_vecs;
  vector<SliceState::Ptr> primary_parity_slice_state_vec;
  vector<SliceState::Ptr> secondary_parity_slice_state_vec;
  vector<vector<shared_ptr<EgroupOpsTester::Extent>>> info_extent_state_vecs;

  // Write a strip with untransformed info extent group.
  LOG(INFO) << "Writing untrasformed erasure code strip "
            << JoinWithSeparator(info_egroup_ids, ",");

  vector<shared_ptr<ReplicaReadEgroupRet>> remote_ret_vec;
  remote_ret_vec.reserve(info_egroup_ids.size());

  string old_value;
  if (replication_bytes > 0) {
    // Set the expected replication bytes on all the stargates. This will
    // ensure that any read performed for erasure coding involves reading at a
    // smaller chunk unit.
    cluster_mgr_->stargate_mgr()->SetGFlag(
      "estore_replication_work_unit_bytes",
      to_string(replication_bytes), -1 /* svm_id */,
      false /* retry */, &old_value);
  }

  // Invoke the strip helper function to create the info egroups.
  // Parities can be created later when the encoding is issued.
  WriteErasureCodedStripHelper(
    info_egroup_ids, info_disk_ids,
    -1 /* primary_parity_egroup_id */,
    -1 /* primary_parity_disk_id */,
    -1 /* secondary_parity_egroup_id */,
    -1 /* secondary_parity_disk_id */,
    &info_slice_state_vecs,
    &primary_parity_slice_state_vec,
    &secondary_parity_slice_state_vec,
    &info_extent_state_vecs,
    false /* compressible_data */,
    0 /* compress_percentage */,
    true /* managed_by_aes */,
    -1 /* primary_parity_inject_error_type */,
    -1 /* secondary_parity_inject_error_type */,
    &remote_ret_vec);

  DCHECK_EQ(remote_ret_vec.size(), info_egroup_ids.size());
  DCHECK_EQ(info_slice_state_vecs.size(), info_egroup_ids.size());
  DCHECK_EQ(info_extent_state_vecs.size(), info_egroup_ids.size());

  // Restart all the participant stargates.
  for (uint ii = 0; ii < info_disk_ids.size(); ++ii) {
    RestartStargateAtIndex(
      cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(
        info_disk_ids.at(ii)));
  }

  // Checkpoint the disk WALs here. This ensures all the above physical
  // state is flushed to the rocksdb backend.
  CheckpointDiskWALs();

  // Restart all the participant stargates.
  for (uint ii = 0; ii < info_disk_ids.size(); ++ii) {
    RestartStargateAtIndex(
      cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(
        info_disk_ids.at(ii)));
  }

  // Now proceed to encoding the egroups. The function ensures the
  // egroups are erasure coded and the map3 entries of the info egroups
  // contain the finalizd ec metadata (i.e. parity egroup ids).
  vector<int64> parity_egroup_id_vec;
  vector<int64> parity_disk_id_vec;
  int64 error_inject_egid = -1;
  ErasureEncodeAESEgroups(info_egroup_ids, info_disk_ids,
                          &parity_egroup_id_vec,
                          &parity_disk_id_vec,
                          inject_error,
                          2 /* parity_size */,
                          &error_inject_egid);

  const bool should_contain_marker = !inject_error;
  if (!inject_error) {
    const uint primary_parity_stargate_index =
      cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(
        parity_disk_id_vec.at(0));
    const uint secondary_parity_stargate_index =
      cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(
        parity_disk_id_vec.at(1));

    // Restart all the participant stargates.
    for (uint ii = 0; ii < info_disk_ids.size(); ++ii) {
      RestartStargateAtIndex(
        cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(
          info_disk_ids.at(ii)));
    }
    RestartStargateAtIndex(primary_parity_stargate_index);
    RestartStargateAtIndex(secondary_parity_stargate_index);

    SetStargateGFlag("estore_aes_checkpoint_applied_state", "true");

    // Checkpoint the Disk WALs.
    LOG(INFO) << "Checkpoint the disk WALs";
    CheckpointDiskWALs();

    SetStargateGFlag("estore_aes_checkpoint_applied_state", "false");

    // Restart all the participant stargates again.
    for (uint ii = 0; ii < info_disk_ids.size(); ++ii) {
      RestartStargateAtIndex(
        cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(
          info_disk_ids.at(ii)));
    }
    RestartStargateAtIndex(primary_parity_stargate_index);
    RestartStargateAtIndex(secondary_parity_stargate_index);

    // Verify the strip metadata again. This should now read the metadata
    // from the actual AESDB itself instead of the WAL state.
    LOG(INFO) << "Read the metadata after checkpointing";
    VerifyAESECStripMetadata(info_egroup_ids,
                             info_disk_ids,
                             parity_egroup_id_vec,
                             parity_disk_id_vec,
                             info_slice_state_vecs,
                             info_extent_state_vecs,
                             -1 /* primary_parity_inject_error_type */,
                             -1 /* secondary_parity_inject_error_type */,
                             should_contain_marker);
  }

  // Restart stargates. Given that we previously checkpointed after the
  // egroups were written to, thbe below restarts should ensure only the
  // EC marker updates are carried forward upon WAL recovery.
  RestartStargateAtIndex(
    cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(
      info_disk_ids.at(0)));
  if (!inject_error) {
    DCHECK(!parity_disk_id_vec.empty());
    RestartStargateAtIndex(
      cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(
        parity_disk_id_vec.at(0)));
  }

  CheckpointDiskWALs();

  // Confirm that the extent store sees the right erasure code config,
  // for AES info egroups on their primary replica disk ids.
  if (inject_error) {
    DCHECK_EQ(parity_egroup_id_vec.size(), 0);
    DCHECK_EQ(parity_disk_id_vec.size(), 0);
    ValidateAESECConfig(info_egroup_ids, info_disk_ids,
                        error_inject_egid);
    return;
  } else {
    DCHECK_EQ(parity_egroup_id_vec.size(), 2);
    DCHECK_EQ(parity_disk_id_vec.size(), 2);
    ValidateAESECConfig(info_egroup_ids, info_disk_ids,
                        error_inject_egid);
  }

  // Now ensure that the parity replicas does contain the metadata of the
  // info AES egroups.
  const int64 primary_parity_egroup_id = parity_egroup_id_vec[0];
  const int64 secondary_parity_egroup_id = parity_egroup_id_vec[1];
  const int64 primary_parity_disk_id = parity_disk_id_vec[0];
  const int64 secondary_parity_disk_id = parity_disk_id_vec[1];

  for (uint ii = 0; ii < info_egroup_ids.size() && !inject_error; ++ii) {
    const int64 info_egroup_id = info_egroup_ids.at(ii);

    // Sort the slice states, if not already.
    sort(info_slice_state_vecs.at(ii).begin(),
         info_slice_state_vecs.at(ii).end(),
         medusa::SliceState::SliceStatePtrOrderIncreasingFileOffset);

    // Read replica on primary parity for this info egroup and verify
    // the metadata is consistent with its primary replica.
    LOG(INFO) << "Reading primary parity for info egroup " << info_egroup_id;
    IOBuffer::Ptr transformed_data = make_shared<IOBuffer>();
    ReplicaReadEgroupAndVerifyMetadata(
      primary_parity_egroup_id, primary_parity_disk_id,
      info_slice_state_vecs.at(ii),
      info_extent_state_vecs.at(ii), &transformed_data,
      true /* managed_by_aes */,
      should_contain_marker, -1 /* inject_error_type */,
      0 /* expected_global_intent_sequence */, info_egroup_id);

    // Read replica on secondary parity for this info egroup and verify
    // the metadata is consistent with its primary replica.
    LOG(INFO) << "Reading secondary parity for info egroup " << info_egroup_id;
    ReplicaReadEgroupAndVerifyMetadata(
      secondary_parity_egroup_id, secondary_parity_disk_id,
      info_slice_state_vecs.at(ii),
      info_extent_state_vecs.at(ii), &transformed_data,
      true /* managed_by_aes */,
      should_contain_marker, -1 /* inject_error_type */,
      0 /* expected_global_intent_sequence */, info_egroup_id);
  }

  if (replication_bytes > 0) {
    CHECK(!old_value.empty());
    cluster_mgr_->stargate_mgr()->SetGFlag(
      "estore_replication_work_unit_bytes", old_value);
  }

  // Reset the EC configuration back to the default for further tests.
  const int32 strip_size_parity = 0;
  const int32 strip_size_info = 0;
  UpdateErasureCodingConfiguration(strip_size_info, strip_size_parity);
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::TestOverwriteErasureCodedEgroupInternal() {
  const vector<int64> info_disk_ids =
    { disk_ids_[0], disk_ids_[1], disk_ids_[2] };
  const vector<int64> info_egroup_ids =
    { GetNextEgroupId(), GetNextEgroupId(), GetNextEgroupId() };
  const int64 primary_parity_egroup_id = GetNextEgroupId();
  const int64 primary_parity_disk_id = disk_ids_[3];
  const int64 secondary_parity_egroup_id = GetNextEgroupId();
  const int64 secondary_parity_disk_id = disk_ids_[4];

  vector<vector<SliceState::Ptr>> info_slice_state_vecs;
  vector<SliceState::Ptr> primary_parity_slice_state_vec;
  vector<SliceState::Ptr> secondary_parity_slice_state_vec;

  vector<vector<shared_ptr<EgroupOpsTester::Extent>>> info_extent_state_vecs;

  // Write a strip with untransformed info extent group.
  LOG(INFO) << "Writing untrasformed erasure code strip "
            << JoinWithSeparator(info_egroup_ids, ",");

  WriteErasureCodedStripHelper(
    info_egroup_ids, info_disk_ids,
    primary_parity_egroup_id, primary_parity_disk_id,
    secondary_parity_egroup_id, secondary_parity_disk_id,
    &info_slice_state_vecs,
    &primary_parity_slice_state_vec,
    &secondary_parity_slice_state_vec,
    &info_extent_state_vecs,
    false /* compressible_data */,
    0 /* compress_percentage */);

  const vector<int64> compressed_info_egroup_ids =
    { GetNextEgroupId(), GetNextEgroupId(), GetNextEgroupId() };
  const int64 compressed_primary_parity_egroup_id = GetNextEgroupId();
  const int64 compressed_secondary_parity_egroup_id = GetNextEgroupId();

  vector<vector<SliceState::Ptr>> compressed_info_slice_state_vecs;
  vector<SliceState::Ptr> compressed_primary_parity_slice_state_vec;
  vector<SliceState::Ptr> compressed_secondary_parity_slice_state_vec;

  vector<vector<shared_ptr<EgroupOpsTester::Extent>>>
    compressed_extent_state_vecs;

  // Write a strip with compressed info extent group.
  LOG(INFO) << "Writing compressed erasure code strip "
            << JoinWithSeparator(compressed_info_egroup_ids, ",");

  WriteErasureCodedStripHelper(
    compressed_info_egroup_ids, info_disk_ids,
    compressed_primary_parity_egroup_id, primary_parity_disk_id,
    compressed_secondary_parity_egroup_id, secondary_parity_disk_id,
    &compressed_info_slice_state_vecs,
    &compressed_primary_parity_slice_state_vec,
    &compressed_secondary_parity_slice_state_vec,
    &compressed_extent_state_vecs,
    true /* compressible_data */,
    Random::TL()->Uniform(10, 90) /* compress_percentage */);

  // Overwrite the erasure coded info egroups created in a loop a few times.
  for (int ii = 0; ii < 3; ++ii) {
    LOG(INFO) << "Overwriting untransformed erasure code strip "
              << JoinWithSeparator(info_egroup_ids, ",");

    OverwriteErasureCodedStripHelper(
      info_egroup_ids, info_disk_ids,
      primary_parity_egroup_id, primary_parity_disk_id,
      secondary_parity_egroup_id, secondary_parity_disk_id,
      &info_slice_state_vecs,
      &primary_parity_slice_state_vec,
      &secondary_parity_slice_state_vec,
      &info_extent_state_vecs,
      false /* compressible_data */,
      0 /* compress_percentage */);

    LOG(INFO) << "Overwriting compressed erasure code strip "
              << JoinWithSeparator(compressed_info_egroup_ids, ",");

    OverwriteErasureCodedStripHelper(
      compressed_info_egroup_ids, info_disk_ids,
      compressed_primary_parity_egroup_id, primary_parity_disk_id,
      compressed_secondary_parity_egroup_id, secondary_parity_disk_id,
      &compressed_info_slice_state_vecs,
      &compressed_primary_parity_slice_state_vec,
      &compressed_secondary_parity_slice_state_vec,
      &compressed_extent_state_vecs,
      true /* compressible_data */,
      Random::TL()->Uniform(10, 90) /* compress_percentage */);

    LOG(INFO) << "Overwriting compressed erasure code strip "
              << JoinWithSeparator(compressed_info_egroup_ids, ",")
              << " and forcing slice replacements ";
    SetStargateGFlag(
      "estore_experimental_slice_replacement_inverse_prob", "1");

    OverwriteErasureCodedStripHelper(
      compressed_info_egroup_ids, info_disk_ids,
      compressed_primary_parity_egroup_id, primary_parity_disk_id,
      compressed_secondary_parity_egroup_id, secondary_parity_disk_id,
      &compressed_info_slice_state_vecs,
      &compressed_primary_parity_slice_state_vec,
      &compressed_secondary_parity_slice_state_vec,
      &compressed_extent_state_vecs,
      true /* compressible_data */,
      Random::TL()->Uniform(10, 90) /* compress_percentage */);

    SetStargateGFlag(
      "estore_experimental_slice_replacement_inverse_prob", "0");
  }
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::TestRollbackEgroup() {
  LOG(INFO) << "Running " << __FUNCTION__;
  // Ensure that we have at least 5 disks available since we will create 3/2
  // strips for this test.
  CHECK_GE(disk_ids_.size(), 5);

  const vector<int64> info_disk_ids =
    { disk_ids_[0], disk_ids_[1], disk_ids_[2] };
  const vector<int64> info_egroup_ids =
    { GetNextEgroupId(), GetNextEgroupId(), GetNextEgroupId() };
  const int64 primary_parity_egroup_id = GetNextEgroupId();
  const int64 primary_parity_disk_id = disk_ids_[3];
  const int64 secondary_parity_egroup_id = GetNextEgroupId();
  const int64 secondary_parity_disk_id = disk_ids_[4];

  vector<vector<SliceState::Ptr>> info_slice_state_vecs;
  vector<SliceState::Ptr> primary_parity_slice_state_vec;
  vector<SliceState::Ptr> secondary_parity_slice_state_vec;

  vector<vector<shared_ptr<EgroupOpsTester::Extent>>> info_extent_state_vecs;

  // Write a strip with untransformed info extent group.
  LOG(INFO) << "Writing untrasformed erasure code strip "
            << JoinWithSeparator(info_egroup_ids, ",");

  WriteErasureCodedStripHelper(
    info_egroup_ids, info_disk_ids,
    primary_parity_egroup_id, primary_parity_disk_id,
    secondary_parity_egroup_id, secondary_parity_disk_id,
    &info_slice_state_vecs,
    &primary_parity_slice_state_vec,
    &secondary_parity_slice_state_vec,
    &info_extent_state_vecs,
    false /* compressible_data */,
    0 /* compress_percentage */);

  const vector<int64> compressed_info_egroup_ids =
    { GetNextEgroupId(), GetNextEgroupId(), GetNextEgroupId() };
  const int64 compressed_primary_parity_egroup_id = GetNextEgroupId();
  const int64 compressed_secondary_parity_egroup_id = GetNextEgroupId();

  vector<vector<SliceState::Ptr>> compressed_info_slice_state_vecs;
  vector<SliceState::Ptr> compressed_primary_parity_slice_state_vec;
  vector<SliceState::Ptr> compressed_secondary_parity_slice_state_vec;

  vector<vector<shared_ptr<EgroupOpsTester::Extent>>>
    compressed_extent_state_vecs;

  // Write a strip with compressed info extent group.
  LOG(INFO) << "Writing compressed erasure code strip "
            << JoinWithSeparator(compressed_info_egroup_ids, ",");

  WriteErasureCodedStripHelper(
    compressed_info_egroup_ids, info_disk_ids,
    compressed_primary_parity_egroup_id, primary_parity_disk_id,
    compressed_secondary_parity_egroup_id, secondary_parity_disk_id,
    &compressed_info_slice_state_vecs,
    &compressed_primary_parity_slice_state_vec,
    &compressed_secondary_parity_slice_state_vec,
    &compressed_extent_state_vecs,
    true /* compressible_data */,
    Random::TL()->Uniform(10, 90) /* compress_percentage */);

  // Perform overwrite/rollback in a loop a few times.
  for (uint ii = 0; ii < 10; ++ii) {
    // We will inject a corruption every even iteration in debug builds.
#ifndef NDEBUG
    const bool inject_corruption = ((ii % 2) == 0);
#else
    const bool inject_corruption = false;
#endif
    LOG(INFO) << "Overwrite and then rollback untrasformed erasure code strip "
              << JoinWithSeparator(info_egroup_ids, ",")
              << ". Inject corruption: " << boolalpha << inject_corruption;

    OverwriteRollbackErasureCodedStripHelper(
      info_egroup_ids, info_disk_ids,
      primary_parity_egroup_id, primary_parity_disk_id,
      secondary_parity_egroup_id, secondary_parity_disk_id,
      &info_slice_state_vecs,
      &primary_parity_slice_state_vec,
      &secondary_parity_slice_state_vec,
      &info_extent_state_vecs,
      false /* compressible_data */,
      0 /* compress_percentage */,
      inject_corruption,
      false /* test_rollback_after_crash */);

    LOG(INFO) << "Overwrite and then rollback compressed erasure code strip "
              << JoinWithSeparator(compressed_info_egroup_ids, ",")
              << ". Inject corruption: " << boolalpha << inject_corruption;

    OverwriteRollbackErasureCodedStripHelper(
      compressed_info_egroup_ids, info_disk_ids,
      compressed_primary_parity_egroup_id, primary_parity_disk_id,
      compressed_secondary_parity_egroup_id, secondary_parity_disk_id,
      &compressed_info_slice_state_vecs,
      &compressed_primary_parity_slice_state_vec,
      &compressed_secondary_parity_slice_state_vec,
      &compressed_extent_state_vecs,
      true /* compressible_data */,
      Random::TL()->Uniform(10, 90) /* compress_percentage */,
      inject_corruption,
      false /* test_rollback_after_crash */);

    LOG(INFO) << "Overwrite and then rollback compressed erasure code strip "
              << JoinWithSeparator(compressed_info_egroup_ids, ",")
              << " while forcing slice replacements. Inject corruption: "
              << boolalpha << inject_corruption;
    SetStargateGFlag(
      "estore_experimental_slice_replacement_inverse_prob", "1");

    OverwriteRollbackErasureCodedStripHelper(
      compressed_info_egroup_ids, info_disk_ids,
      compressed_primary_parity_egroup_id, primary_parity_disk_id,
      compressed_secondary_parity_egroup_id, secondary_parity_disk_id,
      &compressed_info_slice_state_vecs,
      &compressed_primary_parity_slice_state_vec,
      &compressed_secondary_parity_slice_state_vec,
      &compressed_extent_state_vecs,
      true /* compressible_data */,
      Random::TL()->Uniform(10, 90) /* compress_percentage */,
      inject_corruption,
      false /* test_rollback_after_crash */);

    SetStargateGFlag(
      "estore_experimental_slice_replacement_inverse_prob", "0");
  }
  LOG(INFO) << "Overwrite and then rollback untransformed erasure code strip "
            << JoinWithSeparator(info_egroup_ids, ",");

  OverwriteRollbackErasureCodedStripHelper(
    info_egroup_ids, info_disk_ids,
    primary_parity_egroup_id, primary_parity_disk_id,
    secondary_parity_egroup_id, secondary_parity_disk_id,
    &info_slice_state_vecs,
    &primary_parity_slice_state_vec,
    &secondary_parity_slice_state_vec,
    &info_extent_state_vecs,
    false /* compressible_data */,
    0 /* compress_percentage */,
    false /* inject_corruption */,
    true /* test_rollback_after_crash */);

  LOG(INFO) << "Overwrite and then rollback compressed erasure code strip "
            << JoinWithSeparator(compressed_info_egroup_ids, ",");

  OverwriteRollbackErasureCodedStripHelper(
    compressed_info_egroup_ids, info_disk_ids,
    compressed_primary_parity_egroup_id, primary_parity_disk_id,
    compressed_secondary_parity_egroup_id, secondary_parity_disk_id,
    &compressed_info_slice_state_vecs,
    &compressed_primary_parity_slice_state_vec,
    &compressed_secondary_parity_slice_state_vec,
    &compressed_extent_state_vecs,
    true /* compressible_data */,
    Random::TL()->Uniform(10, 90) /* compress_percentage */,
    false /* inject_corruption */,
    true /* test_rollback_after_crash */);

  LOG(INFO) << "Overwrite and then rollback compressed erasure code strip "
            << JoinWithSeparator(compressed_info_egroup_ids, ",")
            << " while forcing slice replacements ";
  SetStargateGFlag(
    "estore_experimental_slice_replacement_inverse_prob", "1");

  OverwriteRollbackErasureCodedStripHelper(
    compressed_info_egroup_ids, info_disk_ids,
    compressed_primary_parity_egroup_id, primary_parity_disk_id,
    compressed_secondary_parity_egroup_id, secondary_parity_disk_id,
    &compressed_info_slice_state_vecs,
    &compressed_primary_parity_slice_state_vec,
    &compressed_secondary_parity_slice_state_vec,
    &compressed_extent_state_vecs,
    true /* compressible_data */,
    Random::TL()->Uniform(10, 90) /* compress_percentage */,
    false /* inject_corruption */,
    true /* test_rollback_after_crash */);

  SetStargateGFlag(
    "estore_experimental_slice_replacement_inverse_prob", "0");
}

//-----------------------------------------------------------------------------

static void PopulateExtentId(
  const int64 egroup_id,
  const int64 disk_id,
  const MedusaExtentGroupPhysicalStateEntry *const entry,
  vector<medusa::ExtentIdProto> *const extent_id_vec) {

  CHECK(entry) << "disk_id: " << disk_id << " egroup_id: " << egroup_id;

  if (entry->ControlBlock()->slices_stored_by_id()) {
    extent_id_vec->reserve(entry->extent_map().size());
    for (const auto& it : entry->extent_map()) {
      CHECK(it.second) << it.first
                       << "disk_id: " << disk_id
                       << " egroup_id: " << egroup_id;
      extent_id_vec->emplace_back();
      extent_id_vec->back().CopyFrom(*it.first->extent_id_proto());
    }
    return;
  }

  CHECK(entry->extent_map().empty());
  extent_id_vec->reserve(entry->slice_group_map().size());
  for (const auto& it : entry->slice_group_map()) {
    CHECK(it.second) << it.first
                     << "disk_id: " << disk_id
                     << " egroup_id: " << egroup_id;
    CHECK(it.first.extent_id) << it.first;
    extent_id_vec->emplace_back();
    extent_id_vec->back().CopyFrom(*it.first.extent_id->extent_id_proto());
  }

  LOG(INFO) << "Populated " << extent_id_vec->size()
            << " extents from egroup " << egroup_id
            << " on disk " << disk_id;
}

static void PopulateMappedSlices(
  const int64 egroup_id,
  const int64 disk_id,
  const MedusaExtentGroupPhysicalStateEntry *const entry,
  vector<SliceState::Ptr> *const slice_state_vec) {

  CHECK(entry) << "disk_id: " << disk_id << " egroup_id: " << egroup_id;
  slice_state_vec->reserve(entry->ControlBlock()->next_slice_id());
  for (const auto& it : entry->slice_group_map()) {
    CHECK(it.second) << it.first
                     << "disk_id: " << disk_id
                     << " egroup_id: " << egroup_id;
     const SliceStateGroupFB *const ss_group_fb =
       GetSliceStateGroupFB(it.second->data());
     if (VectorLength(ss_group_fb->slice_states()) == 0) {
       LOG(INFO) << "No mapped slices in slice group: " << it.first;
       continue;
     }
     for (const auto& slice_state_fb : *ss_group_fb->slice_states()) {
       SliceState::Ptr ss = SliceState::Create(slice_state_fb);
       if (ss->has_extent_group_offset()) {
         LOG(INFO) << "Added slice: " << ss->DebugString();
         slice_state_vec->emplace_back(move(ss));
       } else {
         LOG(INFO) << "Skipping unmapped slice: " << ss->DebugString();
       }
     }
  }
}

void EgroupOpsTest::TestReplicaReadEgroup(const bool managed_by_aes) {
  LOG(INFO) << "Running " << __FUNCTION__
            << " managed_by_aes: " << managed_by_aes;
  const int64 egroup_id = GetNextEgroupId();
  const int num_vblocks_to_write = 3;

  egroup_id_intent_seq_map_[egroup_id] = 0;
  int64 latest_intent_sequence = 0;




  
  uint primary_idx =
    cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(disk_ids_[0]);
  uint secondary_idx =
    cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(disk_ids_[1]);

  int64 primary_svm_id=cluster_mgr_->stargate_mgr()->GetSvmIdForStargate(primary_idx);
  int64 secondary_svm_id=cluster_mgr_->stargate_mgr()->GetSvmIdForStargate(secondary_idx);
  
  
    
  LOG(INFO)<<"primary disk id :"<<disk_ids_[0]<<"  ,  "<<"secondary disk id :"<<disk_ids_[1]<<endl;
  LOG(INFO)<<"primary disk stargate id :"<<primary_idx<<"  ,  "<<"secondary disk stargate id :"<<secondary_idx<<endl;
  LOG(INFO)<<"primary disk stargate svm id :"<<primary_svm_id
    <<"  ,  "<<"secondary disk stargate svm id :"<<secondary_svm_id<<endl;


  // Write some extent groups, collect checksums.
  vector<SliceState::Ptr> slice_state_vec;
  WriteExtentGroupHelper(egroup_id,
                         disk_ids_[0],
                         disk_ids_[1],
                         num_vblocks_to_write,
                         &slice_state_vec,
                         managed_by_aes);

  MedusaValue::PtrConst mvalue;
  if (managed_by_aes) {
    latest_intent_sequence = num_vblocks_to_write - 1;
    // Issue a GetEgroupState RPC to fetch the latest extent group.
    interface::GetEgroupStateRet egroup_state;
    GetEgroupState(egroup_id, disk_ids_[0], latest_intent_sequence,
                   &egroup_state, true /* managed_by_aes */);
    CHECK_EQ(egroup_state.latest_applied_intent_sequence(),
             latest_intent_sequence);

    // Fetch the egroup physical metadata.
    CheckpointDiskWALs();

    mvalue = LookupExtentGroupPhysicalState(disk_ids_[0], egroup_id);
    CHECK_GE(mvalue->timestamp, 0)
      << "disk_id: " << disk_ids_[0] << " egroup_id: " << egroup_id;

    PopulateMappedSlices(egroup_id, disk_ids_[0],
                         mvalue->extent_group_physical_state_entry.get(),
                         &slice_state_vec);
  }



  if(FLAGS_make_the_slices_corrupt_){
    cluster_mgr_->stargate_mgr()->SetGFlag(                      
    "make_the_slices_corrupt", true);
    
    cluster_mgr_->stargate_mgr()->SetGFlag(                      
        "list_of_corrupt_slice_ids", "0,32",primary_svm_id);
    
    cluster_mgr_->stargate_mgr()->SetGFlag(                      
        "list_of_corrupt_slice_ids", "64",secondary_svm_id);

      cluster_mgr_->stargate_mgr()->SetGFlag(                      
    "allow_partial_unreadable_egroups", true);


    LOG(INFO)<<"setting unreadable_egroup"<<egroup_id;
    cluster_mgr_->stargate_mgr()->SetGFlag(                      
    "unreadable_egroup", egroup_id);

    cluster_mgr_->stargate_mgr()->SetGFlag(                      
    "disk_1", disk_ids_[0]);

    cluster_mgr_->stargate_mgr()->SetGFlag(                      
    "disk_2", disk_ids_[1]);


  }


  vector<int> primary_unreadable_slices;
  vector<int> sec_unreadable_slices;
  IOBuffer::Ptr primary_payload = make_shared<IOBuffer>();
  IOBuffer::Ptr sec_payload = make_shared<IOBuffer>();

  //Read extent group from primary replica and verify checksums.
   ReplicaReadEgroupAndVerify(egroup_id,
                             disk_ids_[0],
                             &slice_state_vec,
                             managed_by_aes,
                             -1,
                             StargateError::kNoError,
                             false,
                             &primary_unreadable_slices,
                             &primary_payload);



  // Read extent group from secondary replica and verify checksums.
  ReplicaReadEgroupAndVerify(egroup_id,
                             disk_ids_[1],
                             &slice_state_vec,
                             managed_by_aes,
                              -1,
                             StargateError::kNoError,
                             false,
                             &sec_unreadable_slices,
                             &sec_payload);



  // checking the final_payload
  const int slice_size = 32 * 1024;
  LOG(INFO)<<"@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@";
  LOG(INFO)<<"recieved final primary payload size _: "<<primary_payload->size();
  LOG(INFO)<<"num slices in final primary recieved payload _: "<<primary_payload->size()/slice_size;
  LOG(INFO)<<"num corrupted slices in final primary recieved payload _: "
  <<primary_unreadable_slices.size()<<endl<<endl;
  

  LOG(INFO)<<"recieved final sec payload size _: "<<sec_payload->size();
  LOG(INFO)<<"num slices in final sec recieved payload _: "<<sec_payload->size()/slice_size;
  LOG(INFO)<<"num corrupted slices in final sec recieved payload _: "<<sec_unreadable_slices.size();
  
  LOG(INFO)<<"@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@";


  //merging payloads

  IOBuffer::Ptr merged_payload = make_shared<IOBuffer>();
  vector<int> both_replica_unreadable;
  int p_ind=0;
  int s_ind=0;
  IOBuffer::Ptr iobuf =  make_shared<IOBuffer>();
  for (size_t ii = 0; ii < slice_state_vec.size(); ++ii) {
            bool is_primary_unreadable=(find(primary_unreadable_slices.begin(), primary_unreadable_slices.end(),
                                   slice_state_vec[ii]->slice_id()) != primary_unreadable_slices.end());
            bool is_sec_unreadable=(find(sec_unreadable_slices.begin(), sec_unreadable_slices.end(),
                                   slice_state_vec[ii]->slice_id()) != sec_unreadable_slices.end());

            if(!is_primary_unreadable){
                 iobuf = primary_payload->Clone(p_ind++ * slice_size, slice_size);
                if(!is_sec_unreadable) s_ind++;
            }
            else{
                if(!is_sec_unreadable) 
                    iobuf = sec_payload->Clone(s_ind++ * slice_size, slice_size);
                else{
                  LOG(INFO)<<"Both replicas have corrupt slice id "<<slice_state_vec[ii]->slice_id();
                  both_replica_unreadable.push_back(slice_state_vec[ii]->slice_id());
                  continue;
                }
            }
            merged_payload->AppendIOBuffer(iobuf.get());
    }
     
   LOG(INFO)<<"final merge_payload size _: "<<merged_payload->size();
   LOG(INFO)<<"num slices in final merge_payload _: "<<merged_payload->size()/slice_size;
   LOG(INFO)<<"num corrupted slices in both payload _: "<<both_replica_unreadable.size();
  

  if (managed_by_aes) {
    // Error injection test - read extent group from the replica for an
    // incorrect applied intent sequence and ensure the RPC returns the
    // kUnexpectedIntentSequence error.
    LOG(INFO) << "Running error injection read replica test for egroup "
              << egroup_id;
    ReplicaReadEgroupAndVerify(egroup_id,
                               disk_ids_[1],
                               &slice_state_vec,
                               true /* managed_by_aes */,
                               0 /* expected_applied_intent_sequence */,
                               StargateError::kUnexpectedIntentSequence);

    // Read extent group from the replica for the correct applied
    // intent sequence and verify the checksums.
    LOG(INFO) << "Running read replica test for egroup "
              << egroup_id << " at the correct intent sequence "
              << num_vblocks_to_write - 1;
    ReplicaReadEgroupAndVerify(egroup_id,
                               disk_ids_[1],
                               &slice_state_vec,
                               true /* managed_by_aes */,
                               num_vblocks_to_write - 1);
  }
}

//-----------------------------------------------------------------------------

static void CheckpointWALDone(const bool success, Notification *notification) {
  CHECK(success);
  notification->Notify();
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::CheckpointDiskWALs() {
  Notification checkpoint_notification(disk_ids_.size());
  Function<void(bool)> checkpoint_done_cb =
    bind(&CheckpointWALDone, _1, &checkpoint_notification);
  for (uint ii = 0; ii < disk_ids_.size(); ++ii) {
    CheckpointDiskWAL(disk_ids_[ii], checkpoint_done_cb);
  }
  checkpoint_notification.Wait();
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::TestAESMigrateErasureCode() {
  LOG(INFO) << "Running " << __FUNCTION__;

  CHECK_GE(disk_ids_.size(), 5);
  const vector<int64> info_disk_ids =
    { disk_ids_[0], disk_ids_[1], disk_ids_[2] };
  const vector<int64> info_egroup_ids =
    { GetNextEgroupId(), GetNextEgroupId(), GetNextEgroupId() };

  // Shutdown an SVM so we can later migrate an egroup to this SVM without any
  // additional logic.
  const int64 saved_disk_id = disk_ids_[disk_ids_.size() - 1];
  const int64 saved_svm_idx =
    cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(saved_disk_id);
  const int64 saved_svm_id =
    cluster_mgr_->stargate_mgr()->GetSvmIdForStargate(saved_svm_idx);

  LOG(INFO) << "Shutting down SVM at index: " << saved_svm_idx;
  cluster_mgr_->stargate_mgr()->StopStargateWithSvmId(saved_svm_id);

  // Wait until the node is down.
  while (true) {
    if (!cluster_mgr_->IsHealthyStargate(saved_svm_id)) {
      break;
    }
  }

  // Write a strip with untransformed info extent group.
  LOG(INFO) << "Writing untrasformed erasure code strip "
            << JoinWithSeparator(info_egroup_ids, ",");

  vector<vector<SliceState::Ptr>> info_slice_state_vecs;
  vector<SliceState::Ptr> primary_parity_slice_state_vec;
  vector<SliceState::Ptr> secondary_parity_slice_state_vec;
  vector<vector<shared_ptr<EgroupOpsTester::Extent>>> info_extent_state_vecs;
  vector<shared_ptr<ReplicaReadEgroupRet>> remote_ret_vec;
  remote_ret_vec.reserve(info_egroup_ids.size());

  // Invoke the strip helper function to create the info egroups.
  // Parities can be created later when the encoding is issued.
  WriteErasureCodedStripHelper(
    info_egroup_ids, info_disk_ids,
    -1 /* primary_parity_egroup_id */,
    -1 /* primary_parity_disk_id */,
    -1 /* secondary_parity_egroup_id */,
    -1 /* secondary_parity_disk_id */,
    &info_slice_state_vecs,
    &primary_parity_slice_state_vec,
    &secondary_parity_slice_state_vec,
    &info_extent_state_vecs,
    false /* compressible_data */,
    0 /* compress_percentage */,
    true /* managed_by_aes */,
    -1 /* primary_parity_inject_error_type */,
    -1 /* secondary_parity_inject_error_type */,
    &remote_ret_vec);

  DCHECK_EQ(remote_ret_vec.size(), info_egroup_ids.size());
  DCHECK_EQ(info_slice_state_vecs.size(), info_egroup_ids.size());
  DCHECK_EQ(info_extent_state_vecs.size(), info_egroup_ids.size());

  const int64 eg_id = info_egroup_ids.at(0);
  const int64 disk_id = info_disk_ids.at(0);
  const int64 latest_applied_intent_sequence =
    egroup_id_intent_seq_map_[eg_id];

  // Lookup the Map3 entry for the egroup to figure out the
  // global_metadata_intent_sequence.
  Medusa::MedusaValue::PtrConst mvalue =
    medusa_connector_->LookupExtentGroupIdMapEntry(eg_id);
  CHECK(mvalue);
  CHECK(mvalue->extent_groupid_map_entry);
  const MedusaExtentGroupIdMapEntry& egroup_metadata =
    *mvalue->extent_groupid_map_entry;
  const MedusaExtentGroupIdMapEntryProto::ControlBlock *const
    control_block = egroup_metadata.control_block();
  CHECK(control_block);
  const int64 global_metadata_intent_sequence =
    control_block->latest_intent_sequence();

  // Change the largest seen intent sequence of one of the info egroups to
  // simulate a condition where the latest_applied_intent_sequence and the
  // largest_seen_intent_sequence of the egroup do not match.
  LOG(INFO) << "Setting Egroup State";
  for (int ii = 0; ii < 5; ++ii) {
    if (ii != saved_svm_idx) {
      SetStargateGFlag(
        "stargate_experimental_allow_forceful_eg_state_update", "true", ii);
    }
  }

  shared_ptr<SetEgroupStateArg> set_egroup_state_arg =
    make_shared<SetEgroupStateArg>();
  set_egroup_state_arg->set_extent_group_id(eg_id);
  set_egroup_state_arg->set_disk_id(disk_id);
  set_egroup_state_arg->set_intent_sequence(global_metadata_intent_sequence);
  set_egroup_state_arg->set_expected_applied_intent_sequence(
    latest_applied_intent_sequence);
  set_egroup_state_arg->set_largest_seen_intent_sequence(
    latest_applied_intent_sequence + 1);
  set_egroup_state_arg->set_force_update(true);
  SetExtentGroupState(disk_id, set_egroup_state_arg);

  // Get the EgroupState to ensure that the applied and largest-seen intent
  // sequences are different.
  interface::GetEgroupStateRet egroup_state;
  GetEgroupState(eg_id, disk_id, global_metadata_intent_sequence,
                 &egroup_state, true);
  CHECK_GT(egroup_state.largest_seen_intent_sequence(),
           egroup_state.latest_applied_intent_sequence());

  // Now proceed to encoding the egroups. The function ensures the
  // egroups are erasure coded and the map3 entries of the info egroups
  // contain the finalizd ec metadata (i.e. parity egroup ids).
  vector<int64> parity_egroup_id_vec;
  vector<int64> parity_disk_id_vec;
  ErasureEncodeAESEgroups(info_egroup_ids, info_disk_ids,
                          &parity_egroup_id_vec,
                          &parity_disk_id_vec,
                          false,
                          1 /* parity_size */);

  CHECK_EQ(parity_egroup_id_vec.size(), 1);
  const int64 parity_egroup_id = parity_egroup_id_vec.at(0);
  const int64 parity_disk_id = parity_disk_id_vec.at(0);

  // Start the stargate we had previously shut down.
  cluster_mgr_->stargate_mgr()->StartStargateWithSvmId(saved_svm_id);
  while (true) {
    if (cluster_mgr_->IsHealthyStargate(saved_svm_id)) {
      break;
    }
  }

  // Migrate the parity egroup to the disk that is on the newly started SVM and
  // set flag to crash before we finalize the replication TU.
  SetStargateGFlag(
    "estore_experimental_crash_before_finalize_tu", "true");
  const int64 parity_svm_idx =
    cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(parity_disk_id);
  MigrateEgroups(vdisk_id_,
                 parity_svm_idx,
                 parity_egroup_id,
                 {saved_disk_id});

  // Verify that the parity egroup id has a TU with the parity_disk_id as the
  // source and saved_disk_id as the target.
  VerifyMigrateTentativeUpdate(
    parity_egroup_id, parity_disk_id, saved_disk_id);

  // Wait for the stargate to crash due to
  // --estore_experimental_crash_before_finalize_tu being set.
  while (true) {
    if (!cluster_mgr_->IsHealthyStargate(saved_svm_id)) {
      break;
    }
  }

  // Restart stargate.
  LOG(INFO) << "Starting up crashed stargate";
  cluster_mgr_->stargate_mgr()->RestartStargateWithSvmId(saved_svm_id);
  while (true) {
    if (cluster_mgr_->IsHealthyStargate(saved_svm_id)) {
      break;
    }
  }

  // Ensure that we were able to roll the replication forward in the extent
  // store.
  const string& log_msg =
    StringJoin("egroup_id=", parity_egroup_id,
               ".*Successfully able to roll forward the egroup to intent ",
               "sequence 0, tu_list_.size()=1");
  CHECK(HasStargateLog(log_msg, saved_svm_idx));
  CHECK(cluster_mgr_->IsHealthyStargate(saved_svm_id));
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::TestReplicateExtentGroup(const bool managed_by_aes) {
  LOG(INFO) << "Running " << __FUNCTION__;
  const int64 egroup_id = GetNextEgroupId();
  const int32 num_vblocks_to_write = 4;

  // Let's increase the egroup chunk size to 1MB to test data read done using
  // multiple ReplicaRead requests by replicate op.
  const int64 default_egroup_chunk_size = FLAGS_egroup_test_chunk_size;
  FLAGS_egroup_test_chunk_size = 1024 * 1024;

  egroup_id_intent_seq_map_[egroup_id] = 0;
  int64 latest_intent_sequence = 0;

  // Write some extent groups, collect checksums.
  vector<shared_ptr<EgroupOpsTester::SliceState>> slice_state_vec;
  WriteExtentGroupHelper(egroup_id,
                         disk_ids_[0],
                         disk_ids_[1],
                         num_vblocks_to_write,
                         &slice_state_vec,
                         managed_by_aes);

  interface::GetEgroupStateRet egroup_state;
  MedusaValue::PtrConst mvalue;
  vector<medusa::ExtentIdProto> extent_id_vec;
  if (managed_by_aes) {
    latest_intent_sequence = egroup_id_intent_seq_map_[egroup_id];
    // Issue a GetEgroupState RPC to fetch extents.
    GetEgroupState(egroup_id, disk_ids_[0], latest_intent_sequence,
                   &egroup_state, true /* managed_by_aes */);

    CheckpointDiskWALs();

    mvalue = LookupExtentGroupPhysicalState(disk_ids_[2], egroup_id);
    CHECK_LT(mvalue->timestamp, 0)
      << "disk_id: " << disk_ids_[2] << " egroup_id: " << egroup_id;

    mvalue = LookupExtentGroupPhysicalState(disk_ids_[0], egroup_id);
    CHECK_GE(mvalue->timestamp, 0)
      << "disk_id: " << disk_ids_[0] << " egroup_id: " << egroup_id;

    const ControlBlockFB *const control_block =
      mvalue->extent_group_physical_state_entry->ControlBlock();

    MedusaPrinterUtil::PrintExtentGroupPhysicalStateEntry(
        &LOG(INFO), mvalue->extent_group_physical_state_entry.get());

    PopulateExtentId(egroup_id, disk_ids_[0],
                     mvalue->extent_group_physical_state_entry.get(),
                     &extent_id_vec);
    PopulateMappedSlices(egroup_id, disk_ids_[0],
                         mvalue->extent_group_physical_state_entry.get(),
                         &slice_state_vec);

    CHECK_GT(extent_id_vec.size(), 0);
    CHECK_GT(slice_state_vec.size(), 0);
    LOG(INFO) << "Replicating egroup " << egroup_id << " with "
              << extent_id_vec.size() << " extents";

    // Replicate extent group to a new disk.
    ReplicateExtentGroup(egroup_id,
                         disk_ids_[0],
                         disk_ids_[2],
                         slice_state_vec,
                         latest_intent_sequence,
                         true /* managed_by_aes */,
                         false /* extent_based_format */,
                         container_id_,
                         &extent_id_vec);

    CheckpointDiskWALs();

    MedusaValue::PtrConst new_mvalue = LookupExtentGroupPhysicalState(
      disk_ids_[2], egroup_id);
    CHECK_GE(new_mvalue->timestamp, 0)
      << "disk_id: " << disk_ids_[2] << " egroup_id: " << egroup_id;
    const ControlBlockFB *const new_control_block =
      new_mvalue->extent_group_physical_state_entry->ControlBlock();
    VerifyPhysicalStateControlBlock(control_block, new_control_block);
  } else {
    latest_intent_sequence = egroup_id_intent_seq_map_[egroup_id] + 1;
    // Replicate extent group to a new disk.
    ReplicateExtentGroup(egroup_id,
                         disk_ids_[0],
                         disk_ids_[2],
                         slice_state_vec,
                         latest_intent_sequence,
                         false /* managed_by_aes */);

    // Fetch extent group state from the newly created replica. This should not
    // have any slice diffs and extent diffs because the ReplicateOp finalizes
    // and sets the synced_intent_sequence inline with the replication.
    GetEgroupState(egroup_id, disk_ids_[2], latest_intent_sequence,
                   &egroup_state, false /* managed_by_aes */);

    CHECK_EQ(egroup_state.slices_size(), 0);
    CHECK_EQ(egroup_state.extents_size(), 0);
  }

  // Read extent group from replica and verify checksums.
  ReplicaReadEgroupAndVerify(egroup_id,
                             disk_ids_[2],
                             &slice_state_vec,
                             managed_by_aes);

  // Replicate extent group to another disk and induce a failure during
  // replication. Verify that the migration was appropriately aborted.
  SetStargateGFlag("estore_experimental_inject_statfs_total_bytes",
                   "104857600");
  SetStargateGFlag("stargate_disk_manager_stat_interval_usecs", "500000");
  SetStargateGFlag("stargate_experimental_inject_statfs_usage_bytes",
                   "104857600");

  // Wait for the disk full to take effect.
  sleep(5);

  // Replicate extent group to a new disk since we have simulated a disk full,
  // this will fail with error kDiskSpaceUnavailable.
  ++latest_intent_sequence;
  ReplicateExtentGroup(egroup_id,
                       disk_ids_[0],
                       disk_ids_[3],
                       slice_state_vec,
                       latest_intent_sequence,
                       managed_by_aes,
                       false /* extent_based_format */,
                       container_id_,
                       &extent_id_vec,
                       StargateError::kDiskSpaceUnavailable);

  // Issue a GetEgroupState RPC to verify that the replica was not created.
  ++latest_intent_sequence;
  StargateError::Type error_ret;
  GetEgroupState(egroup_id, disk_ids_[3], latest_intent_sequence,
                 &egroup_state, managed_by_aes,
                 false /* extent_based_format */, &error_ret);
  CHECK_EQ(error_ret, StargateError::kExtentGroupNonExistent);

  SetStargateGFlag("stargate_experimental_inject_statfs_usage_bytes", "-1");
  SetStargateGFlag("estore_experimental_inject_statfs_total_bytes", "-1");
  sleep(5);
  // Restore the chunk size value.
  FLAGS_egroup_test_chunk_size = default_egroup_chunk_size;

  if (managed_by_aes) {
    // Checkpoint the disk WALs. This will ensure that we flush the metadata to
    // AES DB.
    CheckpointDiskWALs();
  }
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::TestDeleteExtentGroup(const bool managed_by_aes) {
  LOG(INFO) << "Running " << __FUNCTION__;
  const int64 egroup_id = GetNextEgroupId();
  const int num_vblocks_to_write = 2;

  egroup_id_intent_seq_map_[egroup_id] = 0;

  // Write some extent groups, collect checksums.
  vector<SliceState::Ptr> slice_state_vec;
  WriteExtentGroupHelper(egroup_id,
                         disk_ids_[0],
                         disk_ids_[1],
                         num_vblocks_to_write,
                         &slice_state_vec,
                         managed_by_aes);

  // Delete extent group from original disk.
  DeleteExtentGroup(egroup_id, disk_ids_[0], managed_by_aes);

  int64 latest_intent_sequence = 0;
  latest_intent_sequence = egroup_id_intent_seq_map_[egroup_id];
  interface::GetEgroupStateRet egroup_state;
  StargateError::Type error_ret;
  GetEgroupState(egroup_id, disk_ids_[0], latest_intent_sequence,
                 &egroup_state, managed_by_aes,
                 false /* extent_based_format */, &error_ret);
  CHECK_EQ(error_ret, StargateError::kExtentGroupNonExistent);

  if (managed_by_aes) {
    // Checkpoint the disk WALs. This will ensure that we flush the
    // metadata to AES DB.
    CheckpointDiskWALs();

    // There should be nothing in AES DB for this egroup on this replica.
    MedusaValue::PtrConst mvalue =
      LookupExtentGroupPhysicalState(disk_ids_[0], egroup_id);
    CHECK_LT(mvalue->timestamp, 0)
      << "disk_id: " << disk_ids_[0] << " egroup_id: " << egroup_id;
  }
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::TestDeleteExtentGroupOnAesAndNonAesEgroups() {
  LOG(INFO) << "Running " << __FUNCTION__;

  // Write some AES and non-AES extent groups.
  int64 egroup_id = 0;
  bool managed_by_aes = false;
  const int num_extent_groups = 10;
  const int num_vblocks_to_write = 2;
  vector<int64> extent_group_id_vec;
  vector<bool> managed_by_aes_vec;
  extent_group_id_vec.reserve(num_extent_groups);
  managed_by_aes_vec.reserve(num_extent_groups);
  vector<shared_ptr<EgroupOpsTester::SliceState>> slice_state_vec;
  for (int ii = 0; ii < num_extent_groups; ++ii) {
    egroup_id = GetNextEgroupId();
    egroup_id_intent_seq_map_[egroup_id] = 0;
    managed_by_aes = FLAGS_use_block_store ||
                     (Random::TL()->Uniform(num_extent_groups) % 2);
    LOG(INFO) << "Writing extent group " << egroup_id
              << " managed_by_aes " << boolalpha << managed_by_aes;
    WriteExtentGroupHelper(egroup_id,
                           disk_ids_[0],
                           disk_ids_[1],
                           num_vblocks_to_write,
                           &slice_state_vec,
                           managed_by_aes);
    extent_group_id_vec.emplace_back(egroup_id);
    managed_by_aes_vec.emplace_back(managed_by_aes);
  }

  // Add few non-existent extent group ids for deletion.
  for (int ii = 0; ii < num_extent_groups; ++ii) {
    egroup_id = GetNextEgroupId();
    egroup_id_intent_seq_map_[egroup_id] = 0;
    managed_by_aes = FLAGS_use_block_store ||
                     Random::TL()->Uniform(num_extent_groups) % 2;
    extent_group_id_vec.emplace_back(egroup_id);
    managed_by_aes_vec.emplace_back(managed_by_aes);
  }

  // Delete extent group from original disk.
  DeleteExtentGroups(extent_group_id_vec, disk_ids_[0], managed_by_aes_vec);

  int64 latest_intent_sequence = 0;
  for (int ii = 0; ii < 2 * num_extent_groups; ++ii) {
    egroup_id = extent_group_id_vec[ii];
    managed_by_aes = managed_by_aes_vec[ii];
    latest_intent_sequence = egroup_id_intent_seq_map_[egroup_id];
    interface::GetEgroupStateRet egroup_state;
    StargateError::Type error_ret;
    GetEgroupState(egroup_id, disk_ids_[0], latest_intent_sequence,
                   &egroup_state, managed_by_aes,
                   false /* extent_based_format */, &error_ret);
    CHECK_EQ(error_ret, StargateError::kExtentGroupNonExistent);
  }

  // Invoke the delete again on already deleted extent groups, the RPC
  // shouldn't fail.
  DeleteExtentGroups(extent_group_id_vec, disk_ids_[0], managed_by_aes_vec);
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::TestDeleteAndReplicateForAESExtentGroup() {
  LOG(INFO) << "Running " << __FUNCTION__;
  const int64 egroup_id = GetNextEgroupId();
  const int32 num_vblocks_to_write = 3;

  egroup_id_intent_seq_map_[egroup_id] = 0;
  int64 latest_intent_sequence = 0;

  // Write some extent groups, collect checksums.
  vector<shared_ptr<EgroupOpsTester::SliceState>> slice_state_vec;
  WriteExtentGroupHelper(egroup_id,
                         disk_ids_[0],
                         disk_ids_[1],
                         num_vblocks_to_write,
                         &slice_state_vec,
                         true /* managed_by_aes */);

  interface::GetEgroupStateRet egroup_state;
  vector<medusa::ExtentIdProto> extent_id_list;
  latest_intent_sequence = egroup_id_intent_seq_map_[egroup_id];
  GetEgroupState(egroup_id, disk_ids_[1], latest_intent_sequence,
                 &egroup_state, true /* managed_by_aes */);
  // Delete one replica.
  DeleteExtentGroup(egroup_id, disk_ids_[1], true /* managed_by_aes */);
  StargateError::Type error_ret;
  GetEgroupState(egroup_id, disk_ids_[1], latest_intent_sequence,
                 &egroup_state, true /* managed_by_aes */,
                 false /* extent_based_format */, &error_ret);
  CHECK_EQ(error_ret, StargateError::kExtentGroupNonExistent);

  // Let's replicate on the same disk from where we deleted the replica.
  slice_state_vec.clear();
  ReplicateExtentGroup(egroup_id,
                       disk_ids_[0],
                       disk_ids_[1],
                       slice_state_vec,
                       latest_intent_sequence,
                       true /* managed_by_aes */,
                       false /* extent_based_format */,
                       container_id_,
                       &extent_id_list);
  GetEgroupState(egroup_id, disk_ids_[1], latest_intent_sequence,
                 &egroup_state, true /* managed_by_aes */,
                 false /* extent_based_format */, nullptr /* error_ret */,
                 false /* set_latest_applied_intent_sequence */,
                 true /* fetch_all_metadata */);
  for (int ii = 0; ii < egroup_state.extent_group_metadata().slices_size();
       ++ii) {
    if (!egroup_state.extent_group_metadata().slices(ii).
           has_extent_group_offset()) {
      continue;
    }
    shared_ptr<EgroupOpsTester::SliceState> slice_state =
      EgroupOpsTester::SliceState::Create(
        egroup_state.extent_group_metadata().slices(ii));
    slice_state_vec.emplace_back(slice_state);
  }

  // Read extent group from replica and verify checksums.
  ReplicaReadEgroupAndVerify(egroup_id,
                             disk_ids_[1],
                             &slice_state_vec,
                             true /* managed_by_aes */);

  // Restart Stargates for both replicas. We should find eg_states to be same
  // after the restart.
  RestartStargateAtIndex(
    cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(disk_ids_[0]));
  RestartStargateAtIndex(
    cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(disk_ids_[1]));
  interface::GetEgroupStateRet new_egroup_state;
  GetEgroupState(egroup_id, disk_ids_[0], latest_intent_sequence,
                 &new_egroup_state, true /* managed_by_aes */);
  GetEgroupState(egroup_id, disk_ids_[1], latest_intent_sequence,
                 &new_egroup_state, true /* managed_by_aes */);
  CHECK_EQ(egroup_state.latest_applied_intent_sequence(),
           new_egroup_state.latest_applied_intent_sequence());

  // Verify extent group physical state.
  CheckpointDiskWALs();

  MedusaValue::PtrConst mvalue =
    LookupExtentGroupPhysicalState(disk_ids_[0], egroup_id);
  CHECK_GE(mvalue->timestamp, 0)
    << "disk_id: " << disk_ids_[0] << " egroup_id: " << egroup_id;
  const ControlBlockFB *const control_block =
    mvalue->extent_group_physical_state_entry->ControlBlock();

  MedusaValue::PtrConst new_mvalue =
    LookupExtentGroupPhysicalState(disk_ids_[1], egroup_id);
  CHECK_GE(new_mvalue->timestamp, 0)
    << "disk_id: " << disk_ids_[1] << " egroup_id: " << egroup_id;
  const ControlBlockFB *const new_control_block =
    new_mvalue->extent_group_physical_state_entry->ControlBlock();
  VerifyPhysicalStateControlBlock(
    control_block, new_control_block, true /* skip_fallocate_length */);
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::TestSyncEgroupSlices() {
  LOG(INFO) << "Running " << __FUNCTION__;
  const int64 egroup_id = GetNextEgroupId();
  const int num_vblocks_to_write = 3;

  egroup_id_intent_seq_map_[egroup_id] = 0;

  // Write some extent groups, collect checksums.
  vector<shared_ptr<EgroupOpsTester::SliceState>> slice_state_vec;
  WriteExtentGroupHelper(egroup_id,
                         disk_ids_[0],
                         disk_ids_[1],
                         num_vblocks_to_write,
                         &slice_state_vec);

  // Sync extent group slices and check to make sure that checksums match
  // what we wrote.
  SyncEgroupSlicesAndVerify(egroup_id,
                            disk_ids_[0],
                            slice_state_vec);
}

//-----------------------------------------------------------------------------

// Given 'slice_state_vec' for egroup 'egroup_id' calculates and returns its
// transformed size.
static int GetEgroupTransformedSize(
  const int64 egroup_id,
  const vector<shared_ptr<EgroupOpsTester::SliceState>>& slice_state_vec) {

  int transformed_size = 0;
  for (const shared_ptr<EgroupOpsTester::SliceState>& state :
        slice_state_vec) {
    CHECK(state) << egroup_id;
    if (!state->has_extent_group_offset()) {
      continue;
    }
    transformed_size = max(transformed_size, state->extent_group_offset() +
                           state->transformed_length() + state->cushion());
  }
  return transformed_size;
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::TestTruncateEgroups() {
  LOG(INFO) << "Running " << __FUNCTION__;
  SetStargateGFlag(
    "estore_experimental_background_scan_pause_processing", "false");

  shared_ptr<vector<int64>> egroup_ids = make_shared<vector<int64>>();
  shared_ptr<vector<int64>> expected_err = make_shared<vector<int64>>();
  shared_ptr<vector<int>> transformed_size_vec = make_shared<vector<int>>();

  // Write some extent groups.
  vector<shared_ptr<EgroupOpsTester::SliceState>> slice_state_vec;

  const int64 starting_egroup_id = GetNextEgroupId() + 1;
  int64 egroup_id = 0;
  for (int ii = 0; ii < 6; ++ii) {
    egroup_id = starting_egroup_id + ii;
    const int num_vblocks_to_write = 3;
    egroup_id_intent_seq_map_[egroup_id] = 0;
    WriteExtentGroupHelper(egroup_id,
                           disk_ids_[0],
                           disk_ids_[1],
                           num_vblocks_to_write,
                           &slice_state_vec,
                           false /* managed_by_aes */,
                           true /* notify */,
                           true /* wait_for_write */);
    egroup_ids->push_back(egroup_id);

    int transformed_size =
      GetEgroupTransformedSize(egroup_id, slice_state_vec);
    CHECK_GT(transformed_size, 0) << egroup_id;
    if (ii == 0) {
      const int old_transformed_size = transformed_size;
      ++transformed_size;
      expected_err->push_back(StargateError::kRetry);
      LOG(INFO) << "Modified egroup " << egroup_id << " transformed_size from "
                << old_transformed_size << " to " << transformed_size
                << " to test negative case";
    } else {
      expected_err->push_back(StargateError::kNoError);
    }
    transformed_size_vec->emplace_back(transformed_size);
    LOG(INFO) << "Egroup " << egroup_id << " transformed_size: "
              << transformed_size;
  }
  // Also push an egroup that will not exist.
  egroup_ids->push_back(egroup_ids->back() + 1);
  expected_err->push_back(StargateError::kExtentGroupNonExistent);
  transformed_size_vec->emplace_back(0);

  // Choose a good disk_id to send the truncate RPC to.
  TruncateEgroups(disk_ids_[0], egroup_ids, expected_err, transformed_size_vec,
                  false /* inject_error */);

  // Verify that the egroups got truncated.
  LOG(INFO) << "Verifying fallocate length after truncate RPC";
  for (int xx = 0; xx < 2; ++xx) {
    unordered_map<int64, int32> egroup_falloc_len_map;
    FetchEgroupFallocateLength(disk_ids_[xx], &egroup_falloc_len_map);
    CHECK(!egroup_falloc_len_map.empty());

    for (uint ii = 0; ii < egroup_ids->size(); ++ii) {
      if (expected_err->at(ii) != StargateError::kNoError) {
        continue;
      }
      const int64 egroup_id = egroup_ids->at(ii);
      auto it = egroup_falloc_len_map.find(egroup_id);
      CHECK(it != egroup_falloc_len_map.end())
        << "disk: " << disk_ids_[ii] << " egroup: " << egroup_id;
      LOG(INFO) << "Egroup " << egroup_id << " falloc_len: " << it->second;
      if (xx == 0) {
        CHECK_EQ(it->second, transformed_size_vec->at(ii)) << egroup_id;
      } else {
        // Truncate was only sent to the first disk, so the egroup should not
        // have been truncated on the 2nd disk.
        CHECK_GT(it->second, transformed_size_vec->at(ii)) << egroup_id;
        CHECK_EQ(it->second, 1048576L);
      }
    }
  }

  // If we request the same egroups to be truncated again, all the good ones
  // should return kRetry and the bad ones still return
  // kExtentGroupNonExsistent.
  for (uint ii = 0; ii < expected_err->size(); ++ii) {
    if (expected_err->at(ii) == StargateError::kNoError) {
      (*expected_err)[ii] = StargateError::kInvalidValue;
    }
  }

  // Send the RPC with same params.
  TruncateEgroups(disk_ids_[0], egroup_ids, expected_err, transformed_size_vec,
                  false /* inject_error */);

  // Inject some error. The RPC should fail.
  TruncateEgroups(disk_ids_[0], egroup_ids, expected_err, transformed_size_vec,
                  true /* inject_error */);

  egroup_id_ = egroup_ids->back() + 1;

  egroup_ids->clear();
  transformed_size_vec->clear();
  expected_err->clear();

  for (int ii = 0; ii < 5; ++ii) {
    egroup_id = egroup_id_ + ii;
    const int num_vblocks_to_write = 3;
    egroup_id_intent_seq_map_[egroup_id] = 0;
    WriteExtentGroupHelper(egroup_id,
                           disk_ids_[0],
                           disk_ids_[1],
                           num_vblocks_to_write,
                           &slice_state_vec);
    egroup_ids->push_back(egroup_id);

    int transformed_size = 0;
    for (const shared_ptr<EgroupOpsTester::SliceState>& state :
          slice_state_vec) {
      CHECK(state) << egroup_id;
      if (!state->has_extent_group_offset()) {
        continue;
      }
      transformed_size = max(transformed_size, state->extent_group_offset() +
                             state->transformed_length() + state->cushion());
    }

    CHECK_GT(transformed_size, 0) << egroup_id;
    transformed_size_vec->emplace_back(transformed_size);
    LOG(INFO) << "Egroup " << egroup_id << " transformed_size: "
              << transformed_size;
  }

  // Set the egroup id for the next test.
  egroup_id_ = egroup_ids->back() + 1;

  for (int xx = 0; xx < 2; ++xx) {
    LOG(INFO) << "Disk usage truncate check iteration: " << xx;
    vector<string> last_disk_usage_completed_ts_vec;
    last_disk_usage_completed_ts_vec.resize(2, "unknown");

    for (int ii = 0; ii < 2; ++ii) {
      const string ts =
        FetchLastDiskUsageCompleted(disk_ids_[ii]);
      if (ts.empty()) {
        continue;
      }

      last_disk_usage_completed_ts_vec[ii] = ts;
    }

    int completed[2] = {0, 0};
    while (completed[0] < 2 || completed[1] < 2) {
      LOG(INFO) << "Waiting for disk usage cycle to complete";

      for (int ii = 0; ii < 2; ++ii) {
        const string ts =
          FetchLastDiskUsageCompleted(disk_ids_[ii]);
        if (ts.empty()) {
          continue;
        }

        LOG(INFO) << "Disk: " << disk_ids_[ii] << " previous: "
                  << last_disk_usage_completed_ts_vec[ii] << " current: " << ts
                  << " completed: " << completed[ii];

        if (last_disk_usage_completed_ts_vec[ii] != ts) {
          ++completed[ii];
          last_disk_usage_completed_ts_vec[ii] = ts;
        }
      }

      sleep(5);
    }
    LOG(INFO) << "Disk usage cycle completed";

    for (int ii = 0; ii < 2; ++ii) {
      unordered_map<int64, int32> egroup_falloc_len_map;
      FetchEgroupFallocateLength(disk_ids_[ii], &egroup_falloc_len_map);
      CHECK(!egroup_falloc_len_map.empty());

      for (const int64 egroup_id : *egroup_ids) {
        auto it = egroup_falloc_len_map.find(egroup_id);
        CHECK(it != egroup_falloc_len_map.end())
          << "disk: " << disk_ids_[ii] << " egroup: " << egroup_id;
        LOG(INFO) << "Egroup " << egroup_id << " falloc_len: " << it->second;

        if (xx == 0) {
          CHECK_GT(it->second, transformed_size_vec->at(ii)) << egroup_id;
        } else {
          CHECK_EQ(it->second, transformed_size_vec->at(ii)) << egroup_id;
        }
      }
    }

    if (xx == 0) {
      // Reduce idle threshold to 1 second and wait for one more disk usage
      // cycle.
      for (int ii = 0; ii < cluster_mgr_->cdp_options().nodes; ++ii) {
        cluster_mgr_->SetFlag(
          "127.0.0.1", cluster_mgr_->stargate_mgr()->stargate_port(ii),
          "disk_manager_truncate_garbage_idle_threshold_secs", "1");
      }
    }
  }
  SetStargateGFlag(
    "estore_experimental_background_scan_pause_processing", "true");
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::SetDiskQueueDropOpMode(bool enable) {
  for (int ii = 0; ii < cluster_mgr_->cdp_options().nodes; ++ii) {
    cluster_mgr_->SetFlag("127.0.0.1",
                          cluster_mgr_->stargate_mgr()->stargate_port(ii),
                          "estore_experimental_disk_queue_drop_first_op",
                          enable ? "true" : "false");
  }
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::SetSSDDiskQueueAdmissionThreshold(int threshold) {
  for (int ii = 0; ii < cluster_mgr_->cdp_options().nodes; ++ii) {
    // The gflag has been deprecated.
    // We still change this incase we ever fallback to the older code.
    cluster_mgr_->SetFlag("127.0.0.1",
                          cluster_mgr_->stargate_mgr()->stargate_port(ii),
                          "estore_ssd_disk_queue_admission_threshold",
                          to_string(threshold));
  }
}

//-----------------------------------------------------------------------------

string EgroupOpsTest::FetchLastDiskUsageCompleted(const int64 disk_id) {
  const string handle =
    CHECK_NOTNULL(cluster_mgr_->stargate_mgr()->GetStargateInterfaceForDisk(
                    disk_id).get())->peer_handle();

  IpAddress ip_address;
  uint16 port;
  IpUtil::ParseHandle(handle, &ip_address, &port);

  const string disk_usage_completed_key =
    StringJoin("stargate/disk/", disk_id,
               "/last_egroup_disk_usage_computation_completed");
  shared_ptr<VariableMapExporterProto> proto =
    cluster_mgr_->FetchVarmapProto(ip_address.ToString(),
                                   port, disk_usage_completed_key);
  CHECK(proto) << ip_address.ToString() << " " << port;

  if (proto->variables_size() == 0) {
    LOG(ERROR) << "Last completed timestamp for disk " << disk_id
               << " unavailable " << " in stargate "
               << ip_address.ToString() << ":" << port;
    return string();
  }

  CHECK_EQ(proto->variables(0).name(), disk_usage_completed_key);
  CHECK_EQ(proto->variables(0).value().type(),
           VariableMapExporterProto::kStringType);
  return proto->variables(0).value().string_value();
}

void EgroupOpsTest::TestExtentStoreOverwrites(int chunk_size) {
  LOG(INFO) << "Running " << __FUNCTION__
            << " with chunk_size " << chunk_size;

  // Write an extent group.
  vector<shared_ptr<EgroupOpsTester::SliceState>> slice_state_vec;
  const int64 egroup_id = GetNextEgroupId();
  const int num_vblocks_to_write = 3;
  egroup_id_intent_seq_map_[egroup_id] = 0;
  LOG(INFO) << "Writing egroup_id " << egroup_id
            << " intent sequence " << egroup_id_intent_seq_map_[egroup_id];
  WriteExtentGroupHelper(egroup_id,
                         disk_ids_[0],
                         disk_ids_[1],
                         num_vblocks_to_write,
                         &slice_state_vec);

  // Sync extent group slices and check to make sure that checksums match what
  // we wrote.
  SyncEgroupSlicesAndVerify(egroup_id,
                            disk_ids_[0],
                            slice_state_vec);

  // Overwrite the extent group, but with a smaller chunk size.
  const int32 egroup_test_chunk_size_saved = FLAGS_egroup_test_chunk_size;
  FLAGS_egroup_test_chunk_size = chunk_size;
  ++egroup_id_intent_seq_map_[egroup_id];
  LOG(INFO) << "Over-writing egroup_id " << egroup_id
            << " intent sequence " << egroup_id_intent_seq_map_[egroup_id];
  WriteExtentGroupHelper(egroup_id,
                         disk_ids_[0],
                         disk_ids_[1],
                         1 /* num_vblocks_to_write */,
                         &slice_state_vec);
  FLAGS_egroup_test_chunk_size = egroup_test_chunk_size_saved;
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::TestExtentStoreDropOps() {
  LOG(INFO) << "Running " << __FUNCTION__;
  SetStargateGFlag("estore_experimental_background_scan_pause_processing",
                   "true");
  SetDiskQueueDropOpMode(true);
  // Set a smaller admission threshold to so that ops are more likely
  // to get queued.
  SetSSDDiskQueueAdmissionThreshold(10);

  // Introduce a delay in each op to further create a queuing delay that will
  // eventually cause the ops to be dropped.
  for (int ii = 0; ii < cluster_mgr_->cdp_options().nodes; ++ii) {
    cluster_mgr_->SetFlag(
      "127.0.0.1",
      cluster_mgr_->stargate_mgr()->stargate_port(ii),
      "estore_experimental_delay_after_register_writer_op_msecs",
      "2000");
  }

  const int64 egroup_count = 500;

  const int64 starting_egroup_id = GetNextEgroupId();
  const int num_vblocks_to_write = 4;

  vector<vector<shared_ptr<EgroupOpsTester::SliceState>>> slice_state_vec_vec;
  vector<vector<shared_ptr<EgroupOpsTester::Extent>>> extent_state_vec_vec;
  vector<vector<Notification::Ptr>> notifications_vec;
  map<StargateError::Type, int> expected_errors;
  expected_errors[StargateError::kNoError] = 0;
  expected_errors[StargateError::kUnexpectedIntentSequence] = 0;
  expected_errors[StargateError::kRetry] = 0;
  expected_errors[StargateError::kTimeout] = 0;
  expected_errors[StargateError::kSecondaryWriteFailure] = 0;
  // Include the sample chunk offset and length to write on the extent group.
  vector<pair<int64, int64>> region_to_write;
  region_to_write.emplace_back(make_pair(0, FLAGS_egroup_test_chunk_size));

  notifications_vec.resize(egroup_count);
  slice_state_vec_vec.resize(egroup_count);
  extent_state_vec_vec.resize(egroup_count);
  LOG(INFO) << "Writing " << egroup_count << " extent groups";
  int64 egroup_id = 0;
  for (int xx = 0; xx < egroup_count; ++xx) {
    egroup_id = starting_egroup_id + xx;
    egroup_id_intent_seq_map_[egroup_id] = 0;
    IOBuffer::Ptr data_to_write = make_shared<IOBuffer>();
    // Write some extent groups, collect checksums.
    WriteExtentGroup(egroup_id,
                     disk_ids_[0],
                     disk_ids_[1],
                     num_vblocks_to_write,
                     &notifications_vec[xx],
                     region_to_write,
                     &slice_state_vec_vec[xx],
                     &extent_state_vec_vec[xx],
                     data_to_write,
                     false /* compressible_data */,
                     0 /* compress_percentage */,
                     false /* managed_by_aes */,
                     &expected_errors);
    CHECK_EQ(notifications_vec[xx].size(), num_vblocks_to_write);
  }
  LOG(INFO) << "Waiting for notifications";
  for (int xx = 0; xx < egroup_count; ++xx) {
    for (int yy = 0; yy < num_vblocks_to_write; ++yy) {
      // Wait for response from extent store.
      notifications_vec[xx][yy]->Wait();
    }
  }

  LOG(INFO) << "StargateError::kNoError:"
            << expected_errors[StargateError::kNoError];
  LOG(INFO) << "StargateError::kUnexpectedIntentSequence:"
            << expected_errors[StargateError::kUnexpectedIntentSequence];
  LOG(INFO) << "StargateError::kRetry:"
            << expected_errors[StargateError::kRetry];
  LOG(INFO) << "StargateError::kTimeout:"
            << expected_errors[StargateError::kTimeout];
  LOG(INFO) << "StargateError::kSecondaryWriteFailure:"
            << expected_errors[StargateError::kSecondaryWriteFailure];
  int total = 0;
  for (const auto& it : expected_errors) {
    total += it.second;
  }
  CHECK_EQ(total, egroup_count * num_vblocks_to_write);
  // We should see some expected errors due to disk manager dropping ops.
  CHECK_GT(expected_errors[StargateError::kUnexpectedIntentSequence], 0);

  for (int ii = 0; ii < cluster_mgr_->cdp_options().nodes; ++ii) {
    cluster_mgr_->SetFlag(
      "127.0.0.1",
      cluster_mgr_->stargate_mgr()->stargate_port(ii),
      "estore_experimental_delay_after_register_writer_op_msecs",
      "0");
  }

  egroup_id_ = egroup_id;
  SetDiskQueueDropOpMode(false);
  SetSSDDiskQueueAdmissionThreshold(50);
  SetStargateGFlag("estore_experimental_background_scan_pause_processing",
                   "false");
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::TestSliceSubregionRead() {
  LOG(INFO) << "Running " << __FUNCTION__;

  // Before testing the slice subregion read, we write an entire slice.
  const int64 egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = 0;

  // Invoke helper function for performing the test.
  SingleSliceTestHelper(egroup_id,
                        disk_ids_[0],
                        false /* test_write */);
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::TestSliceSubregionWrite() {
  LOG(INFO) << "Running " << __FUNCTION__;

  // Ensure that this is the first sequence in this egroup.
  int64 egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = 0;

  // Invoke the helper function.
  SingleSliceTestHelper(egroup_id,
                        disk_ids_[0],
                        true /* test_write */);

  // Let us also test by modifying slice checksum region size to some value
  // lower than AIO block size. This should not cause any crashes and
  // complete successfully. We also verify the data written by reading it. In
  // order to set the flag, we have to restart stargate so disk manager is
  // initialized once again since thats where we initialize the slice
  // subregion size.
  RestartStargate("-estore_checksum_region_size=2048 ");
  PopulateDiskIds();
  CHECK(!disk_ids_.empty());

  LOG(INFO) << "Testing EgroupWriteOp after setting checksum region size to "
            << "a value less than AIOBlockSize";
  egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = 0;

  SingleSliceTestHelper(egroup_id,
                        disk_ids_[0],
                        true /* test_write */);
  // This is to test an optimization in write, where certain subregions are
  // not read from the disk before being written. This happens when we write an
  // entire subregion.
  TestReadBytes(disk_ids_[0]);
  LOG(INFO) << "EgroupWriteOp successful";

  // Once we finish testing, let us change it to 8K so the rest of the tests
  // work with the default value.
  RestartStargate("-estore_checksum_region_size=8192 ");
  PopulateDiskIds();
  CHECK(!disk_ids_.empty());
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::TestMultiSliceWrite() {
  LOG(INFO) << "Running " << __FUNCTION__;

  const int64 egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = 0;
  int64 vblock_num = 6010;
  // ------------------------------------------------
  // Subregion aligned writes each of subregion size.
  // ------------------------------------------------
  MultiSliceTestHelper(egroup_id,
                       vblock_num,
                       disk_ids_[0],
                       disk_ids_[1],
                       false /* misalign_write */,
                       false /* partial_subregion_write */);

  ++egroup_id_intent_seq_map_[egroup_id];
  // Overwrite the above vblock.
  MultiSliceTestHelper(egroup_id,
                       vblock_num,
                       disk_ids_[0],
                       disk_ids_[1],
                       false /* misalign_write */,
                       false /* partial_subregion_write */);

  // ----------------------------------------------------
  // Subregion aligned writes each of subregion size / 2.
  // ----------------------------------------------------
  ++egroup_id_intent_seq_map_[egroup_id];
  ++vblock_num;
  MultiSliceTestHelper(egroup_id,
                       vblock_num,
                       disk_ids_[0],
                       disk_ids_[1],
                       false /* misalign_write */,
                       true /* partial_subregion_write */);

  ++egroup_id_intent_seq_map_[egroup_id];
  // Overwrite the above vblock.
  MultiSliceTestHelper(egroup_id,
                       vblock_num,
                       disk_ids_[0],
                       disk_ids_[1],
                       false /* misalign_write */,
                       true /* partial_subregion_write */);

  // -----------------------------------------------------
  // Subregion unaligned writes each of subregion size / 2.
  // -----------------------------------------------------
  ++egroup_id_intent_seq_map_[egroup_id];
  ++vblock_num;
  MultiSliceTestHelper(egroup_id,
                       vblock_num,
                       disk_ids_[0],
                       disk_ids_[1],
                       true /* misalign_write */,
                       true /* partial_subregion_write */);

  ++egroup_id_intent_seq_map_[egroup_id];
  // Overwrite the above vblock.
  MultiSliceTestHelper(egroup_id,
                       vblock_num,
                       disk_ids_[0],
                       disk_ids_[1],
                       true /* misalign_write */,
                       true /* partial_subregion_write */);

  // -----------------------------------------------------
  // Subregion unaligned writes each of subregion size.
  // -----------------------------------------------------
  ++egroup_id_intent_seq_map_[egroup_id];
  ++vblock_num;
  MultiSliceTestHelper(egroup_id,
                       vblock_num,
                       disk_ids_[0],
                       disk_ids_[1],
                       true /* misalign_write */,
                       false /* partial_subregion_write */);

  ++egroup_id_intent_seq_map_[egroup_id];
  // Overwrite the above vblock.
  MultiSliceTestHelper(egroup_id,
                       vblock_num,
                       disk_ids_[0],
                       disk_ids_[1],
                       true  /* misalign_write */,
                       false /* partial_subregion_write */);

  //------------------------------------------------------
  // Compressible data.
  // -----------------------------------------------------
  ++egroup_id_intent_seq_map_[egroup_id];
  ++vblock_num;
  MultiSliceTestHelper(egroup_id,
                       vblock_num,
                       disk_ids_[0],
                       disk_ids_[1],
                       false /* misalign_write */,
                       false /* partial_subregion_write */,
                       true  /* compressible_data */);

  ++egroup_id_intent_seq_map_[egroup_id];
  // Overwrite the above vblock.
  MultiSliceTestHelper(egroup_id,
                       vblock_num,
                       disk_ids_[0],
                       disk_ids_[1],
                       false /* misalign_write */,
                       false /* partial_subregion_write */,
                       true  /* compressible_data */);
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::TestCompressedSlice() {
  LOG(INFO) << "Running " << __FUNCTION__;

  // Ensure that this is the first sequence in this egroup.
  int64 egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = 0;

  CompressedSliceTestHelper(egroup_id, disk_ids_[0]);

  // Ensure that this is the first sequence in this egroup.
  egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = 0;

  set<StargateError::Type> expected_errors = { StargateError::kDataCorrupt };
  CompressedSliceCorruptTestHelper(egroup_id, disk_ids_[0], expected_errors);
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::TestCompressedSliceReplacement() {
  LOG(INFO) << "Running " << __FUNCTION__;

  // Ensure that this is the first sequence in this egroup.
  const int64 egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = 0;

  CompressedSliceReplacementTestHelper(egroup_id, disk_ids_[0]);
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::TestMixedCompressionEgroup() {
  // For egroups managed by AES, we test if the
  // bytes_to_write_before_retrying_compression field is set to the appropriate
  // value. We do not test this for EC ops because the test performs a restart
  // in-between and EC info egroups that are created by the caller(as a part of
  // a different test) do not like that and fail during recovery.
  if (FLAGS_egroup_test_ec_ops) {
    return;
  }

  LOG(INFO) << "Running " << __FUNCTION__;

  // Sanity test for mixed compression extent group with full slice write.
  int64 egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = 0;
  MixedCompressionEgroupSanityTest(egroup_id, disk_ids_[0], disk_ids_[1],
                                   true /* full slice */);

  // Sanity test for mixed compression extent group with partial slice
  // write.
  egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = 0;
  MixedCompressionEgroupSanityTest(egroup_id, disk_ids_[0], disk_ids_[1],
                                   false /* full slice */);

  // Test the mixed compression extent group with slices that have various
  // compression percentage 0 (non compressible) to 99 (highly compressible).
  const int32 num_slices_in_extent = 16;
  const int32 num_batch = 100 / num_slices_in_extent;
  for (int ii = 0; ii < num_batch; ++ii) {
    const int32 start_compress_percentage = ii * num_slices_in_extent;
    const int32 end_compress_percentage =
      min(start_compress_percentage + num_slices_in_extent - 1, 99);
    egroup_id = GetNextEgroupId();
    egroup_id_intent_seq_map_[egroup_id] = 0;
    LOG(INFO) << "Testing Compressibility with " << num_slices_in_extent
              << " slices, batch: " << ii;
    MixedCompressionEgroupCompressibilityTest(
      egroup_id, disk_ids_[0], disk_ids_[1], start_compress_percentage,
      end_compress_percentage, ii + 1);
  }

  TestEgroupSyncSlicesForMixedCompressionEgroup();
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::TestEgroupSyncSlicesForMixedCompressionEgroup() {
  // Ignore AES egroups for now, we have already verified the write operation
  // with AES egroups in the caller.
  if (FLAGS_egroup_test_estore_aes_enabled) {
    return;
  }

  // Test egroup sync slices. Verify the checksums returned by
  // SyncEgroupSlices.
  vector<shared_ptr<EgroupOpsTester::SliceState>> slice_state_vec;
  int64 egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = 0;
  MixedCompressionEgroupWriteHelper(
    egroup_id, disk_ids_[0], disk_ids_[1], 3 /* num_vblock */,
    true /* full_slice */, &slice_state_vec);
  vector<DataTransformation::Type> transformation_type_vec(
    1, DataTransformation::kCompressionSnappy);
  SyncEgroupSlicesAndVerify(egroup_id,
                            disk_ids_[0],
                            slice_state_vec,
                            "" /* transformation_type */,
                            transformation_type_vec);

  egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = 0;
  MixedCompressionEgroupWriteHelper(
    egroup_id, disk_ids_[0], disk_ids_[1], 3 /* num_vblock */,
    false /* full_slice */, &slice_state_vec);
  SyncEgroupSlicesAndVerify(egroup_id,
                            disk_ids_[0],
                            slice_state_vec,
                            "" /* transformation_type */,
                            transformation_type_vec);
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::TestAbortOnRecovery(const bool first_update_tentative) {
  LOG(INFO) << "Running " << __FUNCTION__ << " first_update_tentative: "
            << boolalpha << first_update_tentative;

  const int64 egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = 0;

  const int64 vblock_num = 5000;
  vector<shared_ptr<SliceState>> applied_slice_state_vec;
  vector<shared_ptr<SliceState>> slice_state_vec;
  vector<shared_ptr<EgroupOpsTester::Extent>> extent_state_vec;

  IOBuffer::Ptr data;

  vector<RegionDescriptor> region_to_write_vec;

  // Test setup - allocate the initial slices of the extent.
  LOG(INFO) << "Allocating slices for vblock " << vblock_num;
  const int kNumAllocatedSlices = 18;
  const int region_size = kNumAllocatedSlices * 32768;

  region_to_write_vec.emplace_back(0, region_size, 0);
  data = Random::TL()->CreateRandomDataIOBuffer(region_size);

  vector<Notification::Ptr> notification_vec;
  notification_vec.emplace_back(make_shared<Notification>());
  WriteSingleExtentInExtentGroup(egroup_id,
                                 disk_ids_[0],
                                 disk_ids_[1],
                                 vblock_num,
                                 string() /* transformation_type */,
                                 vector<DataTransformation::Type>(),
                                 region_to_write_vec,
                                 notification_vec.back().get(),
                                 &applied_slice_state_vec,
                                 &extent_state_vec,
                                 data);

  CHECK_EQ(applied_slice_state_vec.size(), kNumAllocatedSlices);
  for (const auto& slice_state : applied_slice_state_vec) {
    CHECK(slice_state->has_extent_group_offset())
      << slice_state->ShortDebugString();
  }
  CHECK_EQ(extent_state_vec.size(), 1);
  const shared_ptr<EgroupOpsTester::Extent>& extent = extent_state_vec[0];
  CHECK_EQ(extent->diff_data_location().slice_ids_size(), 32);
  CHECK_EQ(extent->diff_data_location().slice_indices_size(), 32);

  // Issue a second write to one of the above slice.
  LOG(INFO) << "Writing to slice index 10";

  region_to_write_vec.clear();
  region_to_write_vec.emplace_back(327680, 32768, 0);
  data = Random::TL()->CreateRandomDataIOBuffer(32768LL);

  ++egroup_id_intent_seq_map_[egroup_id];
  notification_vec.emplace_back(make_shared<Notification>());
  WriteSingleExtentInExtentGroup(egroup_id,
                                 disk_ids_[0],
                                 disk_ids_[1],
                                 vblock_num,
                                 string() /* transformation_type */,
                                 vector<DataTransformation::Type>(),
                                 region_to_write_vec,
                                 notification_vec.back().get(),
                                 &slice_state_vec,
                                 &extent_state_vec,
                                 data);

  // Issue a disk write and wait after the tentative update is issued. Then
  // wait until the tentative update is added to the checkpoint and restart
  // stargate. Validate that the tentative resolution on recovery is able to
  // successfully rollback the transaction.
  SetStargateGFlag("estore_experimental_pause_write_after_getting_fd",
                   "true");
  // We expect the write to fail due to the stargate restart.
  map<StargateError::Type, int> expected_errors;
  expected_errors[StargateError::kTransportError] = 0;

  // Issue another write to a new vblock.
  LOG(INFO) << "Writing to vblock " << vblock_num + 1;

  region_to_write_vec.clear();
  region_to_write_vec.emplace_back(0, 32768, 0);
  data = Random::TL()->CreateRandomDataIOBuffer(32768LL);

  // If first_update_tentative is true, the set the
  // latest_applied_intent_sequence in this write to 1. This will make the
  // extent store sync writes upto intent sequence 1 from the ephemeral state
  // and we will be left with a single tentative update corresponding to this
  // new write. Otherwise, if first_update_tentative is false, we'll set
  // latest_applied_intent_sequence to 0 so that the extent store retains the
  // finalized write #1, and our new write goes after that in the update queue.
  // We discard write #0 always as we don't want the estore to be aware of all
  // the slices to validate correct recovery on restart.
  const int64 latest_applied_intent_sequence =
    (first_update_tentative ? 1 : 0);

  ++egroup_id_intent_seq_map_[egroup_id];
  notification_vec.emplace_back(make_shared<Notification>());
  WriteSingleExtentInExtentGroup(egroup_id,
                                 disk_ids_[0],
                                 disk_ids_[1],
                                 vblock_num + 1,
                                 string() /* transformation_type */,
                                 vector<DataTransformation::Type>(),
                                 region_to_write_vec,
                                 notification_vec.back().get(),
                                 &slice_state_vec,
                                 &extent_state_vec,
                                 data,
                                 &expected_errors,
                                 false /* wait_for_write */,
                                 latest_applied_intent_sequence,
                                 false /* is_sequential */);

  // Wait for the above writes TU to get logged.
  sleep(5);

  // Checkpoint the disk WALs.
  Notification checkpoint_notification(2);
  Function<void(bool)> done_cb =
    bind(&CheckpointWALDone, _1, &checkpoint_notification);
  CheckpointDiskWAL(disk_ids_[0], done_cb);
  CheckpointDiskWAL(disk_ids_[1], done_cb);
  checkpoint_notification.Wait();

  // Restart stargate.
  RestartStargateAtIndex(
    cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(disk_ids_[0]));
  RestartStargateAtIndex(
    cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(disk_ids_[1]));

  // Wait for write RPC.
  for (const Notification::Ptr& notification : notification_vec) {
    notification->Wait();
  }

  CHECK_EQ(expected_errors[StargateError::kTransportError], 1);

  SetStargateGFlag("estore_experimental_pause_write_after_getting_fd",
                   "false");
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::TestAbortOnRecoveryAllUpdatesRollback() {
  LOG(INFO) << "Running " << __FUNCTION__;

  const int64 egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = 0;

  const int64 vblock_num = 5000;
  vector<shared_ptr<SliceState>> applied_slice_state_vec;
  vector<shared_ptr<SliceState>> slice_state_vec;
  vector<shared_ptr<EgroupOpsTester::Extent>> extent_state_vec;

  IOBuffer::Ptr data;

  vector<RegionDescriptor> region_to_write_vec;

  // Issue a disk write and wait after the tentative update is issued. Then
  // wait until the tentative update is added to the checkpoint and restart
  // stargate. Validate that the tentative resolution on recovery is able to
  // successfully rollback the transaction.
  SetStargateGFlag("estore_experimental_pause_write_after_getting_fd",
                   "true");
  // We expect the write to fail due to the stargate restart.
  map<StargateError::Type, int> expected_errors;
  expected_errors[StargateError::kTransportError] = 0;

  LOG(INFO) << "Writing to slice index 0";

  region_to_write_vec.clear();
  region_to_write_vec.emplace_back(0, 32768, 0);
  data = Random::TL()->CreateRandomDataIOBuffer(32768LL);

  vector<Notification::Ptr> notification_vec;
  notification_vec.emplace_back(make_shared<Notification>());
  WriteSingleExtentInExtentGroup(egroup_id,
                                 disk_ids_[0],
                                 disk_ids_[1],
                                 vblock_num,
                                 string() /* transformation_type */,
                                 vector<DataTransformation::Type>(),
                                 region_to_write_vec,
                                 notification_vec.back().get(),
                                 &slice_state_vec,
                                 &extent_state_vec,
                                 data,
                                 &expected_errors,
                                 false /* wait_for_write */);

  region_to_write_vec.clear();
  region_to_write_vec.emplace_back(32768, 32768, 0);
  data = Random::TL()->CreateRandomDataIOBuffer(32768LL);

  ++egroup_id_intent_seq_map_[egroup_id];
  notification_vec.emplace_back(make_shared<Notification>());
  WriteSingleExtentInExtentGroup(egroup_id,
                                 disk_ids_[0],
                                 disk_ids_[1],
                                 vblock_num + 1,
                                 string() /* transformation_type */,
                                 vector<DataTransformation::Type>(),
                                 region_to_write_vec,
                                 notification_vec.back().get(),
                                 &slice_state_vec,
                                 &extent_state_vec,
                                 data,
                                 &expected_errors,
                                 false /* wait_for_write */);

  // Wait for the above write TUs to get logged.
  sleep(5);

  // Checkpoint the disk WALs.
  Notification checkpoint_notification(2);
  Function<void(bool)> done_cb =
    bind(&CheckpointWALDone, _1, &checkpoint_notification);
  CheckpointDiskWAL(disk_ids_[0], done_cb);
  CheckpointDiskWAL(disk_ids_[1], done_cb);
  checkpoint_notification.Wait();

  // Restart stargate.
  RestartStargateAtIndex(
    cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(disk_ids_[0]));
  RestartStargateAtIndex(
    cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(disk_ids_[1]));

  // Wait for write RPC.
  for (const Notification::Ptr& notification : notification_vec) {
    notification->Wait();
  }

  CHECK_EQ(expected_errors[StargateError::kTransportError], 2);

  SetStargateGFlag("estore_experimental_pause_write_after_getting_fd",
                   "false");
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::TestAbortOnRecoveryWithCustomUpalignment() {
  LOG(INFO) << "Running " << __FUNCTION__;

  const int64 egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = 0;

  const int64 vblock1 = 6000;
  const int64 vblock2 = 6001;
  vector<shared_ptr<SliceState>> applied_slice_state_vec;
  vector<shared_ptr<SliceState>> slice_state_vec;
  vector<shared_ptr<EgroupOpsTester::Extent>> extent_state_vec;

  IOBuffer::Ptr data;

  vector<RegionDescriptor> region_to_write_vec;

  // Test setup - allocate all slices.
  LOG(INFO) << "Allocating slices for vblock " << vblock1;

  region_to_write_vec.emplace_back(0, 1048576, 0);
  data = Random::TL()->CreateRandomDataIOBuffer(1048576LL);

  // Issue the write with is_sequential set to true. This will result in a 4MB
  // fallocation instead of the default.
  vector<Notification::Ptr> notification_vec;
  notification_vec.emplace_back(make_shared<Notification>());
  WriteSingleExtentInExtentGroup(egroup_id,
                                 disk_ids_[0],
                                 disk_ids_[1],
                                 vblock1,
                                 string() /* transformation_type */,
                                 vector<DataTransformation::Type>(),
                                 region_to_write_vec,
                                 notification_vec.back().get(),
                                 &applied_slice_state_vec,
                                 &extent_state_vec,
                                 data,
                                 nullptr /* expected_error_map */,
                                 true /* wait_for_write */,
                                 -1 /* latest_applied_intent_sequence */,
                                 true /* is_sequential */);

  CHECK_EQ(applied_slice_state_vec.size(), 32);
  for (const auto& slice_state : applied_slice_state_vec) {
    CHECK(slice_state->has_extent_group_offset())
      << slice_state->ShortDebugString();
  }
  CHECK_EQ(extent_state_vec.size(), 1);
  const shared_ptr<EgroupOpsTester::Extent>& extent = extent_state_vec[0];
  CHECK_EQ(extent->diff_data_location().slice_ids_size(), 32);
  CHECK_EQ(extent->diff_data_location().slice_indices_size(), 32);

  // Issue a disk write and wait after the tentative update is issued. Then
  // wait until the tentative update is added to the checkpoint and restart
  // stargate. Validate that the tentative resolution on recovery is able to
  // successfully rollback the transaction.
  SetStargateGFlag("estore_experimental_pause_write_after_getting_fd",
                   "true");
  // We expect the write to fail due to the stargate restart.
  map<StargateError::Type, int> expected_errors;
  expected_errors[StargateError::kTransportError] = 0;

  // Checkpoint the disk WALs. Pause checkpoint on start until 2nd write is
  // outstanding.
  SetStargateGFlag("estore_experimental_pause_checkpoint_at_start",
                   StringJoin(disk_ids_[0]));

  Notification checkpoint_notification(2);
  Function<void(bool)> done_cb =
    bind(&CheckpointWALDone, _1, &checkpoint_notification);
  CheckpointDiskWAL(disk_ids_[0], done_cb);
  CheckpointDiskWAL(disk_ids_[1], done_cb);

  // Issue a write on the 2nd vblock.
  LOG(INFO) << "Writing to vblock " << vblock2;

  region_to_write_vec.clear();
  region_to_write_vec.emplace_back(0, 32768, 0);
  data = Random::TL()->CreateRandomDataIOBuffer(32768LL);

  ++egroup_id_intent_seq_map_[egroup_id];
  notification_vec.emplace_back(make_shared<Notification>());
  WriteSingleExtentInExtentGroup(egroup_id,
                                 disk_ids_[0],
                                 disk_ids_[1],
                                 vblock2,
                                 string() /* transformation_type */,
                                 vector<DataTransformation::Type>(),
                                 region_to_write_vec,
                                 notification_vec.back().get(),
                                 &slice_state_vec,
                                 &extent_state_vec,
                                 data,
                                 &expected_errors,
                                 false /* wait_for_write */);

  // Unblock the checkpoint.
  sleep(5);
  SetStargateGFlag("estore_experimental_pause_checkpoint_at_start", "-1");
  checkpoint_notification.Wait();

  // Restart stargate.
  RestartStargateAtIndex(
    cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(disk_ids_[0]));
  RestartStargateAtIndex(
    cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(disk_ids_[1]));

  // Wait for write RPC.
  for (const Notification::Ptr& notification : notification_vec) {
    notification->Wait();
  }

  CHECK_EQ(expected_errors[StargateError::kTransportError], 1);

  SetStargateGFlag("estore_experimental_pause_write_after_getting_fd",
                   "false");
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::TestAbortedWrite() {
  LOG(INFO) << "Running " << __FUNCTION__;

  // Using vector instead of a single notification expecting 4 events since the
  // underlying tester methods reset the passed notification.
  vector<Notification::Ptr> notification_vec;

  const int64 egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = 0;

  const int64 vblock_num = 1000;
  vector<SliceState::Ptr> applied_slice_state_vec;
  vector<SliceState::Ptr> slice_state_vec;
  vector<shared_ptr<EgroupOpsTester::Extent>> extent_state_vec;

  IOBuffer::Ptr data;

  vector<RegionDescriptor> region_to_write_vec;

  // Test setup - allocate all slices.
  LOG(INFO) << "Allocating slices for vblock " << vblock_num;

  region_to_write_vec.emplace_back(0, 1048576, 0);
  data = Random::TL()->CreateRandomDataIOBuffer(1048576LL);

  notification_vec.emplace_back(make_shared<Notification>());
  WriteSingleExtentInExtentGroup(egroup_id,
                                 disk_ids_[0],
                                 disk_ids_[1],
                                 vblock_num,
                                 string() /* transformation_type */,
                                 vector<DataTransformation::Type>(),
                                 region_to_write_vec,
                                 notification_vec.back().get(),
                                 &applied_slice_state_vec,
                                 &extent_state_vec,
                                 data);

  CHECK_EQ(applied_slice_state_vec.size(), 32);
  for (const auto& slice_state : applied_slice_state_vec) {
    CHECK(slice_state->has_extent_group_offset())
      << slice_state->ShortDebugString();
  }
  CHECK_EQ(extent_state_vec.size(), 1);
  const shared_ptr<EgroupOpsTester::Extent>& extent = extent_state_vec[0];
  CHECK_EQ(extent->diff_data_location().slice_ids_size(), 32);
  CHECK_EQ(extent->diff_data_location().slice_indices_size(), 32);

  // Setup gflags to force slice replacement and fail subsequent fallocations.
  // This will simulates a disk full condition where the next attempt to extend
  // the extent group will fail.
  SetStargateGFlag("estore_experimental_slice_replacement_inverse_prob", "1");
  SetStargateGFlag("estore_experimental_qos_queue_admit_delay_msecs", "2000");
  SetStargateGFlag("estore_experimental_inject_statfs_total_bytes",
                   "104857600");
  SetStargateGFlag("stargate_disk_manager_stat_interval_usecs", "500000");
  SetStargateGFlag("stargate_experimental_inject_statfs_usage_bytes",
                   "104857600");

  // Wait for the overfull condition to take effect.
  sleep(10);

  // We expect the writes to fail with kDiskSpaceUnavailable due to the
  // simulated fallocate failure and the remaining with
  // kUnexpectedIntentSequence as a result of the aborts.
  map<StargateError::Type, int> expected_errors;
  expected_errors[StargateError::kDiskSpaceUnavailable] = 0;
  expected_errors[StargateError::kUnexpectedIntentSequence] = 0;

  // Issue a write to the first slice.
  LOG(INFO) << "Writing to slice index 0";

  region_to_write_vec.clear();
  region_to_write_vec.emplace_back(0, 32768, 0);
  data = Random::TL()->CreateRandomDataIOBuffer(32768LL);

  ++egroup_id_intent_seq_map_[egroup_id];
  notification_vec.emplace_back(make_shared<Notification>());
  WriteSingleExtentInExtentGroup(egroup_id,
                                 disk_ids_[0],
                                 disk_ids_[1],
                                 vblock_num,
                                 string() /* transformation_type */,
                                 vector<DataTransformation::Type>(),
                                 region_to_write_vec,
                                 notification_vec.back().get(),
                                 &slice_state_vec,
                                 &extent_state_vec,
                                 data,
                                 &expected_errors,
                                 false /* wait_for_write */);

  // Issue a write to the second slice.
  LOG(INFO) << "Writing to slice index 1";

  region_to_write_vec.clear();
  region_to_write_vec.emplace_back(32768, 32768, 0);
  data = Random::TL()->CreateRandomDataIOBuffer(32768LL);

  ++egroup_id_intent_seq_map_[egroup_id];
  notification_vec.emplace_back(make_shared<Notification>());
  WriteSingleExtentInExtentGroup(egroup_id,
                                 disk_ids_[0],
                                 disk_ids_[1],
                                 vblock_num,
                                 string() /* transformation_type */,
                                 vector<DataTransformation::Type>(),
                                 region_to_write_vec,
                                 notification_vec.back().get(),
                                 &slice_state_vec,
                                 &extent_state_vec,
                                 data,
                                 &expected_errors,
                                 false /* wait_for_write */);

  // Issue a partial write to the first slice.
  LOG(INFO) << "Writing partially to slice index 0";

  region_to_write_vec.clear();
  region_to_write_vec.emplace_back(0, 4096, 0);
  data = Random::TL()->CreateRandomDataIOBuffer(4096LL);

  ++egroup_id_intent_seq_map_[egroup_id];
  notification_vec.emplace_back(make_shared<Notification>());
  WriteSingleExtentInExtentGroup(egroup_id,
                                 disk_ids_[0],
                                 disk_ids_[1],
                                 vblock_num,
                                 string() /* transformation_type */,
                                 vector<DataTransformation::Type>(),
                                 region_to_write_vec,
                                 notification_vec.back().get(),
                                 &slice_state_vec,
                                 &extent_state_vec,
                                 data,
                                 &expected_errors,
                                 false /* wait_for_write */);
  for (const Notification::Ptr& notification : notification_vec) {
    notification->Wait();
  }

  LOG(INFO) << "All writes done";

  CHECK_EQ(expected_errors[StargateError::kDiskSpaceUnavailable], 1);
  CHECK_EQ(expected_errors[StargateError::kUnexpectedIntentSequence], 2);

  // Reset gflags.
  SetStargateGFlag("estore_experimental_slice_replacement_inverse_prob", "0");
  SetStargateGFlag("estore_experimental_qos_queue_admit_delay_msecs", "0");
  SetStargateGFlag("stargate_experimental_inject_statfs_usage_bytes", "-1");
  SetStargateGFlag("estore_experimental_inject_statfs_total_bytes", "-1");
  SetStargateGFlag("stargate_disk_manager_stat_interval_usecs", "15000000");

  // Wait for the flags reset to take effect
  sleep(30);

  // Read extent group on both replicas and verify. Since the remaining writes
  // were aborted, we should still be at intent sequence 0.
  egroup_id_intent_seq_map_[egroup_id] = 0;
  for (int ii = 0; ii < 2; ++ii) {
    LOG(INFO) << "Verifying data from replica: " << disk_ids_[ii];
    ReplicaReadEgroupAndVerify(
      egroup_id, disk_ids_[ii], &applied_slice_state_vec);
  }
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::TestEGStateWithAbortedWriteHelper(
  const int32 num_updates,
  const int32 update_to_abort,
  const int32 new_slices_pct,
  const int32 partial_overwrite_pct,
  const bool truncate_egroup,
  const bool fail_truncate,
  const bool replicate_egroup,
  const bool restart_stargate,
  const bool test_same_slice_overwrite) {

  const int64 egroup_id = GetNextEgroupId();
  const int64 vblock_num = Random::TL()->Uniform(1, 1000);
  vector<SliceState::Ptr> slice_state_vec;
  vector<SliceState::Ptr> applied_slice_state_vec;
  vector<shared_ptr<EgroupOpsTester::Extent>> extent_state_vec;
  egroup_id_intent_seq_map_[egroup_id] = 0;
  {
    // Initialize the region we want to work with.
    vector<RegionDescriptor> region_to_write_vec;
    region_to_write_vec.emplace_back(0, 1048576, 0);
    IOBuffer::Ptr data = Random::TL()->CreateRandomDataIOBuffer(1048576ll);
    Notification notification;
    WriteSingleExtentInExtentGroup(egroup_id,
                                   disk_ids_[0],
                                   disk_ids_[1],
                                   vblock_num,
                                   string() /* transformation_type */,
                                   vector<DataTransformation::Type>(),
                                   region_to_write_vec,
                                   &notification,
                                   &applied_slice_state_vec,
                                   &extent_state_vec,
                                   data,
                                   nullptr /* expected_errors */,
                                   true /* wait for write */);
  }

  // starts N writes on different slices and aborts a write on a particular
  // update. The updates can be specified to fully/partially overwrite
  // existing slices or create newer slices.
  constexpr int32 max_slices = 32;
  constexpr int32 max_slice_size = 32 * 1024;
  CHECK_GT(num_updates, 1);
  CHECK_LT(num_updates, max_slices);
  CHECK_LE(new_slices_pct, 100);
  CHECK_GE(new_slices_pct, 0);
  CHECK_LE(partial_overwrite_pct, 100);
  CHECK_GE(partial_overwrite_pct, 0);
  // Partial overwrite with slice replacement does not work. No use case
  // requires it so fixing it is not a priority.
  // There seem to be a bug in code where data not being overwritten is not
  // copied resulting in checksum mismatches in the new slice. Since the slice
  // replacement will only occur for compressed slices where the entire slice
  // will be over-written, this issue will not occur for any practical
  // use case.
  // The scenario can currently only occur through test code which forces a
  // slice replacement with partial overwrite of the slice.
  CHECK(!(partial_overwrite_pct > 0 && new_slices_pct > 0));

  // A vector of pair of region descriptor and a bool indicating whether a
  // slice replacement is needed or not.
  vector<pair<RegionDescriptor, bool>> region_slice_vec;
  region_slice_vec.reserve(num_updates);
  const int32 new_slices_threshold = new_slices_pct * num_updates / 100;
  const int32 partial_overwrite_threshold =
    partial_overwrite_pct * num_updates / 100;
  for (int ii = 0; ii < num_updates; ++ii) {
    int32 rand = ii + 1;
    if (test_same_slice_overwrite && update_to_abort >= 0 &&
        ii > update_to_abort) {
      rand = ii - update_to_abort + 1;
    }
    const int32 slice_size = rand < partial_overwrite_threshold ?
                             Random::TL()->Uniform(1, max_slice_size / 2 - 1) :
                             max_slice_size;
    CHECK_GT(rand, 0);
    const int32 offset = ((rand - 1) * max_slice_size) +
                         (slice_size >= max_slice_size / 2 ? 0 :
                            (max_slice_size / 2) - slice_size);

    region_slice_vec.emplace_back(
      piecewise_construct,
      forward_as_tuple(offset, slice_size, 0),
      forward_as_tuple(rand < new_slices_threshold));
  }

  // Delay finishing the writes, till num_updates writes have been queued.
  SetStargateGFlag("estore_experimental_pause_write_after_getting_fd", "true");
  vector<Notification::Ptr> notification_vec;
  // We expect the writes to fail with kDiskSpaceUnavailable due to the
  // simulated fallocate failure and the remaining with
  // kUnexpectedIntentSequence as a result of the aborts.
  map<StargateError::Type, int> expected_errors;
  expected_errors[StargateError::kDiskSpaceUnavailable] = 0;
  expected_errors[StargateError::kUnexpectedIntentSequence] = 0;
  expected_errors[StargateError::kNoError] = 0;
  expected_errors[StargateError::kTransportError] = 0;
  // Tracks whether a new slice was created.
  bool new_slice_created = false;
  for (int ii = 0; ii < num_updates; ++ii) {
    LOG(INFO) << "Writing region offset = "
              << region_slice_vec[ii].first.offset << " size = "
              << region_slice_vec[ii].first.length << " new slice = "
              << region_slice_vec[ii].second << " ii = " << ii
              << " update to abort = " << update_to_abort;
    const bool create_new_slice =
      (ii == update_to_abort || region_slice_vec[ii].second);
    SetStargateGFlag("estore_experimental_slice_replacement_inverse_prob",
                     create_new_slice ? "1" : "0");
    if (create_new_slice && !restart_stargate &&
        (update_to_abort < 0 ? true : ii < update_to_abort)) {
      new_slice_created = true;
    }

    vector<RegionDescriptor> region_to_write_vec;
    region_to_write_vec.emplace_back(region_slice_vec[ii].first);
    IOBuffer::Ptr data = Random::TL()->CreateRandomDataIOBuffer(
                           region_slice_vec[ii].first.length);
    notification_vec.emplace_back(make_shared<Notification>());
    ++egroup_id_intent_seq_map_[egroup_id];
    WriteSingleExtentInExtentGroup(
      egroup_id,
      disk_ids_[0],
      disk_ids_[1],
      vblock_num,
      string() /* transformation_type */,
      vector<DataTransformation::Type>(),
      region_to_write_vec,
      notification_vec.back().get(),
      (!restart_stargate && ((update_to_abort < 0 && ii == num_updates - 1) ||
         ii == update_to_abort - 1)) ?
         &applied_slice_state_vec : &slice_state_vec,
      &extent_state_vec,
      data,
      &expected_errors,
      false /* wait for write */);
  }
  // Fail the write on the expected intent sequence and allow writes to
  // continue.
  if (update_to_abort >= 0) {
    SetStargateGFlag("estore_experimental_abort_intent_sequence",
                     to_string(update_to_abort + 1));
  }

  if (restart_stargate) {
    RestartStargateAtIndex(
      cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(disk_ids_[0]));
  }

  SetStargateGFlag("estore_experimental_pause_write_after_getting_fd",
                   "false");
  for (const Notification::Ptr& notification : notification_vec) {
    notification->Wait();
  }

  LOG(INFO) << "All writes done";

  // restore the flags.
  SetStargateGFlag("estore_experimental_slice_replacement_inverse_prob", "0");
  SetStargateGFlag("estore_experimental_abort_intent_sequence", "-1");

  // Verify expected errors.
  if (restart_stargate) {
    CHECK_EQ(expected_errors[StargateError::kTransportError], num_updates);
    egroup_id_intent_seq_map_[egroup_id] = 0;
  } else if (update_to_abort > 0 && update_to_abort < num_updates) {
    CHECK_EQ(expected_errors[StargateError::kNoError], update_to_abort);
    CHECK_EQ(expected_errors[StargateError::kDiskSpaceUnavailable], 1);
    CHECK_EQ(expected_errors[StargateError::kUnexpectedIntentSequence],
             max(num_updates - 1 - update_to_abort, 0));
    egroup_id_intent_seq_map_[egroup_id] = update_to_abort;
  } else {
    CHECK_EQ(expected_errors[StargateError::kNoError], num_updates);
    CHECK_EQ(expected_errors[StargateError::kDiskSpaceUnavailable], 0);
    CHECK_EQ(expected_errors[StargateError::kUnexpectedIntentSequence], 0);
    egroup_id_intent_seq_map_[egroup_id] = num_updates;
  }

  if (truncate_egroup) {
    int transformed_size_bytes = -1;
    for (int ii = 0; ii <= 1; ++ii) {
      LOG(INFO) << "Truncating egroup " << egroup_id << " on disk "
                << disk_ids_[ii];
      shared_ptr<vector<int64>> egroup_id_vec =
        make_shared<vector<int64>>();
      egroup_id_vec->emplace_back(egroup_id);
      shared_ptr<vector<int64>> expected_error_vec =
        make_shared<vector<int64>>();
      if (restart_stargate && ii == 1) {
        // The write on secondary replica will finish successfully. So truncate
        // with intent sequence 0 will return kRetry.
        expected_error_vec->emplace_back(StargateError::kRetry);
      } else {
        if (transformed_size_bytes < 0) {
          transformed_size_bytes =
            GetEgroupTransformedSize(egroup_id, applied_slice_state_vec);
          CHECK_GT(transformed_size_bytes, 0) << egroup_id;
          LOG(INFO) << "Egroup " << egroup_id << " transformed_size: "
                    << transformed_size_bytes;
        }
        expected_error_vec->emplace_back(
          new_slice_created ? StargateError::kNoError :
          StargateError::kInvalidValue);
        TruncateEgroups(disk_ids_[ii], egroup_id_vec, expected_error_vec,
                        make_shared<vector<int>>(1, transformed_size_bytes),
                        fail_truncate /* inject errors */);
      }
    }
  }

  if (replicate_egroup) {
    LOG(INFO) << "Replicating egroup from disk 0 to 2";
    ReplicateExtentGroup(egroup_id, disk_ids_[0], disk_ids_[2],
                         applied_slice_state_vec);
  }

  DCHECK(!applied_slice_state_vec.empty());
  for (int32 ii = (replicate_egroup ? 1 : 0);
       ii < (replicate_egroup ? 3 : 2); ++ii) {
    LOG(INFO) << "Verifying data from replica: disk_ids_[" << ii << "] "
              << disk_ids_[ii];
    ReplicaReadEgroupAndVerify(
      egroup_id, disk_ids_[0], &applied_slice_state_vec);
  }
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::TestEGStateWithAbortedWrite(
  ) {
  LOG(INFO) << "Running " << __FUNCTION__;

  LOG(INFO) << "Stack TU 1,2,3...N, abort N+1 and validate that state is "
            << "rolled forward upto N with no new fallocations";
  TestEGStateWithAbortedWriteHelper(8, 7, 0, 0);

  LOG(INFO) << "Stack TU 1,2,3...N, abort N+1 and validate that state is "
            << "rolled forward upto N with some new fallocations";
  TestEGStateWithAbortedWriteHelper(8, 7, 50, 0);

  LOG(INFO) << "Stack TU 1,2,3...N, abort 3 and validate that state is "
            << "rolled forward upto 2 with no new fallocations";
  TestEGStateWithAbortedWriteHelper(8, 2, 0, 0);

  LOG(INFO) << "Stack TU 1,2,3...N, abort 3 and validate that state is "
            << "rolled forward upto 2 with some new fallocations";
  TestEGStateWithAbortedWriteHelper(8, 2, 50, 0);

  LOG(INFO) << "Stack TU 1,2,3...N, followed by truncate and abort 3 and "
            << "validate that state is rolled forward upto 2 with no new "
            << "fallocations";
  TestEGStateWithAbortedWriteHelper(8, 2, 0, 0, true /* truncate egroup */);

  LOG(INFO) << "Stack TU 1,2,3...N, followed by truncate and abort 3 and "
            << "validate that state is rolled forward upto 2 with some new"
            << "fallocations";
  TestEGStateWithAbortedWriteHelper(8, 2, 50, 0, true /* truncate egroup */);

  LOG(INFO) << "Stack TU 1,2,3...N, followed by truncate and validate all "
            << "write complete successfully with no new fallocations";
  TestEGStateWithAbortedWriteHelper(8, -1, 0, 50, true /* truncate egroup */);

  LOG(INFO) << "Stack TU 1,2,3...N, followed by truncate and validate all "
            << "write complete successfully with some new fallocations";
  TestEGStateWithAbortedWriteHelper(8, -1, 50, 0, true /* truncate egroup */);

  LOG(INFO) << "Stack TU 1,2,3...N, followed by failed truncate and validate "
            << "all write complete successfully with no new fallocations";
  TestEGStateWithAbortedWriteHelper(8, -1, 50, 0, true /* truncate egroup */,
                                    true /* fail truncate */);

  LOG(INFO) << "Stack TU 1,2,3...N, followed by failed truncate and validate "
            << "all write complete successfully with some new fallocations";
  TestEGStateWithAbortedWriteHelper(8, -1, 50, 0, true /* truncate egroup */,
                                    true /* fail truncate */);

  LOG(INFO) << "Stack TU 1,2,3...N,  abort 3, followed by failed truncate and "
            << "validate all write complete successfully with some new "
            << "fallocations";
  TestEGStateWithAbortedWriteHelper(8, 2, 50, 0, true /* truncate egroup */,
                                    true /* fail truncate */);

  LOG(INFO) << "Stack TU 1,2,3...N, followed by truncate and replicate. "
            << "Validate all write complete successfully with some new "
            << "fallocations";
  TestEGStateWithAbortedWriteHelper(8, -1, 50, 0, true /* truncate egroup */,
                                    false /* fail truncate */,
                                    true /* replicate */);

  LOG(INFO) << "Stack TU 1,2,3...N, abort 3, followed by truncate & replicate."
            << " Validate all write complete successfully with some new "
            << "fallocations";
  TestEGStateWithAbortedWriteHelper(8, 3, 50, 0, true /* truncate egroup */,
                                    false /* fail truncate */,
                                    true /* replicate */);

  LOG(INFO) << "Stack TU 1,2,3...N, abort 3 followed by replicate. "
            << "Validate all write complete successfully with some new "
            << "fallocations";
  TestEGStateWithAbortedWriteHelper(8, 2, 50, 0, false /* truncate egroup */,
                                    false /* fail truncate */,
                                    true /* replicate */);

  LOG(INFO) << "Stack TU 1,2,3...N, followed by stargate crash. "
            << "Validate all write complete successfully with some new "
            << "fallocations";
  TestEGStateWithAbortedWriteHelper(8, -1, 50, 0, false /* truncate egroup */,
                                    false /* fail truncate */,
                                    false /* replicate */,
                                    true /* restart stargate */);

  LOG(INFO) << "Stack TU 1,2,3...N, followed by stargate crash and truncate. "
            << "Validate all write complete successfully with some new "
            << "fallocations";
  TestEGStateWithAbortedWriteHelper(8, -1, 50, 0, true /* truncate egroup */,
                                    false /* fail truncate */,
                                    false /* replicate */,
                                    true /* restart stargate */);

  LOG(INFO) << "Stack TU 1,2,3...N, followed by stargate crash, truncate and "
            << "replicate. Validate all write complete successfully with some "
            << "new fallocations";
  TestEGStateWithAbortedWriteHelper(8, -1, 50, 0, true /* truncate egroup */,
                                    false /* fail truncate */,
                                    true /* replicate */,
                                    true /* restart stargate */);

  LOG(INFO) << "Stack TU 1,2,3...N, abort 4 and validate that state is "
            << "rolled forward upto 2 with some new fallocations. "
            << "Try writing to a slice previously written";
  TestEGStateWithAbortedWriteHelper(8, 3, 50, 0, false /* truncate egroup */,
                                    false /* fail truncate */,
                                    false /* replicate */,
                                    false /* restart stargate */,
                                    true /* test_same_slice_overwrite */);

  LOG(INFO) << "Stack TU 1,2,3...N, followed by truncate and abort 4 and "
            << "validate that state is rolled forward upto 2 with no new "
            << "fallocations. Try writing to a slice previously written";
  TestEGStateWithAbortedWriteHelper(8, 3, 50, 0, true /* truncate egroup */,
                                    false /* fail truncate */,
                                    false /* replicate */,
                                    false /* restart stargate */,
                                    true /* test_same_slice_overwrite */);

  LOG(INFO) << "Stack TU 1,2,3...N, followed by truncate and abort 4 and "
            << "validate that state is rolled forward upto 2 with some some "
            << "fallocations. Try writing to a slice previously written";
  TestEGStateWithAbortedWriteHelper(8, 3, 50, 0, true /* truncate egroup */,
                                    false /* fail truncate */,
                                    true /* replicate */,
                                    false /* restart stargate */,
                                    true /* test_same_slice_overwrite */);

  TestEGStateWithAbortedWriteHelper(8, 2, 50, 0, true /* truncate egroup */);

  LOG(INFO) << "Stack TU 1,2,3...N, followed by truncate and validate all "
            << "write complete successfully with no new fallocations. "
            << "Try writing to a slice previously written.";
  TestEGStateWithAbortedWriteHelper(8, 8, 50, 0, true /* truncate egroup */,
                                    false /* fail truncate */,
                                    true /* replicate */,
                                    false /* restart stargate */,
                                    true /* test_same_slice_overwrite */);
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::TestOutoforderWritesWithInterleavedRead() {
  LOG(INFO) << "Running " << __FUNCTION__;
  vector<Notification::Ptr> write_notification_vec;
  const int64 egroup_id = GetNextEgroupId();
  const int64 vblock_num = 6000;
  IOBuffer::Ptr write_data_init;
  vector<RegionDescriptor> region_to_write_vec;
  string transformation_type = FLAGS_egroup_encryption_type;

  SetStargateGFlag("estore_experimental_background_scan_pause_processing",
                   "true");
  // Do an initial write to the extent group so that the future read won't
  // return an error.
  vector<shared_ptr<SliceState>> slice_state_vec_init;
  vector<shared_ptr<EgroupOpsTester::Extent>> extent_state_vec_init;
  write_notification_vec.emplace_back(make_shared<Notification>());
  write_data_init = make_shared<IOBuffer>();
  region_to_write_vec.emplace_back(0, 32768, 0);
  egroup_id_intent_seq_map_[egroup_id] = 0;
  WriteSingleExtentInExtentGroup(egroup_id,
                                 disk_ids_[0],
                                 disk_ids_[1],
                                 vblock_num,
                                 transformation_type,
                                 vector<DataTransformation::Type>(),
                                 region_to_write_vec,
                                 write_notification_vec.back().get(),
                                 &slice_state_vec_init,
                                 &extent_state_vec_init,
                                 write_data_init);
  write_notification_vec.clear();

  vector<shared_ptr<SliceState>> slice_state_vec_w1;
  vector<shared_ptr<EgroupOpsTester::Extent>> extent_state_vec_w1;
  IOBuffer::Ptr write_data_w1 = make_shared<IOBuffer>();
  write_notification_vec.emplace_back(make_shared<Notification>());
  // This gflag will wait for 100ms after register_read_write_op.
  // This allows the next read and write to proceeed.
  SetStargateGFlag("estore_experimental_delay_after_register_writer_op_msecs",
                   "100");
  map<StargateError::Type, int> expected_errors;
  expected_errors[StargateError::kUnexpectedIntentSequence] = 0;
  egroup_id_intent_seq_map_[egroup_id] = 2;
  WriteSingleExtentInExtentGroup(egroup_id,
                                 disk_ids_[0],
                                 disk_ids_[1],
                                 vblock_num,
                                 transformation_type,
                                 vector<DataTransformation::Type>(),
                                 region_to_write_vec,
                                 write_notification_vec.back().get(),
                                 &slice_state_vec_w1,
                                 &extent_state_vec_w1,
                                 write_data_w1,
                                 &expected_errors,
                                 false /* wait_for_write */);
  SetStargateGFlag("estore_experimental_delay_after_register_writer_op_msecs",
                   "0");

  vector<Notification::Ptr> read_notification_vec;
  vector<pair<int64, int64>> region_to_read_vec;
  region_to_read_vec.emplace_back(make_pair(0, 32768));
  egroup_id_intent_seq_map_[egroup_id] = 0;
  LOG(INFO) << "Reading from extent group " << egroup_id;
  ReadExtentGroup(egroup_id,
                  disk_ids_[0],
                  read_notification_vec,
                  region_to_read_vec,
                  slice_state_vec_init,
                  extent_state_vec_init,
                  write_data_init,
                  region_to_write_vec[0].offset,
                  false /*compressed_slice*/,
                  false /*wait_for_read*/);

  vector<shared_ptr<SliceState>> slice_state_vec_w2;
  vector<shared_ptr<EgroupOpsTester::Extent>> extent_state_vec_w2;
  IOBuffer::Ptr write_data_w2 = make_shared<IOBuffer>();
  write_notification_vec.emplace_back(make_shared<Notification>());
  egroup_id_intent_seq_map_[egroup_id] = 1;
  LOG(INFO) << "Writing to extent group " << egroup_id << " with "
            << "intent sequence " << egroup_id_intent_seq_map_[egroup_id];

  WriteSingleExtentInExtentGroup(egroup_id,
                                 disk_ids_[0],
                                 disk_ids_[1],
                                 vblock_num,
                                 transformation_type,
                                 vector<DataTransformation::Type>(),
                                 region_to_write_vec,
                                 write_notification_vec.back().get(),
                                 &slice_state_vec_w2,
                                 &extent_state_vec_w2,
                                 write_data_w2,
                                 nullptr/* expected_error_map */,
                                 false /* wait_for_write */);

  for (const Notification::Ptr& notification : write_notification_vec) {
    notification->Wait();
  }

  for (const Notification::Ptr& notification : read_notification_vec) {
    notification->Wait();
  }
  CHECK_EQ(expected_errors[StargateError::kUnexpectedIntentSequence], 1);
  SetStargateGFlag("estore_experimental_background_scan_pause_processing",
                   "false");
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::TestNonFinalizedCheckpointDuringReplication(
  bool crash_before_replica_finalization) {

  LOG(INFO) << "Running " << __FUNCTION__
            << " crash_before_replica_finalization: "
            << crash_before_replica_finalization;

  const int64 egroup_id = GetNextEgroupId();
  string transformation_type = FLAGS_egroup_encryption_type;

  // Test setup - allocate all slices.
  LOG(INFO) << "Allocating egroup " << egroup_id;

  const int64 vblock_num = 2000;
  vector<shared_ptr<SliceState>> all_slice_state_vec;
  vector<shared_ptr<SliceState>> slice_state_vec;
  vector<shared_ptr<EgroupOpsTester::Extent>> extent_state_vec;
  int64 latest_intent_sequence = 0;

  vector<RegionDescriptor> region_to_write_vec;
  region_to_write_vec.emplace_back(0, 1048576, 0);
  IOBuffer::Ptr data = Random::TL()->CreateRandomDataIOBuffer(1048576LL);

  Notification notification[2];
  WriteSingleExtentInExtentGroup(egroup_id,
                                 disk_ids_[0],
                                 disk_ids_[1],
                                 vblock_num,
                                 transformation_type,
                                 vector<DataTransformation::Type>(),
                                 region_to_write_vec,
                                 &notification[0],
                                 &all_slice_state_vec,
                                 &extent_state_vec,
                                 data);

  CHECK_EQ(all_slice_state_vec.size(), 32);
  CHECK_EQ(extent_state_vec.size(), 1);
  egroup_id_intent_seq_map_[egroup_id] = 0;

  LOG(INFO) << "Created egroup " << egroup_id << " on disks " << disk_ids_[0]
            << " and " << disk_ids_[1] << " at applied intent sequence "
            << egroup_id_intent_seq_map_[egroup_id];

  const int64 secondary_stargate_index =
    cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(disk_ids_[1]);
  const int64 secondary_svm_id =
    cluster_mgr_->stargate_mgr()->GetSvmIdForStargate(
      secondary_stargate_index);

  // Now we'll issue some more write on the above egroup. All WAL records
  // associated with this egroup will go in the new delta file that follows the
  // initiated checkpoint. Block the write on the secondary so that only the
  // primary writes succeed.
  cluster_mgr_->stargate_mgr()->SetGFlag(
    "estore_experimental_pause_write_after_getting_fd", "true",
    secondary_svm_id);

  // Start the disk WAL checkpoint on one the estore disk under test, but pause
  // the checkpoint as soon as it starts.
  Notification checkpoint_notification;
  SetStargateGFlag("estore_experimental_pause_checkpoint_at_start",
                   StringJoin(disk_ids_[0]));
  Function<void(bool)> done_cb =
    bind(&CheckpointWALDone, _1, &checkpoint_notification);
  CheckpointDiskWAL(disk_ids_[0], done_cb);

  map<StargateError::Type, int> expected_errors;
  expected_errors[StargateError::kSecondaryWriteFailure] = 0;

  region_to_write_vec.clear();
  region_to_write_vec.emplace_back(0, 32768, 0);
  data = Random::TL()->CreateRandomDataIOBuffer(32768LL);

  ++latest_intent_sequence;
  ++egroup_id_intent_seq_map_[egroup_id];
  WriteSingleExtentInExtentGroup(egroup_id,
                                 disk_ids_[0],
                                 disk_ids_[1],
                                 vblock_num,
                                 transformation_type,
                                 vector<DataTransformation::Type>(),
                                 region_to_write_vec,
                                 &notification[0],
                                 &slice_state_vec,
                                 &extent_state_vec,
                                 data,
                                 &expected_errors,
                                 false /* wait_for_write */);

  region_to_write_vec.clear();
  region_to_write_vec.emplace_back(65536, 32768, 0);
  data = Random::TL()->CreateRandomDataIOBuffer(32768LL);

  ++latest_intent_sequence;
  ++egroup_id_intent_seq_map_[egroup_id];
  WriteSingleExtentInExtentGroup(egroup_id,
                                 disk_ids_[0],
                                 disk_ids_[1],
                                 vblock_num,
                                 transformation_type,
                                 vector<DataTransformation::Type>(),
                                 region_to_write_vec,
                                 &notification[1],
                                 &slice_state_vec,
                                 &extent_state_vec,
                                 data,
                                 &expected_errors,
                                 false /* wait_for_write */);

  sleep(2);

  // Restart secondary stargate.
  RestartStargateAtIndex(secondary_stargate_index);

  notification[0].Wait();
  notification[1].Wait();
  CHECK_EQ(expected_errors[StargateError::kSecondaryWriteFailure], 2);

  // Delete extent group from original disk.
  ++latest_intent_sequence;
  DeleteExtentGroup(egroup_id, disk_ids_[0]);

  // Reset applied intent sequence to 0 to match secondary replica and discard
  // the primary. This simulates a fixer rollback.
  egroup_id_intent_seq_map_[egroup_id] = 0;

  // Replicate extent group back to the original disk at an older intent
  // sequence and pause the replication finalization. If
  // crash_before_replica_finalization is true, we'll be restarting stargate
  // while the replication is blocked at finalize, hence we'll pass
  // kTransportError as the expected error in that case.
  const StargateError::Type expected_error =
    (crash_before_replica_finalization ?
     StargateError::kTransportError :
     StargateError::kNoError);
  notification[0].Reset();
  SetStargateGFlag("estore_experimental_replication_delay_finalize",
                   StringJoin(egroup_id));
  ++latest_intent_sequence;
  ReplicateExtentGroup(egroup_id,
                       disk_ids_[1],
                       disk_ids_[0],
                       all_slice_state_vec,
                       latest_intent_sequence,
                       false /* managed_by_aes */,
                       false /* extent_based_format */,
                       -1 /* owner_container_id */,
                       nullptr /* extent_id_list */,
                       expected_error,
                       &notification[0]);

  // Unblock the checkpoint process and wait for it to finish.
  SetStargateGFlag("estore_experimental_pause_checkpoint_at_start", "-1");
  checkpoint_notification.Wait();

  if (!crash_before_replica_finalization) {
    // Unblock replication so that the replica is finalized.
    SetStargateGFlag("estore_experimental_replication_delay_finalize", "-1");

    // Wait for the replication to complete.
    LOG(INFO) << "Waiting for replication to complete";
    notification[0].Wait();

    // Issue a GetEgroupState RPC to simulate a fixer op and verify the extent
    // group state.
    ++latest_intent_sequence;
    interface::GetEgroupStateRet egroup_state;
    GetEgroupState(egroup_id, disk_ids_[0], latest_intent_sequence,
                   &egroup_state);
    CHECK(egroup_state.has_latest_applied_intent_sequence()) << egroup_id;
    CHECK_EQ(egroup_state.latest_applied_intent_sequence(), 0) << egroup_id;
    CHECK(!egroup_state.is_corrupt()) << egroup_id;
    CHECK_EQ(egroup_state.extents_size(), 0) << egroup_id;
    CHECK_EQ(egroup_state.slices_size(), 0) << egroup_id;

    // Verify data from all replicas.
    for (int ii = 0; ii < 2; ++ii) {
      LOG(INFO) << "Verifying data from replica: " << disk_ids_[ii];
      ReplicaReadEgroupAndVerify(
        egroup_id, disk_ids_[ii], &all_slice_state_vec);
    }
  }

  // Restart stargate to trigger a recovery.
  RestartStargateAtIndex(
      cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(disk_ids_[0]));

  // Wait for the recovery to complete.
  sleep(10);

  // Issue a GetEgroupState RPC to simulate a fixer op and verify the extent
  // group state post recovery.
  ++latest_intent_sequence;
  interface::GetEgroupStateRet egroup_state;
  GetEgroupState(egroup_id, disk_ids_[0], latest_intent_sequence,
                 &egroup_state);
  CHECK(egroup_state.has_latest_applied_intent_sequence()) << egroup_id;
  CHECK_EQ(egroup_state.latest_applied_intent_sequence(), 0) << egroup_id;
  CHECK(!egroup_state.is_corrupt()) << egroup_id;
  CHECK_EQ(egroup_state.extents_size(), 0) << egroup_id;
  CHECK_EQ(egroup_state.slices_size(), 0) << egroup_id;

  // Verify data from all replicas.
  for (int ii = 0; ii < 2; ++ii) {
    LOG(INFO) << "Verifying data from replica: " << disk_ids_[ii];
    ReplicaReadEgroupAndVerify(egroup_id, disk_ids_[ii], &all_slice_state_vec);
  }

  SetStargateGFlag("estore_experimental_replication_delay_finalize", "-1");
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::TestNextSliceAllocationOffsetHintUpdate() {
  LOG(INFO) << "Running " << __FUNCTION__;

  // In order to test if this field is updated, let us perform some mixed
  // compression writes.
  // First let's write few slices on the extent group and check if the value
  // gets updated.
  TestNextSliceAllocationOffsetHintUpdateHelper();
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::TestSparseMultiRegionRead() {
  LOG(INFO) << "Running " << __FUNCTION__;

  const int64 egroup_id = GetNextEgroupId();

  const int64 vblock_num = 5000;
  vector<shared_ptr<SliceState>> applied_slice_state_vec;
  vector<shared_ptr<SliceState>> slice_state_vec;
  vector<shared_ptr<EgroupOpsTester::Extent>> extent_state_vec;

  IOBuffer::Ptr data;

  const int kDataSize = 524288;
  vector<RegionDescriptor> region_to_write_vec;
  region_to_write_vec.emplace_back(0, kDataSize, 0);
  data = Random::TL()->CreateRandomDataIOBuffer(kDataSize);

  Notification notification;
  WriteSingleExtentInExtentGroup(egroup_id,
                                 disk_ids_[0],
                                 disk_ids_[1],
                                 vblock_num,
                                 string() /* transformation_type */,
                                 vector<DataTransformation::Type>(),
                                 region_to_write_vec,
                                 &notification,
                                 &applied_slice_state_vec,
                                 &extent_state_vec,
                                 data,
                                 nullptr /* expected_error_map */,
                                 true /* wait_for_write */,
                                 -1 /* latest_applied_intent_sequence */,
                                 Optional<bool>() /* is_sequential */,
                                 true /* managed_by_aes */);
  notification.Wait();

  Notification checkpoint_notification;
  Function<void(bool)> done_cb =
    bind(&CheckpointWALDone, _1, &checkpoint_notification);
  CheckpointDiskWAL(disk_ids_[0], done_cb);
  checkpoint_notification.Wait();

  shared_ptr<EgroupOpsTester::Extent> extent_info =
    make_shared<EgroupOpsTester::Extent>();
  auto ex = extent_info->mutable_extent_id();
  ex->set_vdisk_block(5000);
  ex->set_owner_id(vdisk_id_);

  {
    vector<pair<int64, int64>> region_to_read_vec;
    region_to_read_vec.emplace_back(make_pair(0, 1048576));
    egroup_id_intent_seq_map_[egroup_id] = 0;

    IOBuffer::Ptr extent_data = data->Clone();
    extent_data->AppendIOBuffer(IOBufferUtil::CreateZerosIOBuffer(524288));

    ReadExtentGroupMultiRegion(
      egroup_id,
      disk_ids_[0],
      region_to_read_vec,
      applied_slice_state_vec,
      extent_info,
      extent_data,
      string() /* transformation_type */,
      vector<DataTransformation::Type>(),
      set<StargateError::Type>({ StargateError::kNoError })
        /* expected_errors */,
      true /* managed_by_aes */,
      32768 /* untransformed_slice_length */,
      true /* slices_stored_by_id */);
  }
  {
    vector<pair<int64, int64>> region_to_read_vec;
    region_to_read_vec.emplace_back(make_pair(0, 4096));
    region_to_read_vec.emplace_back(make_pair(16384, 32768));
    region_to_read_vec.emplace_back(make_pair(262144, 524288));
    egroup_id_intent_seq_map_[egroup_id] = 0;

    IOBuffer::Ptr extent_data = data->Clone();
    extent_data->AppendIOBuffer(IOBufferUtil::CreateZerosIOBuffer(524288));
    CHECK_EQ(extent_data->size(), 1048576);

    IOBuffer::Ptr expected_data = make_shared<IOBuffer>();
    for (uint ii = 0; ii < region_to_read_vec.size(); ++ii) {
      expected_data->AppendIOBuffer(
        extent_data->Clone(region_to_read_vec[ii].first,
                           region_to_read_vec[ii].second));
    }

    ReadExtentGroupMultiRegion(
      egroup_id,
      disk_ids_[0],
      region_to_read_vec,
      applied_slice_state_vec,
      extent_info,
      expected_data,
      string() /* transformation_type */,
      vector<DataTransformation::Type>(),
      set<StargateError::Type>({ StargateError::kNoError })
        /* expected_errors */,
      true /* managed_by_aes */,
      32768 /* untransformed_slice_length */,
      true /* slices_stored_by_id */);
  }
  {
    vector<pair<int64, int64>> region_to_read_vec;
  }
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::TestOwnerVDiskChange() {
  LOG(INFO) << "Running " << __FUNCTION__;
  SetStargateGFlag("estore_experimental_background_scan_pause_processing",
                   "false");
  const string prev_scan_suspend_flag =
    GetStargateGflagStripped("estore_suspend_orphaned_egroup_deletion");
  SetStargateGFlag("estore_suspend_orphaned_egroup_deletion", "true");


  shared_ptr<vector<int64>> egroup_ids = make_shared<vector<int64>>();

  // Write some extent groups.
  vector<shared_ptr<EgroupOpsTester::SliceState>> slice_state_vec;

  const int64 egroup_id = GetNextEgroupId();
  const int num_vblocks_to_write = 3;
  egroup_id_intent_seq_map_[egroup_id] = 0;
  WriteExtentGroupHelper(egroup_id,
                         disk_ids_[0],
                         disk_ids_[1],
                         num_vblocks_to_write,
                         &slice_state_vec,
                         true /* managed_by_aes */);
  egroup_ids->push_back(egroup_id);

  // Also push an egroup that will not exist.
  egroup_ids->push_back(egroup_ids->back() + 1);

  const int64 owner_vdisk_id_map3 =
    LookupOrUpdateOwnerVDiskIdInEgidMap(egroup_id);
  const int latest_intent_sequence = num_vblocks_to_write - 1;
  // Issue a GetEgroupState RPC to fetch the latest extent group.
  interface::GetEgroupStateRet egroup_state;
  GetEgroupState(egroup_id, disk_ids_[0], latest_intent_sequence,
                 &egroup_state, true /* managed_by_aes */);
  CHECK_EQ(egroup_state.latest_applied_intent_sequence(),
           latest_intent_sequence);

  // Lookup non-physical state in map4.
  // Checkpoint the disk WALs. This will ensure that we flush the
  // metadata to AES DB.
  CheckpointDiskWALs();

  // There should be nothing in AES DB for this egroup on this replica.

  MedusaValue::PtrConst mvalue =
    LookupExtentGroupPhysicalState(disk_ids_[0], egroup_id);
  CHECK_GE(mvalue->timestamp, 0)
    << "disk_id: " << disk_ids_[0] << " egroup_id: " << egroup_id;
  const ControlBlockFB *const control_block =
    mvalue->extent_group_physical_state_entry->ControlBlock();

  const int64 owner_vdisk_id_map4 = control_block->owner_vdisk_id();
  DCHECK_EQ(owner_vdisk_id_map3, owner_vdisk_id_map4);

  // Update owner vdisk id in extent group id map entry for this egroup so as
  // to force owner vdisk id correction.
  const int64 new_owner_vdisk_id_map3 = LookupOrUpdateOwnerVDiskIdInEgidMap(
    egroup_id, true /* update */, owner_vdisk_id_map3 + 1);

  // Let background scans fix up the owner vdisk id.
  sleep(30);

  // Lookup non-physical state in map4.
  // Checkpoint the disk WALs. This will ensure that we flush the
  // metadata to AES DB.
  CheckpointDiskWALs();

  // There should be nothing in AES DB for this egroup on this replica.
  MedusaValue::PtrConst mvalue2 =
    LookupExtentGroupPhysicalState(disk_ids_[0], egroup_id);
  CHECK_GE(mvalue2->timestamp, 0)
    << "disk_id: " << disk_ids_[0] << " egroup_id: " << egroup_id;
  const ControlBlockFB *const control_block2 =
    mvalue2->extent_group_physical_state_entry->ControlBlock();

  const int64 new_owner_vdisk_id_map4 = control_block2->owner_vdisk_id();
  DCHECK_EQ(new_owner_vdisk_id_map3, new_owner_vdisk_id_map4);

  // Restore this egroup's metadata before exiting the test.
  LookupOrUpdateOwnerVDiskIdInEgidMap(egroup_id, true /* update */,
    owner_vdisk_id_map3);
  // Checkpoint the disk WALs. This will ensure that we flush the
  // metadata to AES DB.
  CheckpointDiskWALs();
  // Let background scans update the map4 control block entry with original
  // owner vdisk id.
  sleep(30);

  SetStargateGFlag("estore_experimental_background_scan_pause_processing",
                   "true");
  SetStargateGFlag("estore_suspend_orphaned_egroup_deletion",
                   prev_scan_suspend_flag);
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::TestUpdateLastScrubTime(const bool managed_by_aes) {
  LOG(INFO) << "Running " << __FUNCTION__;

  SetStargateGFlag("estore_experimental_background_scan_pause_processing",
                   "true");
  const int64 time_secs = WallTime::NowSecs();
  sleep(5);

  // Write some extent groups.
  const int32 num_vblocks_to_write = 1;
  const int num_egroups_to_write = 5;
  vector<shared_ptr<EgroupOpsTester::SliceState>> slice_state_vec;
  unordered_set<int64> egroup_id_set;
  LOG(INFO) << "Writing " << num_egroups_to_write << " egroups to disks "
            << disk_ids_[0] << ", " << disk_ids_[1];

  for (int ii = 0; ii < num_egroups_to_write; ++ii) {
    const int64 egroup_id = GetNextEgroupId();
    slice_state_vec.clear();
    WriteExtentGroupHelper(egroup_id,
                           disk_ids_[0],
                           disk_ids_[1],
                           num_vblocks_to_write,
                           &slice_state_vec,
                           managed_by_aes);
    egroup_id_set.insert(egroup_id);
  }
  sleep(5);

  // Fetch the egroups in disk_ids_[0] and migrate them to disk_ids_[2] and
  // disk_ids_[3]. Then check that the scrub time has been updated on the first
  // write and after migration.
  string cookie;
  int64 num_egroups = num_egroups_to_write;
  const vector<int64> desired_replica_disk_ids{ disk_ids_[2], disk_ids_[3] };
  unordered_map<int64, int64> egroup_scrub_time;
  FetchEgroupsRet egroups_ret;

  while (1) {
    FetchEgroups(disk_ids_[0], &egroups_ret, &cookie);
    for (int ii = 0; ii < egroups_ret.extent_group_id_size(); ++ii) {
      if (egroup_id_set.find(egroups_ret.extent_group_id(ii)) ==
            egroup_id_set.end()) {
        continue;
      }

      // Check that 'last_scrub_time_secs' was updated on the first write. Then
      // save the time to compare after migration.
      CHECK(egroups_ret.last_scrub_time_secs(ii) > time_secs);
      egroup_scrub_time[egroups_ret.extent_group_id(ii)] =
        egroups_ret.last_scrub_time_secs(ii);

      // Migrate the egroups.
      MigrateEgroups(
        vdisk_id_,
        0 /* stargate_index */,
        egroups_ret.extent_group_id(ii),
        desired_replica_disk_ids);
    }
    if (cookie.empty()) {
      LOG(INFO) << "Done fetching egroups from disk " << disk_ids_[0];
      break;
    }
  }

  sleep(5);

  while (1) {
    FetchEgroups(disk_ids_[2], &egroups_ret, &cookie);
    for (int jj = 0; jj < egroups_ret.extent_group_id_size(); ++jj) {
      if (egroup_id_set.find(egroups_ret.extent_group_id(jj)) ==
            egroup_id_set.end()) {
        continue;
      }

      // Check that after migrating, 'last_scrub_time_secs' was updated.
      CHECK(egroups_ret.last_scrub_time_secs(jj) >
            egroup_scrub_time.at(egroups_ret.extent_group_id(jj)));
      --num_egroups;
    }
    if (cookie.empty()) {
      LOG(INFO) << "Done fetching egroups from disk " << disk_ids_[2];
      break;
    }
  }

  // Check that all egroups were successfully migrated and checked.
  CHECK_EQ(num_egroups, 0) << num_egroups;
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::TestDiskUsageOnDelete() {
  LOG(INFO) << "Running " << __FUNCTION__;
  SetStargateGFlag("stargate_stats_dump_interval_secs", "1");
  SetStargateGFlag("stats_flush_interval_secs", "1");
  SetStargateGFlag("disk_manager_experimental_disable_disk_usage_op", "true");
  SetStargateGFlag("estore_experimental_background_scan_pause_processing",
                   "true");

  shared_ptr<vector<int64>> disk_ids = make_shared<vector<int64>>();
  disk_ids->emplace_back(disk_ids_[0]);
  disk_ids->emplace_back(disk_ids_[1]);
  vector<shared_ptr<DiskUsageStatProto>> disk_usage_stats_vec;
  GetDiskUsageStats(disk_ids, &disk_usage_stats_vec);
  // We expect the container usage to be zero when we start.
  CHECK_EQ(disk_usage_stats_vec[0]->containers_size(), 0);
  CHECK_EQ(disk_usage_stats_vec[1]->containers_size(), 0);
  const int64 initial_transformed_size_1 =
    disk_usage_stats_vec[0]->total_usage().transformed();
  const int64 initial_transformed_size_2 =
    disk_usage_stats_vec[1]->total_usage().transformed();

  // Write some extent groups.
  const int32 num_vblocks_to_write = 3;
  const int num_egroups_to_write = 10;
  vector<shared_ptr<EgroupOpsTester::SliceState>> slice_state_vec;
  vector<int64> egroup_id_vec;
  egroup_id_vec.reserve(num_egroups_to_write);
  LOG(INFO) << "Writing " << num_egroups_to_write << " egroups to disks "
            << disk_ids_[0] << ", " << disk_ids_[1];
  for (int ii = 0; ii < num_egroups_to_write; ++ii) {
    const int64 egroup_id = GetNextEgroupId();
    slice_state_vec.clear();
    WriteExtentGroupHelper(egroup_id,
                           disk_ids_[0],
                           disk_ids_[1],
                           num_vblocks_to_write,
                           &slice_state_vec,
                           true /* managed_by_aes */);
    egroup_id_vec.emplace_back(egroup_id);
  }
  sleep(10);

  disk_usage_stats_vec.clear();
  GetDiskUsageStats(disk_ids, &disk_usage_stats_vec);
  CHECK_EQ(disk_usage_stats_vec[0]->containers_size(), 1);
  CHECK_GT(disk_usage_stats_vec[0]->total_usage().transformed(),
           initial_transformed_size_1);
  CHECK_GT(
    disk_usage_stats_vec[0]->container_unshared_usage(0).transformed(), 0);
  CHECK_GT(
    disk_usage_stats_vec[0]->container_unshared_usage(0).untransformed(), 0);
  CHECK_EQ(disk_usage_stats_vec[1]->containers_size(), 1);
  CHECK_GT(disk_usage_stats_vec[1]->total_usage().transformed(),
           initial_transformed_size_2);
  CHECK_GT(
    disk_usage_stats_vec[1]->container_unshared_usage(0).transformed(), 0);
  CHECK_GT(
    disk_usage_stats_vec[1]->container_unshared_usage(0).untransformed(), 0);

  // Let's delete one replica of all the egroups.
  for (int ii = 0; ii < num_egroups_to_write; ++ii) {
    DeleteExtentGroup(
      egroup_id_vec[ii], disk_ids_[0], true /* managed_by_aes */);
  }

  // Checkpoint the disk WALs.
  Notification checkpoint_notification(2);
  Function<void(bool)> done_cb =
    bind(&CheckpointWALDone, _1, &checkpoint_notification);
  CheckpointDiskWAL(disk_ids_[0], done_cb);
  CheckpointDiskWAL(disk_ids_[1], done_cb);
  checkpoint_notification.Wait();

  // Verify that usage was reset to zero for the disk from where we deleted all
  // the replicas.
  sleep(10);
  disk_usage_stats_vec.clear();
  GetDiskUsageStats(disk_ids, &disk_usage_stats_vec);
  CHECK_EQ(disk_usage_stats_vec[0]->containers_size(), 0)
    << disk_ids_[0];
  int64 total_metadata_usage = 0;
  if (disk_usage_stats_vec[0]->metadata_usage_size() > 0) {
    total_metadata_usage =
      disk_usage_stats_vec[0]->metadata_usage(0).kv_store_usage_bytes() +
      disk_usage_stats_vec[0]->metadata_usage(0).disk_wal_usage_bytes();
  }
  CHECK(disk_usage_stats_vec[0]->total_usage().transformed() == 0 ||
        disk_usage_stats_vec[0]->total_usage().transformed() ==
          total_metadata_usage);
  CHECK_EQ(disk_usage_stats_vec[1]->containers_size(), 1);
  CHECK_GT(disk_usage_stats_vec[1]->total_usage().transformed(),
           initial_transformed_size_2);
  CHECK_GT(
    disk_usage_stats_vec[1]->container_unshared_usage(0).transformed(), 0);
  CHECK_GT(
    disk_usage_stats_vec[1]->container_unshared_usage(0).untransformed(), 0);

  // Let's delete the other replica of all the egroups to test orphan replica
  // case.
  for (int ii = 0; ii < num_egroups_to_write; ++ii) {
    DeleteExtentGroup(
      egroup_id_vec[ii], disk_ids_[1], true /* managed_by_aes */);
  }

  // Checkpoint the disk WALs.
  checkpoint_notification.Reset(2 /* awaiting */);
  CheckpointDiskWAL(disk_ids_[0], done_cb);
  CheckpointDiskWAL(disk_ids_[1], done_cb);
  checkpoint_notification.Wait();

  // Verify that usage was reset to zero for the disk from where we deleted all
  // the replicas.
  sleep(10);
  disk_usage_stats_vec.clear();
  GetDiskUsageStats(disk_ids, &disk_usage_stats_vec);
  CHECK_EQ(disk_usage_stats_vec[0]->containers_size(), 0);
  total_metadata_usage = 0;
  if (disk_usage_stats_vec[0]->metadata_usage_size() > 0) {
    total_metadata_usage =
      disk_usage_stats_vec[0]->metadata_usage(0).kv_store_usage_bytes() +
      disk_usage_stats_vec[0]->metadata_usage(0).disk_wal_usage_bytes();
  }
  CHECK(disk_usage_stats_vec[0]->total_usage().transformed() == 0 ||
        disk_usage_stats_vec[0]->total_usage().transformed() ==
          total_metadata_usage);
  CHECK_EQ(disk_usage_stats_vec[1]->containers_size(), 0);
  total_metadata_usage = 0;
  if (disk_usage_stats_vec[1]->metadata_usage_size() > 0) {
    total_metadata_usage =
      disk_usage_stats_vec[1]->metadata_usage(0).kv_store_usage_bytes() +
      disk_usage_stats_vec[1]->metadata_usage(0).disk_wal_usage_bytes();
  }
  CHECK(disk_usage_stats_vec[1]->total_usage().transformed() == 0 ||
        disk_usage_stats_vec[1]->total_usage().transformed() ==
          total_metadata_usage);
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::TestConversionInteropWithBgScan() {
  LOG(INFO) << "Running " << __FUNCTION__;
  const string prev_scan_pause_gflag = GetStargateGflagStripped(
    "estore_experimental_background_scan_pause_processing");
  const string prev_scan_suspend_flag =
    GetStargateGflagStripped("estore_suspend_orphaned_egroup_deletion");

  SetStargateGFlag("estore_experimental_background_scan_pause_processing",
                   "false");
  SetStargateGFlag("estore_suspend_orphaned_egroup_deletion", "true");

  const vector<int64> disk_ids =
    { disk_ids_[0], disk_ids_[1]};
  const int64 egroup_id = GetNextEgroupId();

  // Add the necessary arguments to the write request.
  shared_ptr<WriteExtentGroupArg> arg = WriteEgArg();
  arg->set_extent_group_id(egroup_id);
  arg->set_owner_vdisk_id(vdisk_id_);
  arg->set_owner_container_id(container_id_);
  for (const int64 disk_id : disk_ids) {
    arg->add_disk_ids(disk_id);
  }

  const int32 untransformed_slice_length =
    StargateUtil::GetSliceSizeForTransformation({ });
  arg->set_untransformed_slice_length(untransformed_slice_length);

  Notification notify;
  shared_ptr<WriteExtentGroupRet> write_ret;
  Function<void(StargateError::Type,
                shared_ptr<string>,
                shared_ptr<WriteExtentGroupRet>)> done_cb =
    [&notify, &arg, &write_ret](const StargateError::Type err,
                    const shared_ptr<string> err_detail,
                    const shared_ptr<WriteExtentGroupRet> ret) {

      CHECK(err == StargateError::kNoError)
        << "Write failed with " << err << " " << arg->ShortDebugString();
      write_ret = ret;
      notify.Notify();
    };
  func_disabler_.Wrap(&done_cb);

  const int32 kRegionLength = 1048576;
  vector<shared_ptr<EgroupOpsTester::SliceState>> first_write_slice_state;
  arg->set_intent_sequence(0);
  WriteExtentGroupArg::Primary *const primary_state = arg->mutable_primary();
  WriteExtentGroupArg::Primary::Extent *const ex =
    primary_state->add_extents();
  ExtentIdProto *const extent_id_proto = ex->mutable_extent_id();
  const int32 kVBlockNum = 0;
  extent_id_proto->set_vdisk_block(kVBlockNum);
  extent_id_proto->set_owner_id(vdisk_id_);
  ex->add_region_offset(0);
  ex->add_region_length(kRegionLength);
  IOBuffer::Ptr data =
    Random::CreateRandomDataIOBuffer(kRegionLength);
  LOG(INFO) << "Add first TU to egroup " << egroup_id;
  MakeTentativeUpdate(egroup_id, disk_ids[0], disk_ids[1], string(), { },
                      extent_id_proto, false /* managed_by_aes */);
  notify.Reset();
  const StargateInterface::Ptr& iface =
    cluster_mgr_->stargate_mgr()->GetStargateInterfaceForDisk(disk_ids[0]);
  CHECK(iface) << disk_ids[0];
  LOG(INFO) << "Perform write on egroup " << egroup_id << " "
            << arg->ShortDebugString();
  auto done_cb_copy = done_cb;
  iface->WriteExtentGroup(arg, data, move(done_cb_copy));
  notify.Wait();
  FinalizeTentativeUpdate(egroup_id, 0, write_ret);

  // Pause the background scan for the egroup so that the background scan
  // meta op doesn't get spawned.
  SetStargateGFlag("stargate_experimental_disk_manager_pause_bg_scan",
                   StringJoin(egroup_id));
  // Sleep for 10 seconds to ensure that the egroup is selected for background
  // scan.
  sleep(10);
  // Issue a conversion op and make sure that it succeeds.
  ConvertEgroupToAES(
    egroup_id, disk_ids[0], 1 /* intent_sequence */, true /* is_primary */);
  ConvertEgroupToAES(
    egroup_id, disk_ids[1], 1 /* intent_sequence */, false /* is_primary */);
  SetStargateGFlag("stargate_experimental_disk_manager_pause_bg_scan",
                   "-1");
  sleep(5);
  const string& log_dir =
    cluster_mgr_->stargate_mgr()->GetLogDir(0 /* index */);
  string cmd = StringJoin(
    "grep \"Ignoring egroup since it is converted to AES while background "
    "scan was in-progress\" ", log_dir, "/*INFO*");
  string output;
  CHECK(ShellUtil::ExecWithOutput(cmd, &output));
  LOG(INFO) << "Output: " << output;
  CHECK_GT(output.size(), 0);
  // Restore the gflag.
  SetStargateGFlag("estore_experimental_background_scan_pause_processing",
                   prev_scan_pause_gflag);
  SetStargateGFlag("estore_suspend_orphaned_egroup_deletion",
                   prev_scan_suspend_flag);
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::TestReplicateAESEgroupToNonAESZombie() {
  LOG(INFO) << "Running " << __FUNCTION__;
  const vector<int64> disk_ids = {disk_ids_[0], disk_ids_[1]};
  const int64 egroup_id = GetNextEgroupId();

  // Inject an error during write so that the write on secondary fails and
  // it becomes a zombie replica on recovery.
  cluster_mgr_->stargate_mgr()->SetGFlag(
    "estore_experimental_slice_write_inject_crash",
    "5" /* kNoSliceWritesOnSecondaryReplica */);
  Notification notify;
  shared_ptr<WriteExtentGroupRet> write_ret;
  Function<void(
    StargateError::Type, shared_ptr<string>, shared_ptr<WriteExtentGroupRet>)>
    done_cb = [&notify](const StargateError::Type err,
                        const shared_ptr<string> err_detail,
                        const shared_ptr<WriteExtentGroupRet> ret) {

      CHECK_EQ(err, StargateError::kTransportError);
      notify.Notify();
    };
  func_disabler_.Wrap(&done_cb);

  // Add the necessary arguments to the write request.
  shared_ptr<WriteExtentGroupArg> arg = WriteEgArg();
  arg->set_extent_group_id(egroup_id);
  arg->set_owner_vdisk_id(vdisk_id_);
  arg->set_owner_container_id(container_id_);
  for (const int64 disk_id : disk_ids) {
    arg->add_disk_ids(disk_id);
  }

  const int32 untransformed_slice_length =
    StargateUtil::GetSliceSizeForTransformation({});
  arg->set_untransformed_slice_length(untransformed_slice_length);

  const int32 kRegionLength = 1048576;
  vector<shared_ptr<EgroupOpsTester::SliceState>> first_write_slice_state;
  arg->set_intent_sequence(0);
  WriteExtentGroupArg::Primary *const primary_state = arg->mutable_primary();
  WriteExtentGroupArg::Primary::Extent *const ex =
    primary_state->add_extents();
  ExtentIdProto *const extent_id_proto = ex->mutable_extent_id();
  const int32 kVBlockNum = 0;
  extent_id_proto->set_vdisk_block(kVBlockNum);
  extent_id_proto->set_owner_id(vdisk_id_);
  ex->add_region_offset(0);
  ex->add_region_length(kRegionLength);
  IOBuffer::Ptr data = Random::CreateRandomDataIOBuffer(kRegionLength);
  LOG(INFO) << "Add first TU to egroup " << egroup_id;
  MakeTentativeUpdate(egroup_id, disk_ids[0], -1 /* secondary_disk_id */,
                      string(), { }, extent_id_proto,
                      false /* managed_by_aes */);
  notify.Reset();
  const StargateInterface::Ptr& iface =
    cluster_mgr_->stargate_mgr()->GetStargateInterfaceForDisk(disk_ids[0]);
  CHECK(iface) << disk_ids[0];
  LOG(INFO) << "Perform write on egroup " << egroup_id << " "
            << arg->ShortDebugString();
  auto done_cb_copy = done_cb;
  uint primary_idx =
    cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(disk_ids[0]);
  uint secondary_idx =
    cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(disk_ids[1]);
  iface->WriteExtentGroup(arg, data, move(done_cb_copy));
  notify.Wait();
  // Restart the crashed Stargates.
  RestartStargateAtIndex(primary_idx);
  RestartStargateAtIndex(secondary_idx);
  // Gets the state of the primary replica on which the write succeeded.
  interface::GetEgroupStateRet egroup_state;
  GetEgroupState(egroup_id,
                 disk_ids_[0],
                 0 /* latest_intent_sequence */,
                 &egroup_state,
                 false /* managed_by_aes */,
                 false /* extent_based_format */,
                 nullptr /* error_ret */,
                 false /* set_latest_applied_intent_sequence */);
  CHECK_EQ(egroup_state.latest_applied_intent_sequence(), 0);
  egroup_id_intent_seq_map_[egroup_id] = 0;
  // Copy the extent states and slice states onto WriteExtentGroupRet so that
  // we can finalize the TU.
  write_ret = make_shared<WriteExtentGroupRet>();
  write_ret->mutable_primary()->mutable_slices()->CopyFrom(
    egroup_state.slices());
  for (int ii = 0; ii < egroup_state.extents_size(); ++ii) {
    WriteExtentGroupRet::Primary::Extent *extent =
      write_ret->mutable_primary()->add_extents();
    extent->mutable_extent_id()->CopyFrom(
      egroup_state.extents(ii).extent_id());
    extent->mutable_diff_data_location()->CopyFrom(
      egroup_state.extents(ii).diff_data_location());
  }
  FinalizeTentativeUpdate(egroup_id, 0, write_ret);

  // Issue a conversion op on the primary replica and make sure that it
  // succeeds.
  ConvertEgroupToAES(
    egroup_id, disk_ids[0], 2 /* intent_sequence */, true /* is_primary */);

  vector<shared_ptr<EgroupOpsTester::SliceState>> slice_state_vec;
  vector<medusa::ExtentIdProto> extent_id_list;
  ReplicateExtentGroup(egroup_id,
                       disk_ids[0],
                       disk_ids[1],
                       slice_state_vec,
                       2 /* latest_intent_sequence */,
                       true /* managed_by_aes */,
                       false /* extent_based_format */,
                       container_id_,
                       &extent_id_list);

  // Restart stargate for the secondary disk so that WAL recovery gets
  // triggered.
  RestartStargateAtIndex(secondary_idx);
  GetEgroupState(
    egroup_id, disk_ids_[1], 3 /* latest_intent_sequence */, &egroup_state);
  CHECK(egroup_state.managed_by_aes());
  cluster_mgr_->stargate_mgr()->SetGFlag(
    "estore_experimental_slice_write_inject_crash", "-1");
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::TestConversionOp(const bool checkpoint_before_recovery) {
  LOG(INFO) << "Running " << __FUNCTION__ << " checkpoint_before_recovery:"
            << boolalpha << checkpoint_before_recovery;
  const vector<int64> disk_ids =
    { disk_ids_[0], disk_ids_[1]};
  const int64 egroup_id = GetNextEgroupId();

  // Add the necessary arguments to the write request.
  shared_ptr<WriteExtentGroupArg> arg = WriteEgArg();
  arg->set_extent_group_id(egroup_id);
  arg->set_owner_vdisk_id(vdisk_id_);
  arg->set_owner_container_id(container_id_);
  for (const int64 disk_id : disk_ids) {
    arg->add_disk_ids(disk_id);
  }

  const int32 untransformed_slice_length =
    StargateUtil::GetSliceSizeForTransformation({ });
  arg->set_untransformed_slice_length(untransformed_slice_length);

  Notification notify;
  shared_ptr<WriteExtentGroupRet> write_ret;
  Function<void(StargateError::Type,
                shared_ptr<string>,
                shared_ptr<WriteExtentGroupRet>)> done_cb =
    [&notify, &arg, &write_ret](const StargateError::Type err,
                    const shared_ptr<string> err_detail,
                    const shared_ptr<WriteExtentGroupRet> ret) {

      CHECK(err == StargateError::kNoError)
        << "Write failed with " << err << " " << arg->ShortDebugString();
      write_ret = ret;
      notify.Notify();
    };
  func_disabler_.Wrap(&done_cb);

  const int32 kRegionLength = 1048576;
  vector<shared_ptr<EgroupOpsTester::SliceState>> first_write_slice_state;
  arg->set_intent_sequence(0);
  WriteExtentGroupArg::Primary *const primary_state = arg->mutable_primary();
  WriteExtentGroupArg::Primary::Extent *const ex =
    primary_state->add_extents();
  ExtentIdProto *const extent_id_proto = ex->mutable_extent_id();
  const int32 kVBlockNum = 0;
  extent_id_proto->set_vdisk_block(kVBlockNum);
  extent_id_proto->set_owner_id(vdisk_id_);
  ex->add_region_offset(0);
  ex->add_region_length(kRegionLength);
  IOBuffer::Ptr data =
    Random::CreateRandomDataIOBuffer(kRegionLength);
  LOG(INFO) << "Add first TU to egroup " << egroup_id;
  MakeTentativeUpdate(egroup_id, disk_ids[0], disk_ids[1], string(), { },
                      extent_id_proto, false /* managed_by_aes */);
  notify.Reset();
  const StargateInterface::Ptr& iface =
    cluster_mgr_->stargate_mgr()->GetStargateInterfaceForDisk(disk_ids[0]);
  CHECK(iface) << disk_ids[0];
  LOG(INFO) << "Perform write on egroup " << egroup_id << " "
            << arg->ShortDebugString();
  auto done_cb_copy = done_cb;
  iface->WriteExtentGroup(arg, data, move(done_cb_copy));
  notify.Wait();
  FinalizeTentativeUpdate(egroup_id, 0, write_ret);

  egroup_id_intent_seq_map_[egroup_id] = 0;
  vector<shared_ptr<EgroupOpsTester::SliceState>> slice_state_vec;
  vector<shared_ptr<EgroupOpsTester::Extent>> extent_state_vec;
  for (int ii = 0; ii < write_ret->primary().slices().size(); ++ii) {
    shared_ptr<EgroupOpsTester::SliceState> slice_state =
      EgroupOpsTester::SliceState::Create(write_ret->primary().slices(ii));
    slice_state_vec.emplace_back(slice_state);
  }
  // Sort the slice states in the order of slice ids.
  sort(slice_state_vec.begin(),
       slice_state_vec.end(),
       SliceState::SliceStatePtrOrderIncreasingSliceId);
  for (int ii = 0; ii < write_ret->primary().extents().size(); ++ii) {
    shared_ptr<EgroupOpsTester::Extent> extent_info =
      make_shared<EgroupOpsTester::Extent>();
    extent_info->mutable_extent_id()->CopyFrom(
      write_ret->primary().extents(ii).extent_id());
    extent_state_vec.emplace_back(extent_info);
  }

  // Issue a conversion op and make sure that it succeeds.
  ConvertEgroupToAES(
    egroup_id, disk_ids[0], 1 /* intent_sequence */, true /* is_primary */);
  ConvertEgroupToAES(
    egroup_id, disk_ids[1], 1 /* intent_sequence */, false /* is_primary */);

  // Issue a conversion op and make sure that it fails since the egroup is
  // already converted.
  ConvertEgroupToAES(egroup_id, disk_ids[0], 1 /* intent_sequence */,
                     true /* is_primary */,
                     StargateError::kUnexpectedIntentSequence);
  ConvertEgroupToAES(egroup_id, disk_ids[1], 1 /* intent_sequence */,
                     false /* is_primary */,
                     StargateError::kUnexpectedIntentSequence);

  if (checkpoint_before_recovery) {
    // Checkpoint the disk WAL.
    Notification checkpoint_notification;
    Function<void(bool)> ckpt_done_cb =
      bind(&CheckpointWALDone, _1, &checkpoint_notification);
    CheckpointDiskWAL(disk_ids[0], ckpt_done_cb);
    checkpoint_notification.Wait();
  }

  // Restart stargate to trigger a recovery.
  RestartStargateAtIndex(
    cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(disk_ids_[0]));

  // Wait for the recovery to complete.
  sleep(10);

  // Issue a read to the AES egroup and verify that it succeeds and matches
  // the written data.
  vector<RegionDescriptor> region_to_write_vec;
  region_to_write_vec.emplace_back(0, kRegionLength, 0);
  vector<Notification::Ptr> read_notification_vec;
  vector<pair<int64, int64>> region_to_read_vec;
  region_to_read_vec.emplace_back(make_pair(0, kRegionLength));
  slice_state_vec.clear();
  LOG(INFO) << "Issuing a read after conversion";
  ReadExtentGroup(egroup_id,
                  disk_ids[0],
                  read_notification_vec,
                  region_to_read_vec,
                  slice_state_vec,
                  extent_state_vec,
                  data,
                  region_to_write_vec[0].offset,
                  false /*compressed_slice*/,
                  true /*wait_for_read*/,
                  set<StargateError::Type>({StargateError::kNoError}),
                  true /* managed_by_aes */);
  data = Random::TL()->CreateRandomDataIOBuffer(kRegionLength);

  Notification notification;
  vector<shared_ptr<EgroupOpsTester::Extent>> extent_state_write;
  egroup_id_intent_seq_map_[egroup_id] = 1;
  // Issue a write to the AES egroup and ensure that it succeeds after
  // conversion.
  LOG(INFO) << "Issuing a write after conversion";
  WriteSingleExtentInExtentGroup(egroup_id,
                                 disk_ids_[0],
                                 disk_ids_[1],
                                 kVBlockNum,
                                 string() /* transformation_type */,
                                 vector<DataTransformation::Type>(),
                                 region_to_write_vec,
                                 &notification,
                                 &slice_state_vec,
                                 &extent_state_write,
                                 data,
                                 nullptr /* expected_error_map */,
                                 true /* wait_for_write */,
                                 -1 /* latest_applied_intent_sequence */,
                                 Optional<bool>() /* is_sequential */,
                                 true /* managed_by_aes */,
                                 1 /* global_metadata_intent_sequence */,
                                 false /* slices_stored_by_id */);
  slice_state_vec.clear();
  LOG(INFO) << "Issuing a read to verify last written data";
  ReadExtentGroup(egroup_id,
                  disk_ids[0],
                  read_notification_vec,
                  region_to_read_vec,
                  slice_state_vec,
                  extent_state_vec,
                  data,
                  region_to_write_vec[0].offset,
                  false /*compressed_slice*/,
                  true /*wait_for_read*/,
                  set<StargateError::Type>({StargateError::kNoError}),
                  true /* managed_by_aes */);
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::TestRaceWriteAndMedusaCheckOp() {
  LOG(INFO) << "Running " << __FUNCTION__;
  const vector<int64> disk_ids =
    { disk_ids_[0], disk_ids_[1]};
  const int64 egroup_id = GetNextEgroupId();
  const int32 kEphemeralDiscardTicks = 2;
  cluster_mgr_->stargate_mgr()->SetGFlag(
    "estore_ephemeral_state_discard_ticks", to_string(kEphemeralDiscardTicks));

  // Add the necessary arguments to the write request.
  shared_ptr<WriteExtentGroupArg> arg = WriteEgArg();
  arg->set_extent_group_id(egroup_id);
  arg->set_owner_vdisk_id(vdisk_id_);
  arg->set_owner_container_id(container_id_);
  for (const int64 disk_id : disk_ids) {
    arg->add_disk_ids(disk_id);
  }

  const int32 untransformed_slice_length =
    StargateUtil::GetSliceSizeForTransformation({ });
  arg->set_untransformed_slice_length(untransformed_slice_length);

  Notification notify;
  shared_ptr<WriteExtentGroupRet> write_ret;
  Function<void(StargateError::Type,
                shared_ptr<string>,
                shared_ptr<WriteExtentGroupRet>)> done_cb =
    [&notify, &arg, &write_ret](const StargateError::Type err,
                    const shared_ptr<string> err_detail,
                    const shared_ptr<WriteExtentGroupRet> ret) {

      CHECK(err == StargateError::kNoError)
        << "Write failed with " << err << " " << arg->ShortDebugString();
      write_ret = ret;
      notify.Notify();
    };
  func_disabler_.Wrap(&done_cb);

  vector<shared_ptr<EgroupOpsTester::SliceState>> first_write_slice_state;
  arg->set_intent_sequence(0);
  WriteExtentGroupArg::Primary *const primary_state = arg->mutable_primary();
  WriteExtentGroupArg::Primary::Extent *const ex =
    primary_state->add_extents();
  ExtentIdProto *const extent_id_proto = ex->mutable_extent_id();
  const int32 kVBlockNum = 0;
  extent_id_proto->set_vdisk_block(kVBlockNum);
  extent_id_proto->set_owner_id(vdisk_id_);
  ex->add_region_offset(0);
  ex->add_region_length(1048576);
  IOBuffer::Ptr data =
    Random::CreateRandomDataIOBuffer(1048576);
  LOG(INFO) << "Add first TU to egroup " << egroup_id;
  MakeTentativeUpdate(egroup_id, disk_ids[0], disk_ids[1], string(), { },
                      extent_id_proto, false /* managed_by_aes */);
  notify.Reset();
  const StargateInterface::Ptr& iface =
    cluster_mgr_->stargate_mgr()->GetStargateInterfaceForDisk(disk_ids[0]);
  CHECK(iface) << disk_ids[0];
  LOG(INFO) << "Perform write on egroup " << egroup_id << " "
            << arg->ShortDebugString();
  auto done_cb_copy = done_cb;
  iface->WriteExtentGroup(arg, data, move(done_cb_copy));
  notify.Wait();
  egroup_id_intent_seq_map_[egroup_id] = 0;
  for (const auto& slice : write_ret->primary().slices()) {
    shared_ptr<EgroupOpsTester::SliceState> slice_state =
      EgroupOpsTester::SliceState::Create(slice);
    if (slice_state->has_extent_group_offset()) {
      first_write_slice_state.emplace_back(slice_state);
    }
  }
  shared_ptr<EgroupOpsTester::Extent> first_write_extent_state =
    make_shared<EgroupOpsTester::Extent>();
  first_write_extent_state->CopyFrom(write_ret->primary().extents(0));
  FinalizeTentativeUpdate(egroup_id, 0, write_ret);

  vector<pair<int32, int32>> write_offset_size_vec =
    { { 0, 4096 }, { 8192, 4096 } };
  for (int xx = 0; xx < 2; ++xx) {
    arg->set_intent_sequence(xx + 1);
    arg->set_latest_applied_intent_sequence(0);
    arg->set_expected_intent_sequence(xx);
    arg->clear_primary();
    WriteExtentGroupArg::Primary *const primary_state = arg->mutable_primary();
    WriteExtentGroupArg::Primary::Extent *const ex =
      primary_state->add_extents();
    ExtentIdProto *const extent_id_proto = ex->mutable_extent_id();
    const int32 kVBlockNum = 0;
    extent_id_proto->set_vdisk_block(kVBlockNum);
    extent_id_proto->set_owner_id(vdisk_id_);
    const pair<int32, int32>& write_off_size = write_offset_size_vec[xx];
    data = Random::CreateRandomDataIOBuffer(write_off_size.second);
    ex->add_region_offset(write_off_size.first);
    ex->add_region_length(write_off_size.second);
    ex->mutable_data_location()->CopyFrom(
      first_write_extent_state->diff_data_location());
    ex->mutable_extent_id()->CopyFrom(first_write_extent_state->extent_id());
    primary_state->clear_slices();
    for (const auto& slice_state : first_write_slice_state) {
      slice_state->CopyTo(primary_state->add_slices());
    }

    LOG(INFO) << "Stack TU on egroup " << egroup_id;
    AddTentativeUpdate(egroup_id, false /* managed_by_aes */, nullptr,
                       extent_id_proto);
    CHECK(write_ret);
    if (xx > 0) {
      arg->set_earliest_tentative_intent_sequence(xx);
      LOG(INFO) << "Finalize previous TU on egroup " << egroup_id;
      FinalizeTentativeUpdate(egroup_id, xx, write_ret);
      sleep(5 * kEphemeralDiscardTicks);
    }
    notify.Reset();
    LOG(INFO) << "Perform write on egroup " << egroup_id << " "
              << arg->ShortDebugString();
    done_cb_copy = done_cb;
    iface->WriteExtentGroup(arg, data, move(done_cb_copy));
    notify.Wait();
    egroup_id_intent_seq_map_[egroup_id] = arg->intent_sequence();
  }

  CHECK(write_ret);
  vector<shared_ptr<EgroupOpsTester::SliceState>> slice_state_vec;
  for (const auto& slice : write_ret->primary().slices()) {
    shared_ptr<EgroupOpsTester::SliceState> slice_state =
      EgroupOpsTester::SliceState::Create(slice);
    if (slice_state->has_extent_group_offset()) {
      slice_state_vec.emplace_back(slice_state);
    }
  }
  sort(slice_state_vec.begin(), slice_state_vec.end(),
       SliceState::SliceStatePtrOrderIncreasingSliceId);
  ReplicaReadEgroupAndVerify(egroup_id, disk_ids[0], &slice_state_vec);
  ReplicaReadEgroupAndVerify(egroup_id, disk_ids[1], &slice_state_vec);
  cluster_mgr_->stargate_mgr()->SetGFlag(
    "estore_ephemeral_state_discard_ticks", "60");
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::WriteExtentGroupDone(
  StargateError::Type err,
  shared_ptr<string> err_detail,
  shared_ptr<WriteExtentGroupRet> ret,
  Notification *notify) {

  CHECK_EQ(err, StargateError::kNoError);
  if (notify) {
    notify->Notify();
  }
}

//-----------------------------------------------------------------------------

static void FetchEgroupsDone(
  const StargateError::Type err,
  const shared_ptr<string>& err_detail,
  const shared_ptr<FetchEgroupsRet>& ret,
  const IOBuffer::Ptr& buf,
  FetchEgroupsRet *egroups_ret,
  string *cookie,
  Notification *const notification) {

  CHECK_EQ(err, StargateError::kNoError);
  LOG(INFO) << "Received FetchEgroups response from disk ";
  egroups_ret->CopyFrom(*ret);
  *cookie = egroups_ret->cookie();
  notification->Notify();
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::FetchEgroups(
  const int64 disk_id,
  FetchEgroupsRet *const fetch_egroups_ret,
  string *cookie) {

  StargateInterface::Ptr iface =
    cluster_mgr_->stargate_mgr()->GetStargateInterfaceForDisk(disk_id);
  CHECK(iface);

  shared_ptr<FetchEgroupsArg> arg = make_shared<FetchEgroupsArg>();
  arg->set_disk_id(disk_id);
  if (!cookie->empty()) {
    arg->set_cookie(*cookie);
  }
  arg->set_qos_principal_name(string());
  arg->set_memory_state_only(false);
  arg->set_qos_priority(StargateQosPriority::kDefault);

  // Save a copy to the configuration as recommended in stargate.h.
  Configuration::PtrConst config = cluster_mgr_->Config();

  Notification notification;
  Function<void(StargateError::Type,
                  shared_ptr<string>,
                  shared_ptr<FetchEgroupsRet>,
                  IOBuffer::Ptr&&)> done_cb =
  bind(&FetchEgroupsDone, _1, _2, _3, _4, fetch_egroups_ret, cookie,
       &notification);
  LOG(INFO) << "Fetching egroups from disk " << disk_id
            << " with cookie " << *cookie;
  iface->FetchEgroups(arg, move(done_cb));
  notification.Wait();
  LOG(INFO) << "Fetched egroups from disk " << disk_id;
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::MigrateEgroups(
  const int64 vdisk_id,
  const int stargate_index,
  const int64 egroup_id,
  const vector<int64> desired_replica_disk_ids) {


  const StargateInterface::Ptr& ifc =
    cluster_mgr_->StargateInterface(stargate_index);

  shared_ptr<FixExtentGroupsArg> arg = make_shared<FixExtentGroupsArg>();
  arg->set_vdisk_id(vdisk_id);
  FixExtentGroupsArg::ExtentGroup *eg = arg->add_extent_groups();
  eg->set_extent_group_id(egroup_id);
  eg->set_migration_reason(MedusaExtentGroupIdMapEntryProto::kILM);
  for (const int64 disk_id : desired_replica_disk_ids) {
    eg->add_desired_replica_disk_ids(disk_id);
  }

  Notification notification;
  Function<void(StargateError::Type,
                shared_ptr<string>,
                shared_ptr<FixExtentGroupsRet>)> done_cb =
    bind(&EgroupOpsTest::MigrateEgroupsDone, this,
         _1, _2, _3, arg, &notification);

  ifc->FixExtentGroups(arg, done_cb);
  notification.Wait();
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::MigrateEgroupsDone(
  StargateError::Type err,
  const shared_ptr<string>& err_detail,
  const shared_ptr<FixExtentGroupsRet>& ret,
  const shared_ptr<FixExtentGroupsArg>& arg,
  Notification *notification) {

  CHECK_EQ(err, StargateError::kNoError);
  CHECK_EQ(ret->err_status_size(), arg->extent_groups_size());
  for (int ii = 0; ii < arg->extent_groups_size(); ++ii) {
    LOG(INFO) << ret->err_status(ii);
    LOG(INFO) << "Successfully migrated egroup";
  }

  notification->Notify();
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::WriteAndVerifyDiskUsage(
  const int64 egroup_id, const int64 write_size_bytes, const int64 num_vblocks,
  const bool overwrites, const bool compression, const int32 num_regions,
  const bool replacement_slices) {

  LOG(INFO) << __FUNCTION__ << " egroup_id=" << egroup_id
            << ", write_size_bytes=" << write_size_bytes << ", num_vblocks="
            << num_vblocks << boolalpha << ", overwrites=" << overwrites
            << ", compression=" << compression << ", num_regions="
            << num_regions;

  // Generate the required bytes of data.
  IOBuffer::Ptr data = make_shared<IOBuffer>();
  data->Clear();
  auto sub = Random::CreateRandomDataIOBuffer(write_size_bytes);

  // Calculate the usage before performing the write. The test expects the
  // background disk usage op to be paused so that the usage isn't skewed while
  // the write is being performed.
  SpaceUsage usage(
    cluster_mgr_, 2 /* stats_wait_secs */, false /* need_sleep */);
  CHECK_GT(container_id_, 0);
  usage.RegisterContainerId(container_id_);
  LOG(INFO) << "Computing the disk usage before performing the write op for "
            << "primary disk " << disk_ids_[0] << " and secondary disk "
            << disk_ids_[1];
  const int64 usage_disk1 = usage.GetUsage(disk_ids_[0]).first;
  const int64 usage_disk2 = usage.GetUsage(disk_ids_[1]).first;
  int64 metadata_usage_disk1 = usage.GetMetadataUsage(disk_ids_[0]);
  int64 metadata_usage_disk2 = usage.GetMetadataUsage(disk_ids_[1]);
  const int64 garbage_before_write_disk1 =
    usage.GetGarbage(disk_ids_[0]).first;

  // Check if the primary and secondary usage is the same for both the
  // replicas. This is achievable if the usage verification is done at the
  // beginning of the test.
  CHECK_EQ(
    usage_disk1 - metadata_usage_disk1, usage_disk2 - metadata_usage_disk2);

  // For the simplicity of the test, we only consider 1MiB as sequential and we
  // set this in the RPC so that the fallocation rules are honored.
  const bool is_sequential = (write_size_bytes == 1 * 1024 * 1024);

  // In order to verify garbage fallocation rules, let us consider the
  // following gflags based on the current logic in stargate extent store. Any
  // change in the fallocation logic mentioned below will result in the test to
  // fail.
  // 1. Default: 1MB (stargate_extent_group_fallocate_increment_MB)
  // 2. Sequential new write:
  // 4MB(stargate_initial_sequential_write_extent_group_size_MB)
  // 3. Random, uncompressed and SSD: 256KB
  // (stargate_ssd_random_write_extent_group_fallocate_increment_KB)
  // TODO (Sreejith): Change this to a model where fallocation alignment is
  // fetched from stargate. This can be done using stargate test interface.
  int64 default_fallocate_size = -1;
  if (is_sequential) {
    default_fallocate_size =
      stoll(GetStargateGFlag(
              "stargate_initial_sequential_write_extent_group_size_MB"))
        * 1024 * 1024LL;
  } else if (IsSSDDisk(disk_ids_[0]) && !compression) {
    default_fallocate_size =
      stoll(GetStargateGFlag(
              "stargate_ssd_random_write_extent_group_fallocate_increment_KB"))
        * 1024LL;
  } else {
    default_fallocate_size =
      stoll(GetStargateGFlag(
              "stargate_extent_group_fallocate_increment_MB")) * 1024 * 1024LL;
  }

  // Construct the write arguments.
  int32 vblock_num = 10;
  int64 new_bytes_written_so_far = 0;
  int64 fallocated_size = default_fallocate_size;
  const int64 slice_size = FLAGS_estore_regular_slice_size * 1LL;
  for (int ii = 0; ii < num_vblocks; ++ii) {
    // Create the RPC with the new egroup.
    auto arg = WriteEgArg();
    arg->set_extent_group_id(egroup_id);
    arg->add_disk_ids(disk_ids_[0]);
    arg->add_disk_ids(disk_ids_[1]);
    arg->set_managed_by_aes(
      FLAGS_egroup_test_estore_aes_enabled);
    arg->set_is_sequential(is_sequential);
    if (FLAGS_egroup_test_estore_aes_enabled) {
      arg->set_intent_sequence(-1);
      arg->set_global_metadata_intent_sequence(0);
      arg->set_slice_group_size(32);
      arg->set_slices_stored_by_id(false);
      arg->set_vdisk_incarnation_id(123);
    } else {
      arg->set_expected_intent_sequence(egroup_id_intent_seq_map_[egroup_id]);
      arg->set_intent_sequence(++egroup_id_intent_seq_map_[egroup_id]);
    }

    if (compression) {
      arg->set_transformation_type("c=low");
    }
    if (!overwrites) {
      vblock_num++;
    }
    auto primary = arg->mutable_primary();
    auto ex = primary->add_extents();
    auto eid = ex->mutable_extent_id();
    eid->set_vdisk_block(vblock_num);
    eid->set_owner_id(vdisk_id_);
    for (int jj = 0; jj < num_regions; ++jj) {
      ex->add_region_offset(jj * 32 * 1024);
      ex->add_region_length(write_size_bytes);
      data->AppendIOBuffer(sub.get());
      if (!overwrites || ii == 0) {
        new_bytes_written_so_far += max(write_size_bytes, slice_size);
        if (new_bytes_written_so_far > fallocated_size) {
          fallocated_size += default_fallocate_size;
        }
      }
    }
    const StargateInterface::Ptr& ifc =
      cluster_mgr_->stargate_mgr()->GetStargateInterfaceForDisk(disk_ids_[0]);
    CHECK(ifc);

    // Issue the Write RPC.
    Notification notif;
    const Function<void(StargateError::Type,
                        shared_ptr<string>,
                        shared_ptr<WriteExtentGroupRet>)> done_cb =
      func_disabler_.Bind(&EgroupOpsTest::WriteExtentGroupDone,
                          this, _1, _2, _3, &notif);
    WriteExtentGroup(arg, data, done_cb);
    notif.Wait();
    data->Clear();
  }

  // We have already reduced the time in which stargate stats get published to
  // arithmos - but sleep for few seconds before querying the disk stats again.
  sleep(10);
  LOG(INFO) << "Computing the disk usage after performing the write op for "
            << "primary disk " << disk_ids_[0] << " and secondary disk "
            << disk_ids_[1];
  const int64 garbage_disk1 = usage.GetGarbage(disk_ids_[0]).first;
  const int64 garbage_disk2 = usage.GetGarbage(disk_ids_[1]).first;
  const int64 updated_usage_disk1 = usage.GetUsage(disk_ids_[0]).first;
  const int64 updated_usage_disk2 = usage.GetUsage(disk_ids_[1]).first;
  metadata_usage_disk1 = usage.GetMetadataUsage(disk_ids_[0]);
  metadata_usage_disk2 = usage.GetMetadataUsage(disk_ids_[1]);

  // Check if the usage matches both primary and secondary replicas. While
  // there are cases where the replicas might have different usage, it is not
  // possible to get divergent usages here because the scans are disabled,
  // the fallocation up alignment is unmodified and there is no truncation.
  // Garbage for both the disks should be the same as well.
  CHECK_EQ(
    updated_usage_disk1 - metadata_usage_disk1,
    updated_usage_disk2 - metadata_usage_disk2);
  CHECK_EQ(garbage_disk1, garbage_disk2);

  const int64 expected_fallocated_garbage_disk1 =
    (fallocated_size - new_bytes_written_so_far) + garbage_before_write_disk1;
  int64 expected_garbage_disk1 = expected_fallocated_garbage_disk1;
  if (!compression && !replacement_slices) {
    CHECK_EQ(garbage_disk1, expected_garbage_disk1)
      << ", total_fallocate_size=" << fallocated_size
      << ", garbage_before_write_disk1=" << garbage_before_write_disk1
      << ", new_bytes_written_so_far=" << new_bytes_written_so_far
      << ", default_fallocate_size=" << default_fallocate_size;

    // Let us check the usage for both disks and see if they match with the
    // expected values based on the data that the test wrote. For compression
    // cases, we skip verifying the garbage since the garbage accounts the
    // cushion which the test is not aware of (unless it looks at the slice
    // state). The test makes sure that the garbage for disk 1 matches with the
    // garbage for disk 2 for both compressed and uncompressed cases.
    usage.LookupAndCompare(
      "primary-disk-usage-verification",
      usage_disk1 + fallocated_size /* total_bytes_transformed */,
      -1 /* total_bytes_untransformed */,
      usage_disk1 + new_bytes_written_so_far
      /* container_unshared_usage_transformed */,
      -1 /* unshared_bytes_untransformed */,
      expected_garbage_disk1 /* garbage_bytes_transformed */,
      -1 /* garbage_bytes_untransformed */,
       0 /* acceptable_deviation_bytes */,
      disk_ids_[0],
      true /* ignore_metadata_transformed_bytes */);
    usage.LookupAndCompare(
      "secondary-disk-usage-verification",
      usage_disk2 + fallocated_size /* total_bytes_transformed */,
      -1 /* total_bytes_untransformed */,
      usage_disk2 + new_bytes_written_so_far
        /* container_unshared_usage_transformed */,
      -1 /* unshared_bytes_untransformed */,
      -1 /* garbage_bytes_transformed */,
      -1 /* garbage_bytes_untransformed */,
      0 /* acceptable_deviation_bytes */,
      disk_ids_[1],
      true /* ignore_metadata_transformed_bytes */);
  }
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::TestPrimaryAndSecondaryDiskUsageCalculation() {
  LOG(INFO) << "Running " << __FUNCTION__;
  SetStargateGFlag(
    "stargate_disk_manager_use_unified_background_scan", "false");
  SetStargateGFlag(
    "estore_compute_secondary_disk_usage_from_primary", "false");
  int64 egroup_id = GetNextEgroupId();

  // Scenarios
  // 1. Writes with no transformation set.
  // 2. Writes with compression to check if cushion space is accounted
  //    correctly.

  //---------------------------------------------------------------------------
  // 1. Writes with no transformation set.
  //---------------------------------------------------------------------------
  egroup_id_intent_seq_map_[egroup_id] = -1;
  WriteAndVerifyDiskUsage(
    egroup_id,
    4 * 1024 /* write_size_bytes */,
    1 /* num_vblocks */,
    false /* overwrite */,
    false /* compression */);
  egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = -1;
  WriteAndVerifyDiskUsage(
    egroup_id,
    4 * 1024 /* write_size_bytes */,
    4 /* num_vblocks */,
    false /* overwrite */,
    false /* compression */);
  egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = -1;
  WriteAndVerifyDiskUsage(
    egroup_id,
    1 * 1024 * 1024 /* write_size_bytes */,
    1 /* num_vblocks */,
    false /* overwrite */);
  egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = -1;
  WriteAndVerifyDiskUsage(
    egroup_id,
    1 * 1024 * 1024 /* write_size_bytes */,
    4 /* num_vblocks */,
    false /* overwrite */);
  egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = -1;
  WriteAndVerifyDiskUsage(
    egroup_id,
    4 * 1024 /* write_size_bytes */,
    1 /* num_vblocks */,
    true /* overwrite */);
  egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = -1;
  WriteAndVerifyDiskUsage(
    egroup_id,
    4 * 1024 /* write_size_bytes */,
    4 /* num_vblocks */,
    true /* overwrite */);
  egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = -1;
  WriteAndVerifyDiskUsage(
    egroup_id,
    1 * 1024 * 1024 /* write_size_bytes */,
    1 /* num_vblocks */,
    true /* overwrite */);
  egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = -1;
  WriteAndVerifyDiskUsage(
    egroup_id,
    1 * 1024 * 1024 /* write_size_bytes */,
    4 /* num_vblocks */,
    true /* overwrite */);
  egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = -1;
  WriteAndVerifyDiskUsage(
    egroup_id,
    1 * 1024 * 1024 /* write_size_bytes */,
    1 /* num_vblocks */,
    true /* overwrite */);
  egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = -1;
  WriteAndVerifyDiskUsage(
    egroup_id,
    1 * 1024 * 1024 /* write_size_bytes */,
    4 /* num_vblocks */,
    true /* overwrite */);
  egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = -1;
  WriteAndVerifyDiskUsage(
    egroup_id,
    4 * 1024 /* write_size_bytes */,
    1 /* num_vblocks */,
    true /* overwrite */,
    false /* compression */,
    2 /* multiple-regions */);
  egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = -1;
  WriteAndVerifyDiskUsage(
    egroup_id,
    4 * 1024 /* write_size_bytes */,
    4 /* num_vblocks */,
    true /* overwrite */,
    false /* compression */,
    2 /* multiple-regions */);
  egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = -1;
  WriteAndVerifyDiskUsage(
    egroup_id,
    4 * 1024 /* write_size_bytes */,
    1 /* num_vblocks */,
    true /* overwrite */,
    false /* compression */,
    4 /* multiple-regions */);
  egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = -1;
  WriteAndVerifyDiskUsage(
    egroup_id,
    4 * 1024 /* write_size_bytes */,
    4 /* num_vblocks */,
    true /* overwrite */,
    false /* compression */,
    4 /* multiple-regions */);
  egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = -1;
  WriteAndVerifyDiskUsage(
    egroup_id,
    32 * 1024 /* write_size_bytes */,
    1 /* num_vblocks */,
    true /* overwrite */,
    false /* compression */,
    4 /* multiple-regions */);
  egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = -1;
  WriteAndVerifyDiskUsage(
    egroup_id,
    32 * 1024 /* write_size_bytes */,
    4 /* num_vblocks */,
    true /* overwrite */,
    false /* compression */,
    4 /* multiple-regions */);
  SetStargateGFlag(
    "estore_experimental_slice_replacement_inverse_prob", "1");
  sleep(2);
  egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = -1;
  WriteAndVerifyDiskUsage(
    egroup_id,
    4 * 1024 /* write_size_bytes */,
    1 /* num_vblocks */,
    false /* overwrite */,
    false /* compression */,
    1 /* num_regions */,
    true /* replacement_slices */);
  egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = -1;
  WriteAndVerifyDiskUsage(
    egroup_id,
    4 * 1024 /* write_size_bytes */,
    4 /* num_vblocks */,
    false /* overwrite */,
    false /* compression */,
    1 /* num_regions */,
    true /* replacement_slices */);
  egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = -1;
  WriteAndVerifyDiskUsage(
    egroup_id,
    32 * 1024 /* write_size_bytes */,
    4 /* num_vblocks */,
    true /* overwrite */,
    false /* compression */,
    4 /* multiple-regions */,
    true /* replacement_slices */);
  SetStargateGFlag(
    "estore_experimental_slice_replacement_inverse_prob", "0");

  //---------------------------------------------------------------------------
  // 2. Writes on compressed slices.
  //---------------------------------------------------------------------------
  egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = -1;
  WriteAndVerifyDiskUsage(
    egroup_id,
    4 * 1024 /* write_size_bytes */,
    1 /* num_vblocks */,
    false /* overwrite */,
    true /* compression */);
  egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = -1;
  WriteAndVerifyDiskUsage(
    egroup_id,
    4 * 1024 /* write_size_bytes */,
    4 /* num_vblocks */,
    false /* overwrite */,
    true /* compression */);
  egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = -1;
  WriteAndVerifyDiskUsage(
    egroup_id,
    1 * 1024 * 1024 /* write_size_bytes */,
    1 /* num_vblocks */,
    false /* overwrite */,
    true /* compression */);
  egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = -1;
  WriteAndVerifyDiskUsage(
    egroup_id,
    1 * 1024 * 1024 /* write_size_bytes */,
    4 /* num_vblocks */,
    false /* overwrite */,
    true /* compression */);
  egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = -1;
  WriteAndVerifyDiskUsage(
    egroup_id,
    4 * 1024 /* write_size_bytes */,
    1 /* num_vblocks */,
    true /* overwrite */,
    true /* compression */);
  egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = -1;
  WriteAndVerifyDiskUsage(
    egroup_id,
    4 * 1024 /* write_size_bytes */,
    4 /* num_vblocks */,
    true /* overwrite */,
    true /* compression */);
  egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = -1;
  WriteAndVerifyDiskUsage(
    egroup_id,
    1 * 1024 * 1024 /* write_size_bytes */,
    1 /* num_vblocks */,
    false /* overwrite */,
    true /* compression */);
  egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = -1;
  WriteAndVerifyDiskUsage(
    egroup_id,
    1 * 1024 * 1024 /* write_size_bytes */,
    4 /* num_vblocks */,
    false /* overwrite */,
    true /* compression */);
  egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = -1;
  WriteAndVerifyDiskUsage(
    egroup_id,
    4 * 1024 /* write_size_bytes */,
    1 /* num_vblocks */,
    true /* overwrite */,
    true /* compression */,
    2 /* multiple-regions */);
  egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = -1;
  WriteAndVerifyDiskUsage(
    egroup_id,
    4 * 1024 /* write_size_bytes */,
    4 /* num_vblocks */,
    true /* overwrite */,
    true /* compression */,
    2 /* multiple-regions */);
  egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = -1;
  WriteAndVerifyDiskUsage(
    egroup_id,
    4 * 1024 /* write_size_bytes */,
    1 /* num_vblocks */,
    true /* overwrite */,
    true /* compression */,
    4 /* multiple-regions */);
  egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = -1;
  WriteAndVerifyDiskUsage(
    egroup_id,
    4 * 1024 /* write_size_bytes */,
    4 /* num_vblocks */,
    true /* overwrite */,
    true /* compression */,
    4 /* multiple-regions */);
  SetStargateGFlag(
    "estore_experimental_slice_replacement_inverse_prob", "1");
  egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = -1;
  WriteAndVerifyDiskUsage(
    egroup_id,
    4 * 1024 /* write_size_bytes */,
    1 /* num_vblocks */,
    true /* overwrite */,
    true /* compression */,
    4 /* multiple-regions */,
    true /* replacement_slices */);
  egroup_id = GetNextEgroupId();
  egroup_id_intent_seq_map_[egroup_id] = -1;
  WriteAndVerifyDiskUsage(
    egroup_id,
    4 * 1024 /* write_size_bytes */,
    4 /* num_vblocks */,
    true /* overwrite */,
    true /* compression */,
    4 /* multiple-regions */,
    true /* replacement_slices */);
  SetStargateGFlag(
    "estore_experimental_slice_replacement_inverse_prob", "0");
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::ResetStargateBgScanGFlag() {
  // Reset stargate with bg scan gflag - This is required since the previous
  // tests could have disabled stargate background scan to check disk usage.
  const string use_unified_scan =
    (FLAGS_egroup_test_use_unified_background_scan ||
     FLAGS_egroup_test_estore_aes_enabled) ? "true " : "false ";
  cmdline_ +=
    StringJoin(" -stargate_disk_manager_use_unified_background_scan=",
               use_unified_scan);
  RestartStargate();
}

//-----------------------------------------------------------------------------

static void TestSpuriousDiskDirConfigWatchCb(
  Configuration::PtrConst new_config, const int64 disk_id, const int64 svm_id,
  shared_ptr<bool> spurious_disk_offline) {

  for (const auto& entry : new_config->disk_id_map()) {
    auto disk = entry.second;
    CHECK(disk->has_service_vm_id() || disk->disk_id() == disk_id)
      << OUTVARS(disk->ShortDebugString(), disk_id);

    if (disk->disk_id() == disk_id && disk->has_last_service_vm_id()) {
      CHECK_EQ(disk->last_service_vm_id(), svm_id)
        << disk->ShortDebugString();

      auto node = new_config->LookupNode(svm_id);
      CHECK_EQ(node->offline_disk_mount_paths_size(), 1)
        << node->ShortDebugString();
      CHECK_EQ(node->offline_disk_mount_paths(0), disk->mount_path())
        << node->ShortDebugString();
      *spurious_disk_offline = true;
      LOG(INFO) << "Spurious disk with id " << disk_id << " is marked offline";
    }
  }
  LOG(INFO) << "Updated zeus config, new_config: "
            << new_config->config_proto()->ShortDebugString();
}

//-----------------------------------------------------------------------------

void EgroupOpsTest::TestSpuriousDiskDir() {
  LOG(INFO) << "Running " << __FUNCTION__;

  const int index = 0;
  const string storage_dir = StargateManager::StargateStorageDir(
    StringJoin(cluster_mgr_->cdp_options().test_out_dir, "/data"), index);

  // Add some disks on the 0th CVM and leave one of them without a
  // disk_config.json. Stargate will mark that disk offline while scanning the
  // disk directory at startup.
  int64 spurious_disk_id = -1;
  for (int ii = 0; ii < 20; ++ii) {
    const int64 disk_id =
      zeus_->CreateAndMountDisk(
        1048576L /* size */,
        "SSD-PCIe" /* storage_tier */,
        cluster_mgr_->stargate_mgr()->GetSvmIdForStargate(index),
        StringJoin(storage_dir, "/disks/"));

    if (ii == 5) {
      spurious_disk_id = disk_id;
      LOG(INFO) << "Spurious disk id: " << spurious_disk_id;
      continue;
    }

    zeus_->Refresh();
    Configuration::PtrConst config = zeus_->ZeusConfig();
    auto disk = config->LookupDisk(disk_id);
    CHECK(disk) << disk_id;

    Json::Value root;
    root["disk_id"] = Json::Int(disk_id);
    root["disk_size"] = Json::Int(disk->disk_size());
    root["storage_tier"] = disk->storage_tier();
    ostringstream oss;
    oss << root;
    const string disk_config_path =
      StringJoin(disk->mount_path(), "/disk_config.json");
    FILE *ofp = fopen(disk_config_path.c_str(), "w");
    SYSCALL_CHECK(ofp);
    const string json_str = oss.str();
    SYSCALL_CHECK(fwrite(json_str.data(), json_str.size(), 1, ofp) == 1U);
    SYSCALL_CHECK(fflush(ofp) == 0);
    fclose(ofp);
  }

  shared_ptr<bool> spurious_disk_offline = make_shared<bool>(false);
  Function<void(const Configuration::PtrConst& config)> config_cb =
    bind(TestSpuriousDiskDirConfigWatchCb, _1, spurious_disk_id,
         cluster_mgr_->stargate_mgr()->GetSvmIdForStargate(index),
         spurious_disk_offline);
  cluster_mgr_->zeus_helper()->SetConfigChangeCB(config_cb);

  RestartStargateAtIndex(0, "" /* additional_flags */,
                         false /* wait_for_stargate_restart */);

  int secs = 0;
  while(*spurious_disk_offline == false) {
    if (secs >= 20) {
      LOG(FATAL) << "Mount path for disk " << spurious_disk_id
                 << " is expected to be marked offline!";
    }
    sleep(1);
    ++secs;
  }
  CHECK(*spurious_disk_offline);
}

//-----------------------------------------------------------------------------

int main(int argc, char *argv[]) {
cout<<endl<<endl<<"Test_Started"<<endl<<endl;

sleep(5);


  InitNutanix(&argc, &argv, InitOptions(true) /* default options */);

  FLAGS_stargate_manager_verbosity = 3;
  cout<<"****************************************************"<<FLAGS_egroup_test_sleep_on_finish_secs<<"****************************************************";
  sleep(5);
  string json_path =
    FLAGS_egroup_test_ec_ops ?
      "/cdp/server/qa/test/stargate/extent_store/egroup_ec_ops.json" :
      "/cdp/server/qa/test/stargate/extent_store/egroup_ops.json";
  if (FLAGS_egroup_test_estore_aes_hybrid_enabled) {
    json_path =
      FLAGS_egroup_test_ec_ops ?
        "/cdp/server/qa/test/stargate/extent_store/egroup_ec_ops_hybrid.json" :
        "/cdp/server/qa/test/stargate/extent_store/egroup_ops_hybrid.json";
  }

  EgroupOpsTest test_egroup_ops(json_path);
//   if (FLAGS_egroup_test_estore_aes_enabled) {
//     // This test needs to be the first one to run because it asserts that the
//     // disk usage is zero.
//     test_egroup_ops.TestDiskUsageOnDelete();cxc
//   }

//   if (FLAGS_egroup_test_disk_usage) {
//     test_egroup_ops.TestPrimaryAndSecondaryDiskUsageCalculation();
//   }

//   test_egroup_ops.ResetStargateBgScanGFlag();

//   if (!FLAGS_egroup_test_estore_aes_enabled) {
//     test_egroup_ops.TestRaceWriteAndMedusaCheckOp();
//   }

//   if (FLAGS_egroup_test_estore_aes_enabled && !FLAGS_use_block_store) {
//     test_egroup_ops.TestConversionOp(false /* checkpoint_before_recovery */);
//     test_egroup_ops.TestConversionOp(true /* checkpoint_before_recovery */);
//     test_egroup_ops.TestConversionInteropWithBgScan();
// #ifndef NDEBUG
//     test_egroup_ops.TestReplicateAESEgroupToNonAESZombie();
// #endif
//   }

  test_egroup_ops.TestReplicaReadEgroup(
    FLAGS_egroup_test_estore_aes_enabled /* managed_by_aes */);
//    test_egroup_ops.TestReplicateExtentGroup(
//     FLAGS_egroup_test_estore_aes_enabled /* managed_by_aes */);
//   test_egroup_ops.TestDeleteExtentGroup(
//     FLAGS_egroup_test_estore_aes_enabled /* managed_by_aes */);
//   test_egroup_ops.TestImmutableEgroupTruncated();

//   // TODO(ckamat): Move these tests out of the aes check once we have all AES
//   // paths implemented.
//   if (FLAGS_egroup_test_estore_aes_enabled && !FLAGS_egroup_test_ec_ops) {
//     test_egroup_ops.TestDeleteExtentGroupOnAesAndNonAesEgroups();
//     test_egroup_ops.TestDeleteAndReplicateForAESExtentGroup();
//     test_egroup_ops.TestNextSliceAllocationOffsetHintUpdate();
//     test_egroup_ops.TestSparseMultiRegionRead();
//     test_egroup_ops.TestOwnerVDiskChange();
//   } else if (FLAGS_egroup_test_estore_aes_enabled &&
//              FLAGS_egroup_test_ec_ops) {
//     test_egroup_ops.TestAESMigrateErasureCode();
//     test_egroup_ops.TestAESWriteParity(false /* inject_error */);
//     test_egroup_ops.TestAESErasureCoding(false /* inject_error */);
//     test_egroup_ops.TestAESErasureCoding(false /* inject_error */,
//                                          1024 /* replication_bytes */);
// #ifndef NDEBUG
//     test_egroup_ops.TestAESWriteParity(true /* inject_error */);
//     test_egroup_ops.TestAESErasureCoding(true /* inject_error */);
// #endif
//   } else if (FLAGS_egroup_test_ec_ops) {
//     test_egroup_ops.TestWriteExtentGroupAtPostWriteIntentSequence();
//     test_egroup_ops.TestRollbackEgroup();
//     test_egroup_ops.TestOverwriteErasureCodedEgroup();
//   } else {
//     test_egroup_ops.TestSyncEgroupSlices();
//     test_egroup_ops.TestTruncateEgroups();
//     test_egroup_ops.TestExtentStoreOverwrites(4096);
//     test_egroup_ops.TestExtentStoreDropOps();
//     test_egroup_ops.TestCompressedSliceReplacement();
//     // Test extent group read and write op for a single slice.
//     test_egroup_ops.TestSliceSubregionRead();
//     test_egroup_ops.TestSliceSubregionWrite();
//     test_egroup_ops.TestMultiSliceWrite();
//     test_egroup_ops.TestCompressedSlice();

//     test_egroup_ops.TestAbortOnRecovery(true /* first_update_tentative */);
//     test_egroup_ops.TestAbortOnRecovery(false /* first_update_tentative */);
//     test_egroup_ops.TestAbortOnRecoveryAllUpdatesRollback();

//     test_egroup_ops.TestAbortOnRecoveryWithCustomUpalignment();

//     test_egroup_ops.TestEGStateWithAbortedWrite();
//     test_egroup_ops.TestAbortedWrite();
//     test_egroup_ops.TestOutoforderWritesWithInterleavedRead();
//     test_egroup_ops.TestNonFinalizedCheckpointDuringReplication(
//       false /* crash_before_replica_finalization */);
//     test_egroup_ops.TestNonFinalizedCheckpointDuringReplication(
//       true /* crash_before_replica_finalization*/);
//   }
//   test_egroup_ops.TestMixedCompressionEgroup();

//   if (FLAGS_egroup_test_update_last_scrub_time) {
//     test_egroup_ops.TestUpdateLastScrubTime(
//       FLAGS_egroup_test_estore_aes_enabled);
//   }

//   test_egroup_ops.TestSpuriousDiskDir();

LOG(INFO) << "Test passed";
sleep(10);
  if (FLAGS_egroup_test_sleep_on_finish_secs > 0) {
    sleep(FLAGS_egroup_test_sleep_on_finish_secs);
  }

  return 0;
}

