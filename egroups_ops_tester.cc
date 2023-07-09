/*
 * Copyright (c) 2013 Nutanix Inc. All rights reserved.
 *
 * Author: partha@nutanix.com
 *
 */

#include "cdp/client/stargate/stargate_base/stargate_parity.h"
#include "pithos/pithos.h"
#include "qa/test/stargate/extent_store/egroup_ops_tester.h"
#include "qa/test/stargate/util/space_usage.h"
#include "util/base/arena.h"
#include "util/base/iobuffer_util.h"
#include "util/base/popen_executor.h"
#include "util/base/sha1.h"
#include "util/base/shell_util.h"
#include "util/thread/callback_waiter.h"

DEFINE_string(container, "Container2",
              "Name of the container used for testing certain test cases.");

DEFINE_int32(egroup_test_chunk_size, 4096,
             "Chunk size to write data into the extent group");

DEFINE_bool(egroup_test_estore_aio_enabled, true,
            "Flag to configure Stargate's 'estore_aio_enabled' flag.");

DEFINE_int32(egroup_test_sleep_on_finish_secs, -1,
             "If > 0, this is the time in seconds to sleep after all tests "
             "finish.");

DEFINE_int32(estore_checksum_subregion_size, 8 * 1024,
             "Size of a subregion in a slice over which the checksum "
             "computation happens.");

DEFINE_int32(egroup_test_write_eg_rpc_timeout, -1,
             "The timeout for WriteExtentGroup RPC specified for the user in "
             "seconds. Used for testing purposes for AWS. A value of -1 "
             "implies that the default value in proto is used.");

DEFINE_int32(egroup_test_replica_read_eg_rpc_timeout, -1,
             "The timeout for ReplicaReadEgroup RPC specified for the user in "
             "seconds. Used for testing purposes for AWS. A value of -1 "
             "implies that the default value in proto is used.");

DEFINE_string(egroup_encryption_type, "",
              "Encryption type to used in extent store. One of e=aes-256-ctr "
              "or e=aes-256-xts.");

DEFINE_bool(egroup_test_estore_aes_enabled, false,
            "Flag to configure Stargate's 'stargate_aes_enabled' flag.");

DEFINE_bool(egroup_test_estore_aes_hybrid_enabled, false,
            "Flag to configure Stargate's 'stargate_hybrid_config_aes_enabled'"
            " flag.");

DEFINE_bool(egroup_test_estore_advanced_error_handling_enabled, false,
            "Flag to configure Stargate's "
            "'stargate_aesdb_advanced_error_handling_enabled' flag.");

DEFINE_bool(use_block_store, false,
            "If true, the disks use a block store to manage their storage.");

// Some tests use smaller number of sub directories so that scrubber op
// can finish scanning the entire extent store faster.
DEFINE_int32(data_dir_sublevels,
             2,
             "Number of sub-levels used under the data directory on each "
             "disk.");
DEFINE_int32(data_dir_sublevel_dirs,
             20,
             "Number of sub-directories at each sub-level used under the "
             "data directory on each disk.");

DEFINE_bool(egroup_test_rocksdb_kv_store_use_fibers, true,
            "If true, RocksDB based KV store uses async IO and fibers.");

DEFINE_bool(egroup_test_use_unified_background_scan, false,
            "Whether to use the unified background scan for all the "
            "background scan ops.");

DEFINE_bool(egroup_test_ec_ops, false,
            "Whether the test needs to issue ec operations.");

DEFINE_int32(egroup_test_num_nodes, -1,
             "If positive, the number of nodes in the cluser.");

DEFINE_int32(egroup_test_num_cassandra_nodes, -1,
             "If positive, the number of cassandra nodes in the cluser.");

DEFINE_bool(egroup_test_fb_wal, false,
            "If true, use flatbuffers as the backing for extent store's WAL, "
            "else use protobufs.");

DEFINE_bool(egroup_test_s3_kv_store_counter, false,
            "If true, enable counters which access S3KVStore APIs.");

DEFINE_bool(egroup_test_migrate_corrupt_replica, false,
            "Test to check corrupt replicas are all migrated during "
            "hibernation and restore.");

DEFINE_bool(egroup_test_use_old_fmt_aesdb_dir_name, false,
            "If true, use old format AESDB directory name 'aesdb'. Use new "
            "format 'aesdb_[disk_id]' otherwise.");

DEFINE_bool(egroup_test_aes_hybrid_upgrade, false,
            "If true, tests cluster upgrade scenarios to a version supporting "
            "AES Hybrid.");

DEFINE_int32(egroup_test_aesdb_open_max_attempts, 4,
             "Maximum number of attempts that the test makes to open aesdb "
             "on a disk.");

DEFINE_int32(egroup_test_force_kvstore_type, -1,
             "Flag to force the use of the specified KV store as the default "
             "AESDB. Valid values are: -1 - Not forced, 0 - rocksdb, "
             "1 - b-tree.");

DEFINE_bool(test_extent_based_metadata, false,
            "Whether to test with extent based metadata enabled.");



DEFINE_bool(make_the_slices_corrupt_, false,
            "make_the_slices_corrupt");



DECLARE_int32(stargate_ideal_extent_group_size_MB);
DECLARE_int32(stargate_migrate_extents_bad_compression_pct);
DECLARE_int32(estore_slice_alignment_size);
DECLARE_int32(estore_regular_slice_size);
DECLARE_string(flatbuffer_schema_path);
DECLARE_int64(stargate_experimental_fail_updates_for_egroup_id);
DECLARE_bool(medusa_enable_chakrdb_backend);

using namespace nutanix;
using namespace nutanix::kv_store;
using namespace nutanix::medusa;
using namespace nutanix::misc;
using namespace nutanix::pithos;
using namespace nutanix::stargate;
using namespace nutanix::stargate::extent_store;
using namespace nutanix::stargate::interface;
using namespace nutanix::stats;
using namespace nutanix::test;
using namespace nutanix::thread;
using namespace nutanix::tools::util::connectors;
using namespace nutanix::zeus;

using namespace std;
using namespace std::placeholders;

//-----------------------------------------------------------------------------

namespace nutanix { namespace stargate { namespace test {

//-----------------------------------------------------------------------------

EgroupOpsTester::EgroupOpsTester(
  const string& config_filename) {

  // TODO (raghav) - Fix Block store based tests to comply with VFS based
  // Env for AESDB.
  if (FLAGS_use_block_store) {
    LOG(INFO) << "Block store is enabled, disabling VFS based Env for AESDB";
  }

  cmdline_ =
    StringJoin("-stargate_disk_manager_aesdb_use_vfs_env=",
               FLAGS_use_block_store ? "false " : "true ",
               "-estore_aio_enabled=",
               FLAGS_egroup_test_estore_aio_enabled ? "true " : "false ",
               "-stargate_aes_enabled=",
               FLAGS_egroup_test_estore_aes_enabled ? "true " : "false ",
               "-estore_experimental_use_old_fmt_aesdb_dir_name=",
               FLAGS_egroup_test_use_old_fmt_aesdb_dir_name ?
                 "true " : "false ",
               "-stargate_hybrid_config_aes_enabled=",
               FLAGS_egroup_test_estore_aes_hybrid_enabled ?
                 "true " : "false ",
               "-stargate_aesdb_advanced_error_handling_enabled=",
               FLAGS_egroup_test_estore_advanced_error_handling_enabled ?
                 "true " : "false ",
               "-stargate_aesdb_use_fibers_aio=",
               FLAGS_egroup_test_rocksdb_kv_store_use_fibers ?
                 "true " : "false ",
               "-estore_experimental_bg_scan_skip_in_memory_aes_egroups=true ",
               "-estore_WAL_use_flatbuffers=",
               FLAGS_egroup_test_fb_wal ? "true " : "false ",
               "-estore_disk_overfull_protection_enabled=true ",
               "-filesystem_WAL_backend_max_buffered_data_bytes=0 ",
               "-stargate_experimental_use_hard_coded_keys=true ",
               "-stargate_nfs_adapter_file_size_threshold=0 "
               "-estore_experimental_verify_payload_data_checksums=true "
               "-estore_experimental_background_scan_pause_processing=true ",
               "-stargate_stats_dump_interval_secs=1 ",
               "-stats_flush_interval_secs=1 ",
               "-v=4 ");

  if (FLAGS_use_block_store) {
    cmdline_ += StringJoin(
      " --estore_experimental_force_enable_block_store=true"
      " --stargate_aes_enable_on_all_containers=true"
      " --stargate_aes_disable_constraint_checks=true");
  } else if (FLAGS_egroup_test_estore_aes_hybrid_enabled) {
    CHECK(FLAGS_egroup_test_estore_aes_enabled);
    cmdline_ += StringJoin(
      " --stargate_aes_enable_on_all_containers=true",
      " --stargate_aes_disable_constraint_checks=true",
      " --stargate_disk_manager_stale_aesdb_directory_check_interval_secs=0",
      " --aesdb_error_stat_window_secs=3000",
      " --aesdb_restart_stat_window_secs=3000",
      " --aesdb_destroy_stat_window_secs=3000",
      " --stargate_disk_manager_stale_aesdb_directory_check_enabled=true");
  } else if (FLAGS_egroup_test_s3_kv_store_counter) {
    const string counters_config_path =
      StringJoin(CHECK_NOTNULL(getenv("TOP")),
                 "/build/cdp/server/counters/counter_families.json ");
    cmdline_ += StringJoin(
      " --counters_enabled=true"
      " --counters_config=", counters_config_path,
      " --stargate_counters_default_dump_interval_secs=1");
  } else if (FLAGS_egroup_test_migrate_corrupt_replica) {
    cmdline_ += StringJoin(
      " --estore_data_dir_sublevel_dirs=", FLAGS_data_dir_sublevel_dirs,
      " --estore_data_dir_sublevels=", FLAGS_data_dir_sublevels,
      " --estore_egroup_scrubber_op_experimental_skip_fixer_op=true");
  } else if (FLAGS_egroup_test_estore_aio_enabled) {
    cmdline_ += " --estore_aio_executor_max_outstanding_requests=1024";
  }

  // Enable Summary View CF in RocksDB.
  cmdline_ += StringJoin(
    " --stargate_enable_summary_view_in_aesdb=true",
    " --estore_experimental_validate_summary_view_creation=true");

  // Allow user-defined EC configuration on a cluster which doesn't have
  // enough nodes to support such configuration, e.g. 3/2 strips on a 5
  // node cluster.
  if (FLAGS_egroup_test_ec_ops) {
    cmdline_ += " --erasure_code_skip_strip_size_validation=true";
  }

  CdpCompositeClusterManager::Options options;
  if (FLAGS_egroup_test_num_nodes > 0) {
    options.nodes = FLAGS_egroup_test_num_nodes;
    CHECK_GT(FLAGS_egroup_test_num_cassandra_nodes, 0);
    options.ndb_cluster_mgr_opts.cassandra_nodes =
      FLAGS_egroup_test_num_cassandra_nodes;
  } else if (FLAGS_egroup_test_ec_ops) {
    options.nodes = 5;
    options.ndb_cluster_mgr_opts.cassandra_nodes = 5;
  } else {
    options.nodes = 3;
  }
  options.abort_on_failure = false;
  options.cdp_cluster_mgr_opts.start_stargates = true;

  if (FLAGS_medusa_enable_chakrdb_backend) {
    options.cdp_cluster_mgr_opts.start_nikes = true;
  }

  options.cdp_cluster_mgr_opts.start_curators = false;
  options.cdp_cluster_mgr_opts.start_chronos_nodes = false;
  options.cdp_cluster_mgr_opts.nfs_adapter_enabled = true;
  options.ndb_cluster_mgr_opts.initial_nutanix_config =
    StringJoin(CHECK_NOTNULL(getenv("TOP")), config_filename);
  options.abort_on_failure = false;

  // We keep state of stargate command line arguments since we restart stargate
  // at one point in the test. We need to ensure that we set the gflags for all
  // nodes in the cluster.
  options.cdp_cluster_mgr_opts.stargate_cmdline_args = cmdline_;
  cluster_mgr_ = make_shared<CdpCompositeClusterManager>(options);
  LOG(INFO) << "Starting cluster...";
  cluster_mgr_->Start();

  // Establish a NFS connector for the cluster. While most of the test cases
  // invoke the ExtentStore RPC directly, some may require end-to-end testing.
  zeus_ = make_shared<ZeusConnector>();
  medusa_connector_ = make_shared<MedusaConnector>(zeus_);
  pithos_ = make_shared<PithosConnector>(zeus_);
  nfs_connector_ =
    make_shared<NfsConnector>(
      make_shared<EventDriver>(), "127.0.0.1" /* nfs_master_ip */,
      cluster_mgr_->stargate_mgr()->stargate_nfs_port(0),
      cluster_mgr_->stargate_mgr()->stargate_ifc(0),
      360 * 1000 /* onc_rpc_timeout_msecs */);
  sleep(15);

  // Initialize stats controller.
  StatsCassandraBackend::Options option(false, cluster_mgr_->zeus());
  option.cassandra_host_ip =
    cluster_mgr_->cassandra_mgr()->cassandra_host();
  option.cassandra_port =
    cluster_mgr_->cassandra_mgr()->cassandra_proto_port();
  StatsCassandraBackend::Ptr backend =
    make_shared<StatsCassandraBackend>(option);
  disk_usage_stats_ =
    make_shared<StatsController<DiskUsageStatProto>>(
      vector<StatsControllerBackend::Ptr>(1, backend), "disk_usage");

  // Fill up the disk Ids from cluster manager.
  PopulateDiskIds();

  // Check zeus configuration meets EC test requirements.
  CheckConfigForErasureCode();

  container_id_ = cluster_mgr_->Config()->container_id_map().begin()->first;
  const string vdisk_name =
    StringJoin("vdisk-", zeus_->FetchNextComponentId());
  vdisk_id_ = pithos_->CreateVDisk(
    vdisk_name, 1024 * 1024 /* size */, container_id_, false /* dedup */);

  const char *const top_dir = CHECK_NOTNULL(getenv("TOP"));
  CHECK(top_dir);
  FLAGS_flatbuffer_schema_path =
    StringJoin(top_dir, "/build/flatbuffers/schema/");
}

EgroupOpsTester::~EgroupOpsTester() {
  // Teardown the zeus connector object first, before the cluster manager
  // stops the zookeeper server. Otherwise, the zeus connector can continue
  // to establish connection with the server even after the latter was stopped
  // by the cluster manager, thereby leading to QFATALs in zookeeper session
  // establishment.
  zeus_.reset();
  cluster_mgr_->Stop();
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::AESHybridSwapDisks() {
  Configuration::PtrConst config = cluster_mgr_->Config();

  unordered_set<int64> inserted_svm_ids;
  uint insert_index = 0;
  for (uint ii = 0; ii < disk_ids_.size(); ++ii) {
    const int64 svm_id = config->LookupDisk(disk_ids_[ii])->service_vm_id();

    if (IsHDDDisk(disk_ids_[ii]) &&
        inserted_svm_ids.find(svm_id) == inserted_svm_ids.end()) {
      swap(disk_ids_[insert_index++], disk_ids_[ii]);
      inserted_svm_ids.insert(svm_id);
    }
  }
  CHECK(IsHDDDisk(disk_ids_[0]));
  CHECK(IsHDDDisk(disk_ids_[1]));
}

//-----------------------------------------------------------------------------

bool EgroupOpsTester::IsSSDDisk(const int64 disk_id) {
  Configuration::PtrConst config = cluster_mgr_->Config();
  unordered_map<int64, const ConfigurationProto::Disk *>::const_iterator it =
    config->disk_id_map().find(disk_id);
  CHECK(it != config->disk_id_map().end()) << disk_id;
  return (it->second->storage_tier() == "SSD-SATA" ||
          it->second->storage_tier() == "SSD-PCIe");
}

//-----------------------------------------------------------------------------

bool EgroupOpsTester::IsHDDDisk(const int64 disk_id) {
  Configuration::PtrConst config = cluster_mgr_->Config();
  unordered_map<int64, const ConfigurationProto::Disk *>::const_iterator it =
    config->disk_id_map().find(disk_id);
  CHECK(it != config->disk_id_map().end()) << disk_id;
  return (it->second->storage_tier() == "DAS-SATA");
}

//-----------------------------------------------------------------------------

static void FilterUnAllocatedSlices(
  vector<SliceState::Ptr> *slice_state_vec) {

  CHECK(slice_state_vec);
  vector<SliceState::Ptr> filtered_slice_state_vec;
  for (uint ii = 0; ii < slice_state_vec->size(); ++ii) {
    if (slice_state_vec->at(ii)->has_extent_group_offset()) {
      filtered_slice_state_vec.push_back(slice_state_vec->at(ii));
    }
  }
  *slice_state_vec = move(filtered_slice_state_vec);
}

//-----------------------------------------------------------------------------

static void WriteExtentGroupDone(
  StargateError::Type err,
  shared_ptr<string> err_detail,
  shared_ptr<WriteExtentGroupRet> ret,
  Notification *notify,
  vector<shared_ptr<EgroupOpsTester::SliceState>> *slice_state_vec,
  vector<shared_ptr<EgroupOpsTester::Extent>> *extent_state_vec,
  map<StargateError::Type, int> *expected_errors,
  IOBuffer::Ptr data_written,
  const bool managed_by_aes,
  const bool is_primary) {

  if (!expected_errors) {
    CHECK_EQ(err, StargateError::kNoError);
  } else if (err == StargateError::kNoError) {
    ++(*expected_errors)[StargateError::kNoError];
  } else {
    auto it = expected_errors->find(err);
    if (it != expected_errors->end()) {
      ++it->second;
    } else {
      LOG(ERROR) << "Unexpected err: " << err;
    }
    LOG(INFO) << "WriteExtentGroup RPC finished with error " << err;
  }

  if (err == StargateError::kNoError && !managed_by_aes) {
    CHECK(!is_primary || ret->has_primary());
    if (slice_state_vec) {
      slice_state_vec->clear();

      // Copy the slice state information from the response.
      for (int ii = 0; ii < ret->primary().slices().size(); ++ii) {
        for (int jj = 0; jj < ret->primary().slices(ii).checksum_size();
             ++jj) {
          VLOG(1) << "Checksum for slice id "
                  << ret->primary().slices(ii).slice_id() << " at subregion "
                  << jj << " is " << ret->primary().slices(ii).checksum(jj);
        }
        // Return both the allocated and un-allocated slices to the caller. It
        // is the callers responsibility to filter out un-allocated slices from
        // any subsequent read requests.
        shared_ptr<EgroupOpsTester::SliceState> slice_state =
          EgroupOpsTester::SliceState::Create(ret->primary().slices(ii));
        slice_state_vec->emplace_back(slice_state);
      }
      // Sort the slice states in the order of slice ids.
      sort(slice_state_vec->begin(), slice_state_vec->end(),
           SliceState::SliceStatePtrOrderIncreasingSliceId);
    }

    // Copy the extent information from the response. This includes the slice
    // index and the data location.
    if (extent_state_vec) {
      for (int ii = 0; ii < ret->primary().extents().size(); ++ii) {
        shared_ptr<EgroupOpsTester::Extent> extent_info =
          make_shared<EgroupOpsTester::Extent>();
        extent_info->CopyFrom(ret->primary().extents(ii));
        extent_state_vec->emplace_back(extent_info);
      }
    }
  }

  // Notify the caller.
  if (notify) {
    notify->Notify();
  }
}

//-----------------------------------------------------------------------------

static void CopyAESMetadata(
  const shared_ptr<ReplicaReadEgroupRet>& remote_ret,
  vector<shared_ptr<EgroupOpsTester::SliceState>> *slice_state_vec,
  vector<shared_ptr<EgroupOpsTester::Extent>> *extent_state_vec) {

  const MedusaExtentGroupIdMapEntryProto& egid_proto =
    remote_ret->extent_group_metadata();
  for (int ii = 0; ii < egid_proto.slices().size(); ++ii) {
    shared_ptr<EgroupOpsTester::SliceState> slice_state =
      EgroupOpsTester::SliceState::Create(egid_proto.slices(ii));
    slice_state_vec->emplace_back(slice_state);
  }

  if (!extent_state_vec) {
    return;
  }
  for (int ii = 0; ii < egid_proto.extents().size(); ++ii) {
    shared_ptr<EgroupOpsTester::Extent> extent_info =
      make_shared<EgroupOpsTester::Extent>();
    extent_info->mutable_extent_id()->CopyFrom(
      egid_proto.extents(ii).extent_id());
    auto *diff_data_location = extent_info->mutable_diff_data_location();
    const auto& extent = egid_proto.extents(ii);
    for (int ii = 0; ii < extent.slice_ids_size(); ++ii) {
      diff_data_location->add_slice_ids(extent.slice_ids(ii));
    }
    for (int ii = 0; ii < extent.slice_indices_size(); ++ii) {
      diff_data_location->add_slice_indices(extent.slice_indices(ii));
    }
    if (extent.has_first_slice_offset()) {
      diff_data_location->set_first_slice_offset(
        extent.first_slice_offset());
    }
    extent_state_vec->emplace_back(extent_info);
  }
}

//-----------------------------------------------------------------------------

static void ReadExtentGroupDone(
  nutanix::test::CdpCompositeClusterManager *const cluster_mgr,
  StargateError::Type err,
  shared_ptr<string> err_detail,
  shared_ptr<ReadExtentGroupRet> ret,
  IOBuffer::Ptr&& iobuf,
  Notification *notify,
  vector<shared_ptr<EgroupOpsTester::SliceState>> slice_state_vec,
  pair<int64, int64> region_offset_length_read,
  IOBuffer::PtrConst data,
  const set<StargateError::Type>& expected_errors) {

  // Check for possible read errors.
  CHECK_GT(expected_errors.count(err), 0) << err;
  if (err != StargateError::kNoError) {
    if (notify) {
      notify->Notify();
    }
    return;
  }

  // Verify if we get the right amount of data.
  CHECK_GT(data->size(), 0);
  CHECK_EQ(iobuf->size(), region_offset_length_read.second);
  // Verify the data read is the same as the data written.
  if (!IOBufferUtil::Memcmp(data.get(), iobuf.get())) {
    LOG(INFO) << "Dumping data files in: "
              << cluster_mgr->cdp_options().test_out_dir;
    int fd = open(
      StringJoin(
        cluster_mgr->cdp_options().test_out_dir, "/expected.data").c_str(),
      O_RDWR | O_CREAT, 0755);
    SYSCALL_CHECK(fd >= 0);
    SYSCALL_CHECK(IOBufferUtil::WriteToFile(data.get(), fd, 0) ==
                  data->size());
    fsync(fd);
    close(fd);

    fd = open(
      StringJoin(cluster_mgr->cdp_options().test_out_dir,
                  "/read.data").c_str(),
      O_RDWR | O_CREAT, 0755);
    SYSCALL_CHECK(fd >= 0);
    SYSCALL_CHECK(IOBufferUtil::WriteToFile(iobuf.get(), fd, 0) ==
                  iobuf->size());
    fsync(fd);
    close(fd);

    LOG(FATAL) << "Data mismatch "
               << "data size: " << data->size()
               << " iobuf size: " << iobuf->size();
  }

  // By this time, the read op is over with no errors. Checksum verification
  // is already performed in egroup_read_op.
  if (notify) {
    notify->Notify();
  }
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::ReadExtentGroup(
  const int64 egroup_id,
  const int64 primary_disk_id,
  vector<Notification::Ptr>& notifications,
  const vector<pair<int64, int64>>& region_offset_size_vec,
  const vector<shared_ptr<EgroupOpsTester::SliceState>>& slice_state_vec,
  const vector<shared_ptr<EgroupOpsTester::Extent>>& extent_state_vec,
  IOBuffer::Ptr data_written,
  const int64 data_write_start_offset,
  const bool compressed_slice,
  const bool wait_for_read,
  const set<StargateError::Type>& expected_errors,
  const bool managed_by_aes,
  const bool extent_based_format,
  const int32 untransformed_slice_length,
  const bool slices_stored_by_id,
  const int slice_group_size) {

  // Get stargate interface associated with the primary disk.
  const StargateInterface::Ptr& iface =
    cluster_mgr_->stargate_mgr()->GetStargateInterfaceForDisk(primary_disk_id);
  CHECK(iface);
  notifications.resize(region_offset_size_vec.size());

  // Perform the read for every combinations of slice subregions.
  for (uint ii = 0; ii < region_offset_size_vec.size(); ++ii) {
    notifications[ii] = make_shared<Notification>();
    pair<int64, int64> offset_length_read = region_offset_size_vec[ii];

    // Prepare the read request.
    shared_ptr<ReadExtentGroupArg> arg = make_shared<ReadExtentGroupArg>();
    arg->set_extent_group_id(egroup_id);
    arg->set_qos_principal_name(string());
    arg->set_qos_priority(StargateQosPriority::kRead);
    arg->set_owner_vdisk_id(vdisk_id_);
    arg->set_owner_container_id(container_id_);
    arg->set_disk_id(primary_disk_id);
    arg->set_managed_by_aes(managed_by_aes);
    if (managed_by_aes) {
      arg->set_untransformed_slice_length(untransformed_slice_length);
    }
    arg->set_extent_based_metadata_format(extent_based_format);
    arg->set_slices_stored_by_id(slices_stored_by_id);
    arg->set_slice_group_size(slice_group_size);

    // Intent seqeunce should be the previous successful write intent sequence
    // number for this egroup.
    arg->set_min_expected_intent_sequence(
      egroup_id_intent_seq_map_[egroup_id]);

    // Set the slice state to arg. This is populated with the necessary
    // information by the caller.
    for (uint jj = 0; jj < slice_state_vec.size(); ++jj) {
      CHECK(slice_state_vec[jj]->has_extent_group_offset());
      slice_state_vec[jj]->CopyTo(arg->add_slices());
    }

    string transformation_type;
    if (compressed_slice) {
      transformation_type = "c=low";
    }
    if (!FLAGS_egroup_encryption_type.empty()) {
      if (!transformation_type.empty()) {
        transformation_type += ",";
      }
      transformation_type += FLAGS_egroup_encryption_type;
    }
    if (!transformation_type.empty()) {
      arg->set_transformation_type(transformation_type);
    }
    // Set the extent information for each extent. This is provided by the
    // caller.
    for (uint xx = 0; xx < extent_state_vec.size(); ++xx) {
      ReadExtentGroupArg::Extent *ex = arg->add_extents();

      ex->add_region_offset(get<0>(offset_length_read));
      ex->add_region_length(get<1>(offset_length_read));

      ex->mutable_data_location()->CopyFrom(
        extent_state_vec[xx]->diff_data_location());
      ex->mutable_extent_id()->CopyFrom(extent_state_vec[xx]->extent_id());
    }

    // Calculate the data written given the read offset/length.
    const int64 data_offset = get<0>(offset_length_read) -
                              data_write_start_offset;
    IOBuffer::PtrConst data_written_buf =
      data_written->Clone(data_offset, get<1>(offset_length_read));
    Function<void(StargateError::Type,
                  shared_ptr<string>,
                  shared_ptr<ReadExtentGroupRet>,
                  IOBuffer::Ptr&&)> done_cb =
      func_disabler_.Bind(&ReadExtentGroupDone, cluster_mgr_.get(),
                          _1, _2, _3, _4, notifications[ii].get(),
                          slice_state_vec, offset_length_read,
                          data_written_buf, expected_errors);
    notifications[ii]->Reset();

    // Invoke the stargate read extent group request.
    iface->ReadExtentGroup(move(arg), move(done_cb));

    // Wait for partial reads from extent store.
    if (wait_for_read)
      notifications[ii]->Wait();
  }
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::PopulateDiskIds() {

  Configuration::PtrConst config = cluster_mgr_->Config();
  const unordered_map<int64, const ConfigurationProto::Disk *>& disk_map =
    config->disk_id_map();
  disk_ids_.clear();
  blockstore_disks_.clear();

  for (const auto& iter : disk_map) {
    disk_ids_.emplace_back(iter.first);
    blockstore_disks_.push_back(iter.second);
  }
  if (FLAGS_egroup_test_estore_aes_hybrid_enabled) {
    AESHybridSwapDisks();
  }
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::CheckConfigForErasureCode() {
  if (!FLAGS_egroup_test_ec_ops) {
    return;
  }

  // Currently EC tests need exactly 5 stargates.
  CHECK_EQ(cluster_mgr_->stargate_mgr()->svm_ids().size(), 5);

  // The test can create more SSD disks to store metadata.
  CHECK_GE(disk_ids_.size(), 5);

  // The SSD disks created by the test should have larger ids. Sort the
  // disk ids so that disks in the json file will be placed in the first
  // half. These disks belong to different service VMs.
  sort(disk_ids_.begin(), disk_ids_.end());
  if (FLAGS_egroup_test_estore_aes_hybrid_enabled) {
    AESHybridSwapDisks();
  }

  // Let's verify that the first 5 disks are spread across different stargates.
  // We will build a 3/2 strip on the first 5 disks.
  LOG(INFO) << "disk_ids " << JoinWithSeparator(disk_ids_, ",");
  Configuration::PtrConst config = cluster_mgr_->Config();
  set<int64> seen_svm_ids;
  for (int ii = 0; ii < 5; ++ii) {
    const int64 disk_id = disk_ids_[ii];
    const ConfigurationProto::Disk *const disk = config->LookupDisk(disk_id);
    const int64 svm_id = disk->service_vm_id();
    CHECK(seen_svm_ids.insert(svm_id).second)
      << ii << " " << disk_id << " " << svm_id
      << " " << JoinWithSeparator(seen_svm_ids, ",");
  }
}

//-----------------------------------------------------------------------------

bool EgroupOpsTester::IsEvenDateToday() {
  time_t rawtime;
  struct tm timeinfo;
  time(&rawtime);
  localtime_r(&rawtime, &timeinfo);
  return (timeinfo.tm_yday % 2 == 0);
}

//-----------------------------------------------------------------------------

static IOBuffer::Ptr FillGapBetweenSlices(
  const vector<SliceState::Ptr>& slices,
  IOBuffer::Ptr iobuf) {

  // Find out whether the slice vector represents a contiguous region which
  // starts at offset zero.
  CHECK(slices.at(0)->has_extent_group_offset());
  const int64 start_offset = slices.at(0)->extent_group_offset();
  int64 extent_group_offset = start_offset;
  bool contiguous_region = true;
  for (uint ii = 0; ii < slices.size() && contiguous_region; ++ii) {
    CHECK(slices.at(ii)->has_extent_group_offset());
    if (slices.at(ii)->extent_group_offset() != extent_group_offset) {
      CHECK_GT(slices.at(ii)->extent_group_offset(), extent_group_offset);
      contiguous_region = false;
    } else {
      extent_group_offset += slices.at(ii)->transformed_length();
    }
  }

  // Return the original buffer if it exactly matches the disk file content.
  const int32 last_cushion = slices.back()->cushion();
  if (contiguous_region && last_cushion == 0) {
    return iobuf;
  }

  // Fill the gaps between slices with zeros.
  extent_group_offset = start_offset;
  int64 iobuffer_offset = 0;
  IOBuffer::Ptr output_iobuf = make_shared<IOBuffer>();
  for (uint ii = 0; ii < slices.size(); ++ii) {
    // Fill gaps with zeros.
    if (slices.at(ii)->extent_group_offset() > extent_group_offset) {
      const int64 offset_difference =
        slices.at(ii)->extent_group_offset() - extent_group_offset;
      IOBuffer::PtrConst zero_iobuf =
        IOBufferUtil::CreateZerosIOBuffer(offset_difference);
      output_iobuf->AppendIOBuffer(zero_iobuf.get(), 0, offset_difference);
    }
    // Copy the next slice to the output buffer.
    output_iobuf->AppendIOBuffer(iobuf.get(), iobuffer_offset,
                                 slices.at(ii)->transformed_length());
    iobuffer_offset += slices.at(ii)->transformed_length();
    // Set the offset past the end of the copied slice.
    extent_group_offset = slices.at(ii)->extent_group_offset() +
                          slices.at(ii)->transformed_length();
  }

  // Fill the last cushion with zeros.
  if (last_cushion > 0) {
    IOBuffer::PtrConst zero_iobuf =
      IOBufferUtil::CreateZerosIOBuffer(last_cushion);
    output_iobuf->AppendIOBuffer(zero_iobuf.get(), 0, last_cushion);
  }
  return output_iobuf;
}

//-----------------------------------------------------------------------------

static void ConvertSliceAndExtentMapsToVectors(
  const unordered_map<int32, SliceState::Ptr>& slice_id_to_state,
  const unordered_map<string, shared_ptr<EgroupOpsTester::Extent>>&
    extent_to_state,
  vector<SliceState::Ptr> *info_slice_state_vec,
  vector<shared_ptr<EgroupOpsTester::Extent>> *info_extent_state_vec) {

  info_slice_state_vec->clear();
  info_extent_state_vec->clear();

  for (const auto& iter : slice_id_to_state) {
    info_slice_state_vec->emplace_back(iter.second);
  }

  for (const auto& iter : extent_to_state) {
    shared_ptr<EgroupOpsTester::Extent> extent_info =
      make_shared<EgroupOpsTester::Extent>();
    extent_info->mutable_extent_id()->CopyFrom(iter.second->extent_id());
    ExtentDataLocation *data_location =
      extent_info->mutable_diff_data_location();
    data_location->CopyFrom(iter.second->diff_data_location());
    CHECK_EQ(data_location->slice_indices(0), 0)
      << extent_info->ShortDebugString();
    info_extent_state_vec->emplace_back(extent_info);
  }
}

//-----------------------------------------------------------------------------

static void MergeOldAndNewParitySliceStates(
  vector<vector<SliceState::Ptr> *> *old_parity_slice_state_vec,
  const vector<vector<SliceState::Ptr>>& new_parity_slice_state_vec) {

  // We should have the same number of parity slice states.
  CHECK_EQ(old_parity_slice_state_vec->size(),
           new_parity_slice_state_vec.size());
  // Examine slice states per parity egroup.
  for (uint ii = 0; ii < old_parity_slice_state_vec->size(); ++ii) {
    map<int32, SliceState::Ptr> slice_id_to_state;
    vector<SliceState::Ptr> *old_parity_slice_states =
      old_parity_slice_state_vec->at(ii);
    // Build a map of id to state for the old slice states.
    for (uint jj = 0; jj < old_parity_slice_states->size(); ++jj) {
      const int32 slice_id = old_parity_slice_states->at(jj)->slice_id();
      slice_id_to_state[slice_id] = old_parity_slice_states->at(jj);
    }
    const vector<SliceState::Ptr>& new_parity_slice_states =
      new_parity_slice_state_vec.at(ii);
    // Update any existing slice state or add a new slice state.
    for (uint jj = 0; jj < new_parity_slice_states.size(); ++jj) {
      const int32 slice_id = new_parity_slice_states.at(jj)->slice_id();
      slice_id_to_state[slice_id] = new_parity_slice_states.at(jj);
    }
    old_parity_slice_states->clear();
    for (const auto& iter : slice_id_to_state) {
      old_parity_slice_states->emplace_back(iter.second);
    }
    CHECK_GT(old_parity_slice_states->size(), 0);
  }
}


//-----------------------------------------------------------------------------

static void MergeInfoSliceAndExtentStates(
  unordered_map<int32, SliceState::Ptr> *slice_id_to_state,
  unordered_map<string, shared_ptr<EgroupOpsTester::Extent>> *extent_to_state,
  const vector<SliceState::Ptr>& tmp_info_slice_state_vec,
  const vector<shared_ptr<EgroupOpsTester::Extent>>&
    tmp_info_extent_state_vec) {

  // Add new slice state or update existing slice state.
  for (uint ii = 0; ii < tmp_info_slice_state_vec.size(); ++ii) {
    (*slice_id_to_state)[tmp_info_slice_state_vec[ii]->slice_id()]
      = tmp_info_slice_state_vec[ii];
  }

  for (uint ii = 0; ii < tmp_info_extent_state_vec.size(); ++ii) {
    const string& extent_id_str  =
      tmp_info_extent_state_vec[ii]->extent_id().ShortDebugString();

    if (extent_to_state->count(extent_id_str) == 0) {
      // This is a new extent create state for the new extent.
      (*extent_to_state)[extent_id_str] =
        make_shared<EgroupOpsTester::Extent>();
      (*extent_to_state)[extent_id_str]->mutable_extent_id()->CopyFrom(
        tmp_info_extent_state_vec[ii]->extent_id());
      (*extent_to_state)[extent_id_str]->mutable_diff_data_location()->
        CopyFrom(tmp_info_extent_state_vec[ii]->diff_data_location());
    } else {
      // If the extent already exists add the diff information to the existing
      // extent information.
      const auto& new_diff_data_location =
        tmp_info_extent_state_vec[ii]->diff_data_location();
      const auto& old_diff_data_location =
        (*extent_to_state)[extent_id_str]->diff_data_location();
      CHECK_EQ(new_diff_data_location.slice_indices_size(),
               new_diff_data_location.slice_ids_size());
      CHECK_EQ(old_diff_data_location.slice_indices_size(),
               old_diff_data_location.slice_ids_size());
      map<int32, int32> slice_indx_to_id;
      for (int jj = 0; jj < old_diff_data_location.slice_ids_size(); ++jj) {
        const int32 idx = old_diff_data_location.slice_indices(jj);
        const int32 id = old_diff_data_location.slice_ids(jj);
        slice_indx_to_id[idx] = id;
      }
      for (int jj = 0; jj < new_diff_data_location.slice_ids_size(); ++jj) {
        const int32 idx = new_diff_data_location.slice_indices(jj);
        const int32 id = new_diff_data_location.slice_ids(jj);
        slice_indx_to_id[idx] = id;
      }
      const int32 first_slice_offset =
        new_diff_data_location.has_first_slice_offset() ?
        new_diff_data_location.first_slice_offset() :
        old_diff_data_location.first_slice_offset();

      (*extent_to_state)[extent_id_str]->clear_diff_data_location();
      for (const auto iter : slice_indx_to_id) {
        (*extent_to_state)[extent_id_str]->mutable_diff_data_location()->
          add_slice_indices(iter.first);
        (*extent_to_state)[extent_id_str]->mutable_diff_data_location()->
          add_slice_ids(iter.second);
      }
      (*extent_to_state)[extent_id_str]->mutable_diff_data_location()->
        set_first_slice_offset(first_slice_offset);
    }
  }
  CHECK_GT(slice_id_to_state->size(), 0);
  CHECK_GT(extent_to_state->size(), 0);
}

//-----------------------------------------------------------------------------

static void WriteErasureCodeEgroupDone(
  StargateError::Type err,
  shared_ptr<string> err_detail,
  shared_ptr<WriteExtentGroupRet> ret,
  const int write_num,
  Notification *notify,
  vector<SliceState::Ptr> *info_slice_state_vec,
  vector<shared_ptr<EgroupOpsTester::Extent>> *info_extent_state_vec,
  vector<vector<SliceState::Ptr>> *parity_slice_state_vec,
  const set<StargateError::Type>& expected_errs) {

  LOG(INFO) << "Write " << write_num << " finished";

  CHECK_GT(expected_errs.count(err), 0);

  CHECK(ret->has_primary());
  CHECK(info_slice_state_vec);
  CHECK_EQ(info_slice_state_vec->size(), 0);
  CHECK(parity_slice_state_vec);
  CHECK(notify);

  // Copy the slice state information from the response.
  for (int ii = 0; ii < ret->primary().slices().size(); ++ii) {
    shared_ptr<EgroupOpsTester::SliceState> slice_state =
      EgroupOpsTester::SliceState::Create(ret->primary().slices(ii));
    info_slice_state_vec->emplace_back(slice_state);
  }

  // Sort the slice states in the order of slice ids.
  sort(info_slice_state_vec->begin(), info_slice_state_vec->end(),
       SliceState::SliceStatePtrOrderIncreasingSliceId);

  for (int ii = 0; ii < ret->primary().extents().size(); ++ii) {
    shared_ptr<EgroupOpsTester::Extent> extent_info =
      make_shared<EgroupOpsTester::Extent>();
    extent_info->CopyFrom(ret->primary().extents(ii));
    info_extent_state_vec->emplace_back(extent_info);
  }

  CHECK_GT(ret->parity_egroup_size(), 0) << ret->ShortDebugString();
  for (int ii = 0; ii < ret->parity_egroup_size(); ++ii) {
    const WriteExtentGroupRet::ParityExtentGroup& parity_egroup_ret =
      ret->parity_egroup(ii);
    CHECK(parity_egroup_ret.has_primary());
    const WriteExtentGroupRet::Primary& primary = parity_egroup_ret.primary();
    unordered_map<int32, int32> parity_slice_id_to_idx;
    for (uint jj = 0; jj < parity_slice_state_vec->at(ii).size(); ++jj) {
      parity_slice_id_to_idx[
        parity_slice_state_vec->at(ii).at(jj)->slice_id()] = jj;
    }
    for (int jj = 0; jj < primary.slices_size(); ++jj) {
      shared_ptr<EgroupOpsTester::SliceState> slice_state =
        EgroupOpsTester::SliceState::Create(primary.slices(jj));
      const int32 slice_id = slice_state->slice_id();
     if (parity_slice_id_to_idx.count(slice_id) > 0) {
       auto iter = parity_slice_id_to_idx.find(slice_id);
       parity_slice_state_vec->at(ii).at(iter->second) = slice_state;
     } else {
      parity_slice_state_vec->at(ii).emplace_back(slice_state);
     }
    }
    // Sort the slice states in the order of slice ids.
    sort(parity_slice_state_vec->at(ii).begin(),
         parity_slice_state_vec->at(ii).end(),
         SliceState::SliceStatePtrOrderIncreasingSliceId);
  }
  notify->Notify();
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::OverwriteSingleExtentInErasureCodedEgroup(
  const int64 info_egroup_id,
  const int64 info_disk_id,
  const vector<int64>& info_egroup_ids,
  const vector<int64>& parity_egroup_ids,
  const vector<int64>& parity_disk_ids,
  const int64 vblock_num,
  const string& transformation_type,
  const vector<DataTransformation::Type>& transformation_type_vec,
  const vector<RegionDescriptor>& region_to_write_vec,
  Notification *const notification,
  vector<SliceState::Ptr> *info_slice_state_vec,
  vector<shared_ptr<EgroupOpsTester::Extent>> *info_extent_state_vec,
  vector<vector<SliceState::Ptr>> *parity_slice_state_vec,
  IOBuffer::Ptr *data_to_write,
  const bool inject_corruption) {

  // Perform the preliminary checks.
  CHECK(info_slice_state_vec);
  CHECK(info_extent_state_vec);
  CHECK(parity_slice_state_vec);
  CHECK(notification);
  CHECK(data_to_write && *data_to_write);
  CHECK_EQ(parity_egroup_ids.size(), parity_disk_ids.size());

  (*data_to_write)->Clear();

  // Get stargate interface associated with the primary disk.
  const StargateInterface::Ptr& iface =
    cluster_mgr_->stargate_mgr()->GetStargateInterfaceForDisk(info_disk_id);
  CHECK(iface);

  const int64 current_intent_sequence =
    egroup_id_intent_seq_map_[info_egroup_id];
  const int64 expected_intent_sequence =
    egroup_id_expected_intent_seq_map_[info_egroup_id];

  if (inject_corruption) {
    // Set the stargate gflag to inject corruption on this write.
    const int64 svm_id = cluster_mgr_->stargate_mgr()->GetSvmIdForStargate(
      cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(info_disk_id));
    cluster_mgr_->stargate_mgr()->SetGFlag(
      "estore_experimental_corrupt_write_on_intent_sequence",
      to_string(current_intent_sequence), svm_id);
  }

  // Add the necessary arguments to the write request.
  shared_ptr<WriteExtentGroupArg> arg = make_shared<WriteExtentGroupArg>();
  arg->set_extent_group_id(info_egroup_id);
  arg->set_qos_principal_name(string());
  arg->set_qos_priority(StargateQosPriority::kWrite);
  arg->set_owner_vdisk_id(vdisk_id_);
  arg->set_owner_container_id(container_id_);
  arg->set_managed_by_aes(false);
  vector<DataTransformation::Type> tmp_transformation_type_vec;
  if (!transformation_type.empty()) {
    arg->set_transformation_type(transformation_type);
    CHECK(StargateUtil::StringToEnumTransformationTypes(
            transformation_type, &tmp_transformation_type_vec))
      << transformation_type;
  } else {
    for (const DataTransformation::Type transform_type :
           transformation_type_vec) {
      arg->add_transformation_type_list(transform_type);
    }
    tmp_transformation_type_vec = transformation_type_vec;
  }
  arg->set_owner_container_id(container_id_);
  arg->add_disk_ids(info_disk_id);
  arg->set_intent_sequence(current_intent_sequence);

  if (expected_intent_sequence > 0) {
    arg->set_expected_intent_sequence(expected_intent_sequence);
  }

  arg->set_egroup_type(WriteExtentGroupArg::kDataEgroup);

  WriteExtentGroupArg::Primary *primary_state = arg->mutable_primary();
  WriteExtentGroupArg::Primary::Extent *ex = primary_state->add_extents();
  ExtentIdProto *extent_id_proto = ex->mutable_extent_id();
  extent_id_proto->set_vdisk_block(vblock_num);
  extent_id_proto->set_owner_id(vdisk_id_);
  for (uint ii = 0; ii < info_extent_state_vec->size(); ++ii) {
    const auto& extent_id = info_extent_state_vec->at(ii)->extent_id();
    if (extent_id.vdisk_block() == vblock_num &&
        extent_id.owner_id() == vdisk_id_) {
      ex->mutable_data_location()->CopyFrom(
        info_extent_state_vec->at(ii)->diff_data_location());
      break;
    }
  }

  const int32 untransformed_slice_length =
    StargateUtil::GetSliceSizeForTransformation(
      tmp_transformation_type_vec);
  arg->set_untransformed_slice_length(untransformed_slice_length);

  // Iterate through the regions and generate the data.
  for (const RegionDescriptor& region : region_to_write_vec) {
    IOBuffer::Ptr offset_buf;
    if (region.compress_percentage > 0) {
      const int32 chunk_size =
        untransformed_slice_length > region.length ?
        region.length : untransformed_slice_length;
      offset_buf = Random::CreateCompressibleDataIOBuffer(
        region.length, region.compress_percentage, chunk_size);
    } else {
      offset_buf = Random::CreateRandomDataIOBuffer(region.length);
    }

    // Append the iobuffer for the offset data to data_to_write.
    CHECK(offset_buf);
    CHECK_EQ(offset_buf->size(), region.length);
    (*data_to_write)->AppendIOBuffer(offset_buf.get());

    // Include the region to write.
    ex->add_region_offset(region.offset);
    ex->add_region_length(region.length);
  }

  for (uint ii = 0; ii < info_slice_state_vec->size(); ++ii) {
    info_slice_state_vec->at(ii)->CopyTo(arg->mutable_primary()->add_slices());
  }

  info_slice_state_vec->clear();
  info_extent_state_vec->clear();

  // Populate additional fields required for overwriting erasure coded egroups.
  WriteExtentGroupArg::ErasureCodingInfo *ec_info =
    arg->mutable_erasure_coding_info();
  ec_info->set_num_info_egroups(info_egroup_ids.size());
  ec_info->set_num_parity_egroups(parity_egroup_ids.size());
  for (uint ii = 0; ii < info_egroup_ids.size(); ++ii) {
    if (info_egroup_ids[ii] == info_egroup_id) {
      ec_info->set_info_egroup_strip_position(ii);
      break;
    }
  }
  CHECK(ec_info->has_info_egroup_strip_position());

  for (uint ii = 0; ii < parity_egroup_ids.size(); ++ii) {
    const int64 parity_egroup_id = parity_egroup_ids[ii];
    WriteExtentGroupArg::ErasureCodingInfo::ParityExtentGroup* parity_egroup =
      ec_info->add_parity_egroup();
    parity_egroup->set_extent_group_id(parity_egroup_id);
    parity_egroup->set_owner_vdisk_id(vdisk_id_);
    parity_egroup->set_parity_egroup_strip_position(
      info_egroup_ids.size() + ii);
    parity_egroup->set_expected_intent_sequence(
      egroup_id_expected_intent_seq_map_[parity_egroup_id]);
    parity_egroup->set_intent_sequence(
      egroup_id_intent_seq_map_[parity_egroup_id]);
    parity_egroup->set_disk_id(parity_disk_ids[ii]);
    sort(parity_slice_state_vec->at(ii).begin(),
         parity_slice_state_vec->at(ii).end(),
         medusa::SliceState::SliceStatePtrOrderIncreasingFileOffset);
    const SliceState::Ptr& slice_state =
      parity_slice_state_vec->at(ii).back();
    const int32 max_eg_offset = slice_state->extent_group_offset() +
                                slice_state->transformed_length();
    for (uint jj = 0; jj < parity_slice_state_vec->at(ii).size(); ++jj) {
      parity_slice_state_vec->at(ii).at(jj)->CopyTo(
        parity_egroup->add_slices());
    }
    parity_egroup->mutable_extent_id()->set_sha1_hash(string(""));
    parity_egroup->mutable_extent_id()->set_extent_size(max_eg_offset);
    parity_egroup->mutable_extent_id()->set_owner_id(container_id_);
  }

  set<StargateError::Type> expected_errs = { StargateError::kNoError };
  if (inject_corruption) {
    expected_errs.insert(StargateError::kDataCorrupt);
  }
  Function<void(StargateError::Type,
                shared_ptr<string>,
                shared_ptr<WriteExtentGroupRet>)> done_cb =
    func_disabler_.Bind(&WriteErasureCodeEgroupDone,
                        _1, _2, _3, vblock_num, notification,
                        info_slice_state_vec, info_extent_state_vec,
                        parity_slice_state_vec, expected_errs);
  notification->Reset();
  iface->WriteExtentGroup(arg, *data_to_write, move(done_cb),
                          FLAGS_egroup_test_write_eg_rpc_timeout * 1000);
}

//-----------------------------------------------------------------------------

static void CompareSubregionChecksums(
  IOBuffer::Ptr parity_buf,
  const vector<SliceState::Ptr>& slice_state_vec) {

  const int slice_size = FLAGS_estore_regular_slice_size;
  // We might need to pad the computed parity buf to align it to size boundary.
  if (parity_buf->size() % slice_size > 0) {
    const int32 zero_buf_sz = slice_size - (parity_buf->size() % slice_size);
    const IOBuffer::Ptr zero_buf =
      IOBufferUtil::CreateZerosIOBuffer(zero_buf_sz);
    parity_buf->AppendIOBuffer(zero_buf.get());
  }

  const int num_chunks = parity_buf->size() / slice_size;
  map<int32, vector<uint>> slice_id_cksum_map;
  for (int ii = 0; ii < num_chunks; ++ii) {
    IOBuffer::Ptr iobuf = parity_buf->Clone(ii * slice_size, slice_size);
    // Compute the checksum for every slice subregion.
    const int subregion_size = FLAGS_estore_regular_slice_size /
      slice_state_vec[ii]->checksum_size();
    for (int jj = 0; jj < slice_state_vec[ii]->checksum_size(); ++jj) {
      // Compute the checksum on the subregion.
      slice_id_cksum_map[slice_state_vec[ii]->slice_id()].emplace_back(
        IOBufferUtil::Adler32(
          iobuf.get(), jj * subregion_size, subregion_size));
    }
  }

  CHECK_EQ(slice_state_vec.size(), slice_id_cksum_map.size());
  for (uint ii = 0; ii < slice_state_vec.size(); ++ii) {
    const int32 slice_id = slice_state_vec[ii]->slice_id();
    const vector<uint>& cksum_vec = slice_id_cksum_map[slice_id];
    for (int jj = 0; jj < slice_state_vec[ii]->checksum_size(); ++jj) {
      CHECK_EQ(slice_state_vec[ii]->checksum(jj), cksum_vec[jj])
        << "Chksum mismatch slice: " << slice_id << ", subregion: " << jj;
    }
  }
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::OverwriteRollbackErasureCodedStripHelper(
  const vector<int64>& info_egroup_ids,
  const vector<int64>& info_disk_ids,
  const int64 primary_parity_egroup_id,
  const int64 primary_parity_disk_id,
  const int64 secondary_parity_egroup_id,
  const int64 secondary_parity_disk_id,
  vector<vector<SliceState::Ptr>> *info_slice_state_vecs,
  vector<SliceState::Ptr> *primary_parity_slice_state_vec,
  vector<SliceState::Ptr> *secondary_parity_slice_state_vec,
  vector<vector<shared_ptr<EgroupOpsTester::Extent>>> *info_extent_state_vecs,
  const bool compressible_data,
  const int32 compress_percentage,
  const bool inject_corruption,
  const bool test_rollback_after_crash) {

  CHECK_EQ(info_egroup_ids.size(), info_disk_ids.size());
  CHECK_EQ(info_slice_state_vecs->size(), info_egroup_ids.size());
  CHECK_EQ(info_extent_state_vecs->size(), info_egroup_ids.size());
  vector<IOBuffer::Ptr> info_vec;
  vector<vector<SliceState::Ptr>> old_slice_state_vec;
  for (uint ii = 0; ii < info_egroup_ids.size(); ++ii) {
    const int64 info_egroup_id = info_egroup_ids[ii];
    const int64 info_disk_id = info_disk_ids[ii];
    IOBuffer::Ptr transformed_data = make_shared<IOBuffer>();

    // Read the transformed data back from the info egroup.
    sort(info_slice_state_vecs->at(ii).begin(),
         info_slice_state_vecs->at(ii).end(),
         medusa::SliceState::SliceStatePtrOrderIncreasingFileOffset);
    vector<SliceState::Ptr> slice_state_vec = info_slice_state_vecs->at(ii);
    FilterUnAllocatedSlices(&slice_state_vec);
    ReplicaReadEgroup(
      info_egroup_id, info_disk_id, slice_state_vec, &transformed_data);
    CHECK(transformed_data && transformed_data->size() > 0);
    transformed_data =
      FillGapBetweenSlices(slice_state_vec, transformed_data);
    info_vec.emplace_back(move(transformed_data));
    old_slice_state_vec.push_back(slice_state_vec);
  }

  // We will save the old slice and extent states before overwriting. This is
  // because we will issue a rollback immediately after the overwrite which
  // should revert the physical state of the extent group back to this saved
  // state.
  vector<vector<SliceState::Ptr>> save_info_slice_state_vecs =
    *info_slice_state_vecs;
  vector<SliceState::Ptr> saved_primary_parity_slice_state_vec =
    *primary_parity_slice_state_vec;
  vector<SliceState::Ptr> saved_secondary_parity_slice_state_vec =
    *secondary_parity_slice_state_vec;
  vector<vector<shared_ptr<EgroupOpsTester::Extent>>>
    saved_info_extent_state_vec = *info_extent_state_vecs;

  IOBuffer::Ptr transformed_data;
  vector<SliceState::Ptr> slice_state_vec;
  for (uint ii = 0; ii < info_egroup_ids.size(); ++ii) {
    const int64 info_egroup_id = info_egroup_ids[ii];
    const int64 info_disk_id = info_disk_ids[ii];
    egroup_id_intent_seq_map_[info_egroup_id] += 1;
    egroup_id_intent_seq_map_[primary_parity_egroup_id] += 1;
    egroup_id_intent_seq_map_[secondary_parity_egroup_id] += 1;

    // Do a write, write, rollback, write loop on the info egroup.

    // Perform an overwrite but skip the read verficiation, the read on the
    // extent group will force estore to clear the ephemeral state which is
    // needed to perform a rollback.
    LOG(INFO) << "Overwrite 1 on egroup " << info_egroup_id;
    OverwriteErasureCodedInfoEgroupHelper(
      info_egroup_id,
      info_disk_id,
      primary_parity_egroup_id,
      primary_parity_disk_id,
      secondary_parity_egroup_id,
      secondary_parity_disk_id,
      info_egroup_ids,
      &info_slice_state_vecs->at(ii),
      primary_parity_slice_state_vec,
      secondary_parity_slice_state_vec,
      &info_extent_state_vecs->at(ii),
      compressible_data,
      compress_percentage);

    // We will save the old slice and extent states before overwriting. This is
    // because we will issue a rollback immediately after the overwrite which
    // should revert the physical state of the extent group back to this saved
    // state.
    vector<SliceState::Ptr> saved_info_slice_state_vec =
      info_slice_state_vecs->at(ii);
    vector<SliceState::Ptr> saved_primary_parity_slice_state_vec =
      *primary_parity_slice_state_vec;
    vector<SliceState::Ptr> saved_secondary_parity_slice_state_vec =
      *secondary_parity_slice_state_vec;
    vector<shared_ptr<EgroupOpsTester::Extent>>
      saved_info_extent_state_vec = info_extent_state_vecs->at(ii);

    // Once every while we will rollback to an intent sequence not in the
    // update queue.
    if (Random::TL()->Uniform(1, 100) >= 50) {
      // Read the new transformed data back from the info egroup. This will
      // force the applied update till now to be dropped from the update_q.
      transformed_data = make_shared<IOBuffer>();
      sort(info_slice_state_vecs->at(ii).begin(),
           info_slice_state_vecs->at(ii).end(),
           medusa::SliceState::SliceStatePtrOrderIncreasingFileOffset);
      slice_state_vec = info_slice_state_vecs->at(ii);
      FilterUnAllocatedSlices(&slice_state_vec);
      ReplicaReadEgroup(
        info_egroup_id, info_disk_id, slice_state_vec, &transformed_data);
      CHECK(transformed_data && transformed_data->size() > 0);
    }
    // Perform another write, we will rollback this write. If asked to inject
    // corruption, we will corrupt this write.
    LOG(INFO) << "Overwrite 2 on egroup " << info_egroup_id;
    egroup_id_intent_seq_map_[info_egroup_id] += 1;
    egroup_id_intent_seq_map_[primary_parity_egroup_id] += 1;
    egroup_id_intent_seq_map_[secondary_parity_egroup_id] += 1;
    OverwriteErasureCodedInfoEgroupHelper(
      info_egroup_id,
      info_disk_id,
      primary_parity_egroup_id,
      primary_parity_disk_id,
      secondary_parity_egroup_id,
      secondary_parity_disk_id,
      info_egroup_ids,
      &info_slice_state_vecs->at(ii),
      primary_parity_slice_state_vec,
      secondary_parity_slice_state_vec,
      &info_extent_state_vecs->at(ii),
      compressible_data,
      compress_percentage,
      inject_corruption);

    // Restore the saved slice and extent states.
    info_slice_state_vecs->at(ii) = saved_info_slice_state_vec;
    *primary_parity_slice_state_vec = saved_primary_parity_slice_state_vec;
    *secondary_parity_slice_state_vec = saved_secondary_parity_slice_state_vec;
    info_extent_state_vecs->at(ii) = saved_info_extent_state_vec;

    // Now rollback the last write.
    LOG(INFO) << "Rollback overwrite 2 on egroup " << info_egroup_id;
    const int num_updates_to_rollback =
      FLAGS_stargate_ideal_extent_group_size_MB;
    RollbackErasureCodedEgroupHelper(
      info_egroup_id, info_disk_id,
      &saved_info_slice_state_vec, &saved_info_extent_state_vec,
      primary_parity_egroup_id, primary_parity_disk_id,
      &saved_primary_parity_slice_state_vec,
      secondary_parity_egroup_id, secondary_parity_disk_id,
      &saved_secondary_parity_slice_state_vec,
      num_updates_to_rollback,
      compressible_data,
      inject_corruption,
      test_rollback_after_crash);

    // Let us perform another write.
    LOG(INFO) << "Overwrite 3 on egroup " << info_egroup_id;
    egroup_id_intent_seq_map_[info_egroup_id] += 1;
    egroup_id_intent_seq_map_[primary_parity_egroup_id] += 1;
    egroup_id_intent_seq_map_[secondary_parity_egroup_id] += 1;
    OverwriteErasureCodedInfoEgroupHelper(
      info_egroup_id,
      info_disk_id,
      primary_parity_egroup_id,
      primary_parity_disk_id,
      secondary_parity_egroup_id,
      secondary_parity_disk_id,
      info_egroup_ids,
      &info_slice_state_vecs->at(ii),
      primary_parity_slice_state_vec,
      secondary_parity_slice_state_vec,
      &info_extent_state_vecs->at(ii),
      compressible_data,
      compress_percentage);

    // Read the new transformed data back from the info egroup.
    transformed_data = make_shared<IOBuffer>();
    sort(info_slice_state_vecs->at(ii).begin(),
         info_slice_state_vecs->at(ii).end(),
         medusa::SliceState::SliceStatePtrOrderIncreasingFileOffset);

    slice_state_vec = info_slice_state_vecs->at(ii);
    FilterUnAllocatedSlices(&slice_state_vec);
    ReplicaReadEgroup(
      info_egroup_id, info_disk_id, slice_state_vec, &transformed_data);
    CHECK(transformed_data && transformed_data->size() > 0);

    transformed_data =
      FillGapBetweenSlices(slice_state_vec, transformed_data);

    info_vec[ii] = move(transformed_data);

    // Read the new parity data from the parity egroups.
    sort(primary_parity_slice_state_vec->begin(),
         primary_parity_slice_state_vec->end(),
         medusa::SliceState::SliceStatePtrOrderIncreasingFileOffset);
    sort(secondary_parity_slice_state_vec->begin(),
         secondary_parity_slice_state_vec->end(),
         medusa::SliceState::SliceStatePtrOrderIncreasingFileOffset);

    // Compute the new expected parities.
    vector<IOBuffer::Ptr> parity_vec =
      { make_shared<IOBuffer>(), make_shared<IOBuffer>() };
    StargateParity parity_encoder;
    parity_encoder.Encode(info_vec, parity_vec);

    slice_state_vec = *primary_parity_slice_state_vec;
    FilterUnAllocatedSlices(&slice_state_vec);

    LOG(INFO) << "Verify parity egroup " << primary_parity_egroup_id
              << " contents";

    CompareSubregionChecksums(parity_vec.at(0), slice_state_vec);
    ReplicaReadEgroup(
      primary_parity_egroup_id, primary_parity_disk_id, slice_state_vec,
      &transformed_data);

    CHECK_EQ(transformed_data->size(), parity_vec.at(0)->size());
    CHECK(IOBufferUtil::Memcmp(transformed_data.get(),
                               parity_vec.at(0).get()));

    slice_state_vec = *secondary_parity_slice_state_vec;
    FilterUnAllocatedSlices(&slice_state_vec);

    LOG(INFO) << "Verify parity egroup " << secondary_parity_egroup_id
              << " contents";

    CompareSubregionChecksums(parity_vec.at(1), slice_state_vec);
    ReplicaReadEgroup(
      secondary_parity_egroup_id, secondary_parity_disk_id, slice_state_vec,
      &transformed_data);

    CHECK_EQ(transformed_data->size(), parity_vec.at(1)->size());
    CHECK(IOBufferUtil::Memcmp(transformed_data.get(),
                               parity_vec.at(1).get()));
  }
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::OverwriteErasureCodedStripHelper(
  const vector<int64>& info_egroup_ids,
  const vector<int64>& info_disk_ids,
  const int64 primary_parity_egroup_id,
  const int64 primary_parity_disk_id,
  const int64 secondary_parity_egroup_id,
  const int64 secondary_parity_disk_id,
  vector<vector<SliceState::Ptr>> *info_slice_state_vecs,
  vector<SliceState::Ptr> *primary_parity_slice_state_vec,
  vector<SliceState::Ptr> *secondary_parity_slice_state_vec,
  vector<vector<shared_ptr<EgroupOpsTester::Extent>>> *info_extent_state_vecs,
  const bool compressible_data,
  const int32 compress_percentage) {

  CHECK_EQ(info_egroup_ids.size(), info_disk_ids.size());
  CHECK_EQ(info_slice_state_vecs->size(), info_egroup_ids.size());
  CHECK_EQ(info_extent_state_vecs->size(), info_egroup_ids.size());
  vector<IOBuffer::Ptr> info_vec;
  vector<vector<SliceState::Ptr>> old_slice_state_vec;
  for (uint ii = 0; ii < info_egroup_ids.size(); ++ii) {
    const int64 info_egroup_id = info_egroup_ids[ii];
    const int64 info_disk_id = info_disk_ids[ii];
    IOBuffer::Ptr transformed_data = make_shared<IOBuffer>();

    // Read the transformed data back from the info egroup.
    sort(info_slice_state_vecs->at(ii).begin(),
         info_slice_state_vecs->at(ii).end(),
         medusa::SliceState::SliceStatePtrOrderIncreasingFileOffset);
    vector<SliceState::Ptr> slice_state_vec = info_slice_state_vecs->at(ii);
    FilterUnAllocatedSlices(&slice_state_vec);
    ReplicaReadEgroup(
      info_egroup_id, info_disk_id, slice_state_vec, &transformed_data);
    CHECK(transformed_data && transformed_data->size() > 0);
    transformed_data =
      FillGapBetweenSlices(slice_state_vec, transformed_data);
    info_vec.emplace_back(move(transformed_data));
    old_slice_state_vec.push_back(slice_state_vec);
  }

  for (uint ii = 0; ii < info_egroup_ids.size(); ++ii) {
    const int64 info_egroup_id = info_egroup_ids[ii];
    const int64 info_disk_id = info_disk_ids[ii];
    egroup_id_intent_seq_map_[info_egroup_id] += 1;
    egroup_id_intent_seq_map_[primary_parity_egroup_id] += 1;
    egroup_id_intent_seq_map_[secondary_parity_egroup_id] += 1;

    OverwriteErasureCodedInfoEgroupHelper(
      info_egroup_id,
      info_disk_id,
      primary_parity_egroup_id,
      primary_parity_disk_id,
      secondary_parity_egroup_id,
      secondary_parity_disk_id,
      info_egroup_ids,
      &info_slice_state_vecs->at(ii),
      primary_parity_slice_state_vec,
      secondary_parity_slice_state_vec,
      &info_extent_state_vecs->at(ii),
      compressible_data,
      compress_percentage);

    // Read the new transformed data back from the info egroup.
    IOBuffer::Ptr transformed_data = make_shared<IOBuffer>();
    sort(info_slice_state_vecs->at(ii).begin(),
         info_slice_state_vecs->at(ii).end(),
         medusa::SliceState::SliceStatePtrOrderIncreasingFileOffset);

    vector<SliceState::Ptr> slice_state_vec = info_slice_state_vecs->at(ii);
    FilterUnAllocatedSlices(&slice_state_vec);
    ReplicaReadEgroup(
      info_egroup_id, info_disk_id, slice_state_vec, &transformed_data);
    CHECK(transformed_data && transformed_data->size() > 0);

    transformed_data =
      FillGapBetweenSlices(slice_state_vec, transformed_data);

    info_vec[ii] = move(transformed_data);

    // Read the new parity data from the parity egroups.
    sort(primary_parity_slice_state_vec->begin(),
         primary_parity_slice_state_vec->end(),
         medusa::SliceState::SliceStatePtrOrderIncreasingFileOffset);
    sort(secondary_parity_slice_state_vec->begin(),
         secondary_parity_slice_state_vec->end(),
         medusa::SliceState::SliceStatePtrOrderIncreasingFileOffset);

    // Compute the new expected parities.
    vector<IOBuffer::Ptr> parity_vec =
      { make_shared<IOBuffer>(), make_shared<IOBuffer>() };
    StargateParity parity_encoder;
    parity_encoder.Encode(info_vec, parity_vec);

    slice_state_vec = *primary_parity_slice_state_vec;
    FilterUnAllocatedSlices(&slice_state_vec);

    LOG(INFO) << "Verify parity egroup " << primary_parity_egroup_id
              << " contents";

    CompareSubregionChecksums(parity_vec.at(0), slice_state_vec);
    ReplicaReadEgroup(
      primary_parity_egroup_id, primary_parity_disk_id, slice_state_vec,
      &transformed_data);

    CHECK_EQ(transformed_data->size(), parity_vec.at(0)->size());
    CHECK(IOBufferUtil::Memcmp(transformed_data.get(),
                               parity_vec.at(0).get()));

    slice_state_vec = *secondary_parity_slice_state_vec;
    FilterUnAllocatedSlices(&slice_state_vec);

    LOG(INFO) << "Verify parity egroup " << secondary_parity_egroup_id
              << " contents";

    CompareSubregionChecksums(parity_vec.at(1), slice_state_vec);
    ReplicaReadEgroup(
      secondary_parity_egroup_id, secondary_parity_disk_id, slice_state_vec,
      &transformed_data);

    CHECK_EQ(transformed_data->size(), parity_vec.at(1)->size());
    CHECK(IOBufferUtil::Memcmp(transformed_data.get(),
                               parity_vec.at(1).get()));
  }
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::OverwriteErasureCodedInfoEgroupHelper(
  const int64 info_egroup_id,
  const int64 info_disk_id,
  const int64 primary_parity_egroup_id,
  const int64 primary_parity_disk_id,
  const int64 secondary_parity_egroup_id,
  const int64 secondary_parity_disk_id,
  const vector<int64>& info_egroup_ids,
  vector<SliceState::Ptr> *info_slice_state_vec,
  vector<SliceState::Ptr> *primary_parity_slice_state_vec,
  vector<SliceState::Ptr> *secondary_parity_slice_state_vec,
  vector<shared_ptr<EgroupOpsTester::Extent>> *info_extent_state_vec,
  const bool compressible_data,
  const int32 compress_percentage,
  const bool inject_corruption) {

  string transformation_type = compressible_data ? "c=low" : "";
  if (!FLAGS_egroup_encryption_type.empty()) {
    if (!transformation_type.empty()) {
      transformation_type += ",";
    }
    transformation_type += FLAGS_egroup_encryption_type;
  }
  vector<RegionDescriptor> region_to_write_vec;
  const int32 overwrite_begin = Random::TL()->Uniform(0, 768 * 1024);
  const int32 overwrite_size = Random::TL()->Uniform(1 * 1024, 256 * 1024);
  LOG(INFO) << "Performing overwrite on region: (" << overwrite_begin
            << ", " << overwrite_size << ")";
  region_to_write_vec.emplace_back(
    overwrite_begin, overwrite_size, compress_percentage);

  const vector<int64> parity_egroup_ids =
    { primary_parity_egroup_id, secondary_parity_egroup_id };
  const vector<int64> parity_disk_ids =
    { primary_parity_disk_id, secondary_parity_disk_id };
  const int64 base_info_intent_sequence =
    egroup_id_intent_seq_map_[info_egroup_id];
  const int64 base_primary_parity_intent_sequence =
    egroup_id_intent_seq_map_[primary_parity_egroup_id];
  const int64 base_secondary_parity_intent_sequence =
    egroup_id_intent_seq_map_[secondary_parity_egroup_id];

  LOG(INFO) << "Egroup id and intent sequence "
            << info_egroup_id << ":" << base_info_intent_sequence << ", "
            << primary_parity_egroup_id << ":"
            << base_primary_parity_intent_sequence << ", "
            << secondary_parity_egroup_id << ":"
            << base_secondary_parity_intent_sequence;

  // Prepare structures for the write requests.
  const int num_writes = FLAGS_stargate_ideal_extent_group_size_MB;
  vector<Notification::Ptr> notifications;
  notifications.reserve(num_writes);

  vector<vector<SliceState::Ptr>> tmp_info_slice_state_vecs;
  tmp_info_slice_state_vecs.reserve(num_writes);

  vector<vector<shared_ptr<EgroupOpsTester::Extent>>>
    tmp_info_extent_state_vecs;
  tmp_info_extent_state_vecs.reserve(num_writes);

  vector<vector<vector<SliceState::Ptr>>> tmp_parity_slice_state_vecs;
  tmp_parity_slice_state_vecs.reserve(num_writes);

  vector<IOBuffer::Ptr> data_to_write_vec;
  data_to_write_vec.reserve(num_writes);

  for (int ii = 0; ii < num_writes; ++ii) {
    tmp_info_slice_state_vecs.push_back(*info_slice_state_vec);
    tmp_info_extent_state_vecs.push_back(*info_extent_state_vec);
    vector<SliceState::Ptr> tmp_primary_parity_slice_state_vec;
    tmp_parity_slice_state_vecs.push_back(
      {*primary_parity_slice_state_vec, *secondary_parity_slice_state_vec});
    notifications.push_back(make_shared<Notification>());
    data_to_write_vec.push_back(make_shared<IOBuffer>());

    // Update the intent sequence to use for the write.
    egroup_id_intent_seq_map_[info_egroup_id] = ii + base_info_intent_sequence;
    egroup_id_intent_seq_map_[primary_parity_egroup_id] =
      ii + base_primary_parity_intent_sequence;
    egroup_id_intent_seq_map_[secondary_parity_egroup_id] =
      ii + base_secondary_parity_intent_sequence;

    const bool corrupt_write = inject_corruption && (ii == num_writes - 1);
    LOG_IF(INFO, corrupt_write) << "Corrupting write with intent sequence "
                                << ii + base_info_intent_sequence
                                << " on egroup " << info_egroup_id;

    OverwriteSingleExtentInErasureCodedEgroup(
      info_egroup_id,
      info_disk_id,
      info_egroup_ids,
      parity_egroup_ids,
      parity_disk_ids,
      ii,
      transformation_type,
      vector<DataTransformation::Type>(),
      region_to_write_vec,
      notifications.at(ii).get(),
      &tmp_info_slice_state_vecs.at(ii),
      &tmp_info_extent_state_vecs.at(ii),
      &tmp_parity_slice_state_vecs.at(ii),
      &data_to_write_vec.at(ii),
      corrupt_write);

    // Updated the expected intent sequence for the info and parity egroups.
    egroup_id_expected_intent_seq_map_[info_egroup_id] =
      egroup_id_intent_seq_map_[info_egroup_id];
    egroup_id_expected_intent_seq_map_[primary_parity_egroup_id] =
      egroup_id_intent_seq_map_[primary_parity_egroup_id];
    egroup_id_expected_intent_seq_map_[secondary_parity_egroup_id] =
      egroup_id_intent_seq_map_[secondary_parity_egroup_id];
  }

  IOBuffer::Ptr data_written = make_shared<IOBuffer>();
  unordered_map<int32, SliceState::Ptr> slice_id_to_state;
  unordered_map<string, shared_ptr<EgroupOpsTester::Extent>> extent_to_state;
  MergeInfoSliceAndExtentStates(
    &slice_id_to_state, &extent_to_state, *info_slice_state_vec,
    *info_extent_state_vec);
  vector<vector<SliceState::Ptr> *> parity_slice_state_vec
    = { primary_parity_slice_state_vec, secondary_parity_slice_state_vec };
  // Wait for the writes to finish and merge the slice and extent information
  // written.
  for (int ii = 0; ii < num_writes; ++ii) {
    LOG(INFO) << "Waiting for write " << ii;
    notifications[ii]->Wait();
    data_written->AppendIOBuffer(data_to_write_vec[ii].get());
    MergeInfoSliceAndExtentStates(
      &slice_id_to_state, &extent_to_state, tmp_info_slice_state_vecs[ii],
      tmp_info_extent_state_vecs[ii]);
    MergeOldAndNewParitySliceStates(
      &parity_slice_state_vec, tmp_parity_slice_state_vecs[ii]);
  }

  ConvertSliceAndExtentMapsToVectors(
    slice_id_to_state, extent_to_state, info_slice_state_vec,
    info_extent_state_vec);
  CHECK_GT(data_written->size(), 0);

  sort(info_slice_state_vec->begin(),
       info_slice_state_vec->end(),
       medusa::SliceState::SliceStatePtrOrderIncreasingFileOffset);

  // Read the data in the egroup to verify that it was written correctly.
  LOG(INFO) << "Verify info egroup " << info_egroup_id << " contents";
  set<StargateError::Type> expected_errors = { StargateError::kNoError };
  if (inject_corruption) {
    expected_errors.insert(StargateError::kDataCorrupt);
  }
  vector<Notification::Ptr> notifications_read;
  vector<pair<int64, int64>> region_to_read_vec(
    1, make_pair(overwrite_begin, overwrite_size));
  vector<SliceState::Ptr> slice_state_vec = *info_slice_state_vec;
  FilterUnAllocatedSlices(&slice_state_vec);
  for (int ii = 0; ii < num_writes; ++ii) {
    // Find the extent state corresponding to this vblock.
    shared_ptr<EgroupOpsTester::Extent> extent;
    for (uint jj = 0; jj < info_extent_state_vec->size(); ++jj) {
      const int32 vblock_num =
        info_extent_state_vec->at(jj)->extent_id().vdisk_block();
      if (vblock_num == static_cast<int32>(ii)) {
        extent = info_extent_state_vec->at(jj);
        break;
      }
    }
    CHECK(extent);
    CHECK_LE(ii * overwrite_size + overwrite_size, data_written->size());
    IOBuffer::Ptr region_data =
      data_written->Clone(ii * overwrite_size, overwrite_size);
    ReadExtentGroup(
      info_egroup_id,
      info_disk_id,
      notifications_read,
      region_to_read_vec,
      slice_state_vec,
      { extent },
      region_data,
      region_to_read_vec[0].first /* data_write_start_offset */,
      compressible_data,
      true /* wait_for_read */,
      expected_errors);
  }
}

//-----------------------------------------------------------------------------
static void ValidateExtentStates(
  const GetEgroupStateRet& egroup_state,
  const vector<shared_ptr<EgroupOpsTester::Extent>>&
    expected_extent_state_vec) {

  for (int ii = 0; ii < egroup_state.extents_size(); ++ii) {
    const auto& extent_info = egroup_state.extents(ii);
    for (uint jj = 0; jj < expected_extent_state_vec.size(); ++jj) {
      const EgroupOpsTester::Extent& expected_extent_info =
        *expected_extent_state_vec.at(jj);
      CHECK_EQ(expected_extent_info.extent_id().owner_id(),
               extent_info.extent_id().owner_id())
        << egroup_state.ShortDebugString();
      if (expected_extent_info.extent_id().vdisk_block() !=
          extent_info.extent_id().vdisk_block()) {
        continue;
      }
      // Found the expected extent, ensure that the diff data location doesn't
      // contain any information other than the expected.
      const ExtentDataLocation& expected_data_location =
        expected_extent_info.diff_data_location();
      const ExtentDataLocation& new_data_location =
        extent_info.diff_data_location();
      CHECK_GE(expected_data_location.slice_ids_size(),
               new_data_location.slice_ids_size())
        << egroup_state.ShortDebugString();
      CHECK_EQ(new_data_location.slice_ids_size(),
               new_data_location.slice_indices_size())
        << egroup_state.ShortDebugString();
      for (int kk = 0; kk < new_data_location.slice_ids_size(); ++kk) {
        const int32 slice_indx = new_data_location.slice_indices(kk);
        const int32 new_slice_id = new_data_location.slice_ids(kk);
        const int32 expected_slice_id =
          expected_data_location.slice_ids(slice_indx);
        CHECK_EQ(new_slice_id, expected_slice_id)
          << egroup_state.ShortDebugString();
      }
      // We found and validated the extent state, break out of the loop.
      break;
    }
  }
}

//-----------------------------------------------------------------------------

static void ValidateSliceStates(
  const GetEgroupStateRet& egroup_state,
  const vector<SliceState::Ptr>& expected_slice_state_vec) {

  map<int32, vector<uint>> slice_id_cksum_map;
  // Gather checksums for the slices.
  for (int ii = 0; ii < egroup_state.slices_size(); ++ii) {
    const auto& slice_state = egroup_state.slices(ii);
    for (int jj = 0; jj < slice_state.checksum_size(); ++jj) {
      slice_id_cksum_map[slice_state.slice_id()].emplace_back(
        slice_state.checksum(jj));
    }
  }

  // Iterate over the slices in the expected slice state vec. If the slice
  // is present in the slices in 'egroup_state', then the checksums match up.
  for (uint ii = 0; ii < expected_slice_state_vec.size(); ++ii) {
    const auto& slice_state = expected_slice_state_vec.at(ii);
    if (slice_id_cksum_map.count(slice_state->slice_id()) == 0) {
      continue;
    }
    const vector<uint>& chksums = slice_id_cksum_map[slice_state->slice_id()];
    CHECK_EQ(slice_state->checksum_size(), static_cast<int32>(chksums.size()));
    for (uint jj = 0; jj < chksums.size(); ++jj) {
      CHECK_EQ(chksums[jj], slice_state->checksum(jj))
        << "Checksum mismatch for slice " << slice_state->slice_id()
        << " at subregion " << jj;
    }
  }
}

//-----------------------------------------------------------------------------

static void CheckpointWALDone(const bool success, Notification *notification) {
  CHECK(success);
  notification->Notify();
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::RollbackErasureCodedEgroupHelper(
  const int64 info_egroup_id,
  const int64 info_disk_id,
  vector<SliceState::Ptr> *expected_info_slice_state_vec,
  vector<shared_ptr<EgroupOpsTester::Extent>>
    *expected_info_extent_state_vec,
  const int64 primary_parity_egroup_id,
  const int64 primary_parity_disk_id,
  vector<SliceState::Ptr> *expected_primary_parity_slice_state_vec,
  const int64 secondary_parity_egroup_id,
  const int64 secondary_parity_disk_id,
  vector<SliceState::Ptr> *expected_secondary_parity_slice_state_vec,
  const int32 num_updates_to_rollback,
  const bool compressible_data,
  const bool rolling_back_corrupt_egroup,
  const bool test_rollback_after_crash) {

  const string debug_string =
    StringJoin(" info_egroup_id ", info_egroup_id,
               " info_disk_id ", info_disk_id,
               " primary_parity_egroup_id ", primary_parity_egroup_id,
               " primary_parity_disk_id ", primary_parity_disk_id,
               " secondary_parity_egroup_id ", secondary_parity_egroup_id,
               " secondary_parity_disk_id ", secondary_parity_disk_id,
               " num_updates_to_rollback ", num_updates_to_rollback,
               " compressible_data ", compressible_data,
               " rolling_back_corrupt_egroup ", rolling_back_corrupt_egroup,
               " test_rollback_after_crash ", test_rollback_after_crash);
  LOG(INFO) << __FUNCTION__ << debug_string;

  vector<int64> egroup_ids =
    { info_egroup_id, primary_parity_egroup_id, secondary_parity_egroup_id };
  vector<int64> disk_ids =
    { info_disk_id, primary_parity_disk_id, secondary_parity_disk_id };
  vector<int64> rollback_to_intent_sequences =
    { egroup_id_intent_seq_map_[info_egroup_id] - num_updates_to_rollback,
      egroup_id_intent_seq_map_[primary_parity_egroup_id] -
        num_updates_to_rollback,
      egroup_id_intent_seq_map_[secondary_parity_egroup_id] -
        num_updates_to_rollback };

  vector<Notification::Ptr> notifications;
  notifications.reserve(egroup_ids.size());
  for (uint ii = 0; ii < egroup_ids.size(); ++ii) {
    notifications.push_back(make_shared<Notification>());
  }

  // If we have been asked to rollback a corrupt egroup, ensure that the egroup
  // is really corrupt.
  if (rolling_back_corrupt_egroup) {
    GetEgroupStateRet egroup_state;
    GetEgroupState(info_egroup_id, info_disk_id,
                   egroup_id_intent_seq_map_[info_egroup_id] + 1,
                   &egroup_state, false /* managed_by_aes*/,
                   false /* extent_based_format */, nullptr /* error_ret */,
                   false /* set_latest_applied_intent_sequence */,
                   false /* fetch_all_metadata */,
                   10000 /* rpc_timeout_msecs */);
    ++egroup_id_intent_seq_map_[info_egroup_id];
    CHECK(egroup_state.is_corrupt()) << debug_string;
  }
  if (test_rollback_after_crash) {
    SetStargateGFlag(
      "estore_experimental_crash_after_adding_rollback_tu", "true");
  }

  // Once a while we will for a check point and then try rolling back.
  const bool checkpoint_before_rollback =
    test_rollback_after_crash || (Random::TL()->Uniform(0, 2) == 0);
  if (checkpoint_before_rollback) {
    for (uint ii = 0; ii < egroup_ids.size(); ++ii) {
      const int64 disk_id = disk_ids[ii];
      LOG(INFO) << "Checkpoint disk WAL on " << disk_id << " before rollback";
      notifications[ii]->Reset();
      Function<void(bool)> done_cb =
        bind(&CheckpointWALDone, _1, notifications[ii].get());
      CheckpointDiskWAL(disk_id, done_cb);
      notifications[ii]->Wait();
    }
  }

  // Fire rollback on the info and parity extent groups.
  for (uint ii = 0; ii < egroup_ids.size(); ++ii) {
    // We will simulate a crash while rolling back to test the flow via
    // tentative resolver op too.
    const StargateError::Type expected_error =
      test_rollback_after_crash ? StargateError::kTransportError :
        StargateError::kNoError;
    const bool is_corrupt =
      rolling_back_corrupt_egroup ? (egroup_ids[ii] == info_egroup_id) : false;

    notifications[ii]->Reset();
    RollbackExtentGroup(
      egroup_ids[ii], disk_ids[ii], rollback_to_intent_sequences[ii],
      notifications[ii].get(), expected_error, is_corrupt);
  }

  // Wait for rollback to complete on all.
  for (uint ii = 0; ii < notifications.size(); ++ii) {
    notifications[ii]->Wait();
  }

  if (rolling_back_corrupt_egroup) {
    // Now we need to sync slices with the estore to mark the extent group not
    // corrupt.
    vector<DataTransformation::Type> transformation_type_vec;
    const string& transformation_type = compressible_data ? "c=low" : "";
    SyncEgroupSlicesAndVerify(
      info_egroup_id, info_disk_id, *expected_info_slice_state_vec,
      transformation_type, transformation_type_vec,
      true /* mark_not_corrupt */);
    rollback_to_intent_sequences[0] =
      egroup_id_intent_seq_map_[info_egroup_id];
  }
  if (test_rollback_after_crash) {
    // Restart all the stargates.
    unordered_set<int64> restarted_stargates;
    for (uint ii = 0; ii < disk_ids.size(); ++ii) {
      const int32 stargate_indx =
        cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(disk_ids[ii]);
     if (restarted_stargates.count(stargate_indx) > 0) {
      continue;
     }

     RestartStargateAtIndex(stargate_indx);
     restarted_stargates.insert(stargate_indx);
    }
    SetStargateGFlag(
      "estore_experimental_crash_after_adding_rollback_tu", "false");
  }

  // Now that the rollbacks have finished validate the egroup states.
  vector<vector<SliceState::Ptr> *> expected_slice_state_vecs =
    { expected_info_slice_state_vec, expected_primary_parity_slice_state_vec,
      expected_secondary_parity_slice_state_vec };
  vector<vector<shared_ptr<EgroupOpsTester::Extent>> *>
    expected_info_extent_state_vecs =
      { expected_info_extent_state_vec, nullptr, nullptr };

  for (uint ii = 0; ii < egroup_ids.size(); ++ii) {
    const int64 egroup_id = egroup_ids.at(ii);
    const int64 disk_id = disk_ids.at(ii);
    GetEgroupStateRet egroup_state;
    GetEgroupState(egroup_id, disk_id,
                   egroup_id_intent_seq_map_[egroup_id] + 1,
                   &egroup_state, false /* managed_by_aes */,
                   false /* extent_based_format */, nullptr /* error_ret */,
                   false /* set_latest_applied_intent_sequence */);
    CHECK(!egroup_state.is_corrupt()) << egroup_id << " " << debug_string;
    CHECK_EQ(egroup_state.latest_applied_intent_sequence(),
             rollback_to_intent_sequences[ii])
      << egroup_id << " " <<debug_string;
    CHECK_GE(egroup_state.extents_size(), 0)
      << egroup_id << " " << debug_string;
    ++egroup_id_intent_seq_map_[egroup_id];
    egroup_id_expected_intent_seq_map_[egroup_id] =
      rollback_to_intent_sequences[ii];
    if (expected_info_extent_state_vecs.at(ii)) {
      ValidateExtentStates(
        egroup_state, *(expected_info_extent_state_vecs.at(ii)));
    }
    if ((!rolling_back_corrupt_egroup || ii != 0) &&
        expected_slice_state_vecs.at(ii)) {
      ValidateSliceStates(egroup_state, *(expected_slice_state_vecs.at(ii)));
    }
  }
}

//-----------------------------------------------------------------------------

void RollbackExtentGroupDone(
  StargateError::Type err,
  const shared_ptr<string>& err_detail,
  const shared_ptr<RollbackExtentGroupRet>& ret,
  Notification *const notification,
  const int64 egroup_id,
  StargateError::Type expected_error,
  const bool is_corrupt) {

  CHECK(notification);

  LOG(INFO) << "Rollback on extent group " << egroup_id << " finished with "
            << "error " << err;
  CHECK_EQ(err, expected_error) << err_detail;
  CHECK_EQ(ret->is_corrupt(), is_corrupt);
  notification->Notify();
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::RollbackExtentGroup(
  const int64 egroup_id,
  const int64 disk_id,
  const int64 rollback_to_intent_sequence,
  Notification *const notification,
  const StargateError::Type expected_error,
  const bool is_corrupt) {

  CHECK(notification);

  // Get stargate interface associated with the disk.
  const StargateInterface::Ptr& iface =
    cluster_mgr_->stargate_mgr()->GetStargateInterfaceForDisk(disk_id);

  shared_ptr<RollbackExtentGroupArg> arg =
    make_shared<RollbackExtentGroupArg>();
  arg->set_extent_group_id(egroup_id);
  arg->set_disk_id(disk_id);
  const int64 current_intent_sequence =
    egroup_id_expected_intent_seq_map_[egroup_id];
  arg->set_expected_intent_sequence(current_intent_sequence);
  arg->set_intent_sequence(
    ++egroup_id_intent_seq_map_[egroup_id]);
  arg->set_rollback_to_intent_sequence(rollback_to_intent_sequence);
  egroup_id_expected_intent_seq_map_[egroup_id] =
    egroup_id_intent_seq_map_[egroup_id];
  Function<void(StargateError::Type,
                shared_ptr<string>,
                shared_ptr<RollbackExtentGroupRet>)> done_cb =
    func_disabler_.Bind(&RollbackExtentGroupDone, _1, _2, _3,
                       notification, egroup_id, expected_error, is_corrupt);

  notification->Reset();
  LOG(INFO) << "Sending RollbackExtentGroup to " << iface->peer_handle();
  iface->RollbackExtentGroup(arg, done_cb, 100000 /* rpc_timeout_msecs */);
}


//-----------------------------------------------------------------------------

void EgroupOpsTester::WriteErasureCodedStripHelper(
  const vector<int64>& info_egroup_ids,
  const vector<int64>& info_disk_ids,
  const int64 primary_parity_egroup_id,
  const int64 primary_parity_disk_id,
  const int64 secondary_parity_egroup_id,
  const int64 secondary_parity_disk_id,
  vector<vector<SliceState::Ptr>> *info_slice_state_vecs,
  vector<SliceState::Ptr> *primary_parity_slice_state_vec,
  vector<SliceState::Ptr> *secondary_parity_slice_state_vec,
  vector<vector<shared_ptr<EgroupOpsTester::Extent>>> *info_extent_state_vecs,
  const bool compressible_data,
  const int32 compress_percentage,
  const bool managed_by_aes,
  const int32 primary_parity_inject_error_type,
  const int32 secondary_parity_inject_error_type,
  vector<shared_ptr<ReplicaReadEgroupRet>> *const remote_ret_vec,
  vector<IOBuffer::Ptr> *const parity_data_vec) {

  CHECK_EQ(info_egroup_ids.size(), info_disk_ids.size());

  info_slice_state_vecs->reserve(info_egroup_ids.size());
  info_extent_state_vecs->reserve(info_egroup_ids.size());
  vector<IOBuffer::Ptr> info_vec;
  unordered_map<int64, shared_ptr<ReplicaReadEgroupRet>> info_egroup_map;

  for (uint ii = 0; ii < info_egroup_ids.size(); ++ii) {
    const int64 info_egroup_id = info_egroup_ids[ii];
    const int64 info_disk_id = info_disk_ids[ii];

    egroup_id_intent_seq_map_[info_egroup_id] = 0;

    info_slice_state_vecs->emplace_back(vector<SliceState::Ptr>());
    info_extent_state_vecs->emplace_back(
      vector<shared_ptr<EgroupOpsTester::Extent>>());

    WriteErasureCodedInfoEgroupHelper(
      info_egroup_id,
      info_disk_id,
      primary_parity_egroup_id,
      primary_parity_disk_id,
      secondary_parity_egroup_id,
      secondary_parity_disk_id,
      info_egroup_ids,
      &info_slice_state_vecs->at(ii),
      primary_parity_slice_state_vec,
      secondary_parity_slice_state_vec,
      &info_extent_state_vecs->at(ii),
      compressible_data,
      compress_percentage,
      managed_by_aes);

    sort(info_slice_state_vecs->at(ii).begin(),
         info_slice_state_vecs->at(ii).end(),
         medusa::SliceState::SliceStatePtrOrderIncreasingFileOffset);

    // Read the transformed data back from the info egroup.
    IOBuffer::Ptr transformed_data = make_shared<IOBuffer>();
    vector<SliceState::Ptr> slice_state_vec = info_slice_state_vecs->at(ii);
    FilterUnAllocatedSlices(&slice_state_vec);
    if (!managed_by_aes) {
      ReplicaReadEgroup(
        info_egroup_id, info_disk_id, slice_state_vec, &transformed_data);
    } else {
      shared_ptr<ReplicaReadEgroupRet> remote_ret;
      ReplicaReadEgroup(
        info_egroup_id, info_disk_id,
        vector<shared_ptr<EgroupOpsTester::SliceState>>()
          /* slice_state_vec */,
        &transformed_data,
        StargateError::kNoError /* expected_error */,
        true /* managed_by_aes */, nullptr /* user_notification */,
        &remote_ret);
      CHECK(remote_ret && remote_ret.get()) << info_egroup_id << " idx " << ii;
      CHECK(remote_ret->has_extent_group_metadata())
        << transformed_data->size()
        << " " << info_egroup_id;
      if (remote_ret_vec) {
        remote_ret_vec->emplace_back(remote_ret);
      }
      info_egroup_map[info_egroup_id] = move(remote_ret);
    }

    // Fill any cushion in the extent group with zeros.
    transformed_data =
      FillGapBetweenSlices(slice_state_vec, transformed_data);
    CHECK(transformed_data && transformed_data->size() > 0);
    info_vec.emplace_back(move(transformed_data));

    // Updated the expected intent sequence for the info egroup.
    egroup_id_expected_intent_seq_map_[info_egroup_id] =
      egroup_id_intent_seq_map_[info_egroup_id];
  }

  if (primary_parity_egroup_id < 0) {
    return;
  }

  egroup_id_intent_seq_map_[primary_parity_egroup_id] = 0;
  egroup_id_intent_seq_map_[secondary_parity_egroup_id] = 0;
  Notification notification;
  const vector<int64> parity_egroup_ids =
    { primary_parity_egroup_id, secondary_parity_egroup_id };

  // Compute the parity data.
  vector<IOBuffer::Ptr> parity_vec =
    { make_shared<IOBuffer>(), make_shared<IOBuffer>() };

  StargateParity parity_encoder;
  parity_encoder.Encode(info_vec, parity_vec);

  LOG(INFO) << "Writing primary parity egroup " << primary_parity_egroup_id
            << " to disk " << primary_parity_disk_id;

  int64 primary_parity_svm_id = -1;
  if (primary_parity_inject_error_type > -1) {
    // Set the stargate gflag to inject corruption on this write.
    primary_parity_svm_id = cluster_mgr_->stargate_mgr()->GetSvmIdForStargate(
      cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(
        primary_parity_disk_id));
    cluster_mgr_->stargate_mgr()->SetGFlag(
      "estore_experimental_slice_write_inject_crash",
      to_string(primary_parity_inject_error_type), primary_parity_svm_id);
    LOG(INFO) << "Injected error " << primary_parity_inject_error_type
              << " on primary parity disk id " << primary_parity_disk_id
              << " svm id " << primary_parity_svm_id;
  }

  WriteParityExtentGroup(
    primary_parity_egroup_id,
    primary_parity_disk_id,
    true /* is_primary_parity */,
    info_egroup_ids,
    parity_egroup_ids,
    parity_vec.at(0),
    &notification,
    primary_parity_slice_state_vec,
    managed_by_aes,
    &info_egroup_map,
    false /* decode_request */,
    primary_parity_inject_error_type);

  LOG(INFO) << "Writing secondary parity egroup " << secondary_parity_egroup_id
            << " to disk " << secondary_parity_disk_id;

  int64 secondary_parity_svm_id = -1;
  if (secondary_parity_inject_error_type > -1) {
    // Set the stargate gflag to inject corruption on this write.
    secondary_parity_svm_id =
      cluster_mgr_->stargate_mgr()->GetSvmIdForStargate(
        cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(
          secondary_parity_disk_id));
    CHECK_NE(primary_parity_svm_id, secondary_parity_svm_id);
    cluster_mgr_->stargate_mgr()->SetGFlag(
      "estore_experimental_slice_write_inject_crash",
      to_string(secondary_parity_inject_error_type), secondary_parity_svm_id);
    LOG(INFO) << "Injected error " << secondary_parity_inject_error_type
              << " on secondary parity disk id " << secondary_parity_disk_id
              << " svm " << secondary_parity_svm_id;
  }

  WriteParityExtentGroup(
    secondary_parity_egroup_id,
    secondary_parity_disk_id,
    false /* is_primary_parity */,
    info_egroup_ids,
    parity_egroup_ids,
    parity_vec.at(1),
    &notification,
    secondary_parity_slice_state_vec,
    managed_by_aes,
    &info_egroup_map,
    false /* decode_request */,
    secondary_parity_inject_error_type);

  // Updated the expected intent sequence for the parity egroups.
  egroup_id_expected_intent_seq_map_[primary_parity_egroup_id] =
    egroup_id_intent_seq_map_[primary_parity_egroup_id];
  egroup_id_expected_intent_seq_map_[secondary_parity_egroup_id] =
    egroup_id_intent_seq_map_[secondary_parity_egroup_id];

  if (parity_data_vec) {
    for (auto& io_buf : parity_vec) {
      parity_data_vec->emplace_back(move(io_buf));
    }
  }
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::WriteErasureCodedInfoEgroupHelper(
  const int64 info_egroup_id,
  const int64 info_disk_id,
  const int64 primary_parity_egroup_id,
  const int64 primary_parity_disk_id,
  const int64 secondary_parity_egroup_id,
  const int64 secondary_parity_disk_id,
  const vector<int64>& info_egroup_ids,
  vector<SliceState::Ptr> *info_slice_state_vec,
  vector<SliceState::Ptr> *primary_parity_slice_state_vec,
  vector<SliceState::Ptr> *secondary_parity_slice_state_vec,
  vector<shared_ptr<EgroupOpsTester::Extent>> *info_extent_state_vec,
  const bool compressible_data,
  const int32 compress_percentage,
  const bool managed_by_aes) {

  // Perform the preliminary checks.
  CHECK(info_slice_state_vec);
  CHECK(primary_parity_slice_state_vec);
  CHECK(secondary_parity_slice_state_vec);

  Notification notification;
  string transformation_type = compressible_data ? "c=low" : "";
  if (!FLAGS_egroup_encryption_type.empty()) {
    if (!transformation_type.empty()) {
      transformation_type += ",";
    }
    transformation_type += FLAGS_egroup_encryption_type;
  }
  vector<RegionDescriptor> region_to_write_vec;
  region_to_write_vec.emplace_back(
    0, FLAGS_egroup_test_chunk_size, compress_percentage);

  LOG(INFO) << "Writing info egroup " << info_egroup_id << " to disk "
            << info_disk_id;
  IOBuffer::Ptr data_to_write = make_shared<IOBuffer>();
  const int64 base_intent_sequence = egroup_id_intent_seq_map_[info_egroup_id];

  unordered_map<int32, SliceState::Ptr> slice_id_to_state;
  unordered_map<string, shared_ptr<EgroupOpsTester::Extent>> extent_to_state;
  for (int ii = 0; ii < FLAGS_stargate_ideal_extent_group_size_MB; ++ii) {
    const int64 current_intent_sequence = ii + base_intent_sequence;
    egroup_id_intent_seq_map_[info_egroup_id] = current_intent_sequence;
    vector<SliceState::Ptr> tmp_info_slice_state_vec;
    vector<shared_ptr<EgroupOpsTester::Extent>> tmp_info_extent_state_vec;
    WriteSingleExtentInExtentGroup(
      info_egroup_id,
      info_disk_id,
      -1,
      ii /* vblock_num */,
      transformation_type,
      vector<DataTransformation::Type>(),
      region_to_write_vec,
      &notification,
      &tmp_info_slice_state_vec,
      &tmp_info_extent_state_vec,
      data_to_write,
      nullptr,
      false /* wait_for_write */,
      -1 /* latest_applied_intent_sequence */,
      Optional<bool>() /* is_sequential */,
      managed_by_aes);
    notification.Wait();

    if (managed_by_aes) {
      continue;
    }
    // Ensure that new extents and slices were written.
    CHECK_GT(tmp_info_slice_state_vec.size(), 0);
    CHECK_GT(tmp_info_extent_state_vec.size(), 0);

    // Merge the new extent and slice information with exisiting extent and
    // slice information.
    MergeInfoSliceAndExtentStates(
      &slice_id_to_state, &extent_to_state, tmp_info_slice_state_vec,
      tmp_info_extent_state_vec);
  }

  if (!managed_by_aes) {
    ConvertSliceAndExtentMapsToVectors(
      slice_id_to_state, extent_to_state, info_slice_state_vec,
      info_extent_state_vec);
  } else {
    // Issue a ReplicaRead rpc to fetch the AES egroup metadata and copy
    // the result to the slice and extent state vectors.
    IOBuffer::Ptr transformed_data = make_shared<IOBuffer>();
    shared_ptr<ReplicaReadEgroupRet> remote_ret;
    ReplicaReadEgroup(
      info_egroup_id, info_disk_id,
      vector<shared_ptr<EgroupOpsTester::SliceState>>() /* slice_state_vec */,
      &transformed_data,
      StargateError::kNoError /* expected_error */,
      true /* managed_by_aes */, nullptr /* user_notification */,
      &remote_ret);
    CHECK_GT(transformed_data->size(), 0);

    CHECK(remote_ret && remote_ret.get());
    CHECK(remote_ret->has_extent_group_metadata());

    // Copy the received AES metadata to the outgoing slice and extent
    // state params.
    CopyAESMetadata(remote_ret, info_slice_state_vec, info_extent_state_vec);
  }

  CHECK_GT(info_slice_state_vec->size(), 0);
  CHECK_GT(info_extent_state_vec->size(), 0);
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::WriteExtentGroup(
  const int64 egroup_id,
  const int64 primary_disk_id,
  const int64 secondary_disk_id,
  const int num_vblocks_to_write,
  vector<Notification::Ptr> *notifications,
  const vector<pair<int64, int64>>& offset_size_to_write,
  vector<SliceState::Ptr> *slice_state_vec,
  vector<shared_ptr<EgroupOpsTester::Extent>> *extent_state_vec,
  const IOBuffer::Ptr& data_to_write,
  const bool compressible_data,
  const int32 compress_percentage,
  const bool managed_by_aes,
  map<StargateError::Type, int> *expected_errors,
  const bool wait_for_write) {

  // Perform the preliminary checks.
  CHECK_LE(num_vblocks_to_write, FLAGS_stargate_ideal_extent_group_size_MB);
  CHECK(slice_state_vec);
  CHECK(notifications);
  notifications->resize(num_vblocks_to_write);

  // This should be initialized to zero by the caller, except for overwrites
  // (e.g., in TestExtentStoreOverwrites).
  const int64 base_intent_sequence = egroup_id_intent_seq_map_[egroup_id];
  string transformation_type = compressible_data ? "c=low" : "";
  if (!FLAGS_egroup_encryption_type.empty()) {
    if (!transformation_type.empty()) {
      transformation_type += ",";
    }
    transformation_type += FLAGS_egroup_encryption_type;
  }
  vector<RegionDescriptor> region_to_write_vec;
  for (const pair<int64, int64>& offset_size_pair : offset_size_to_write) {
    region_to_write_vec.emplace_back(offset_size_pair.first,
                                     offset_size_pair.second,
                                     compress_percentage);
  }

  for (int ii = 0; ii < num_vblocks_to_write; ++ii) {
    data_to_write->Clear();
    (*notifications)[ii] = make_shared<Notification>();
    egroup_id_intent_seq_map_[egroup_id] =
      managed_by_aes ? base_intent_sequence : ii + base_intent_sequence;
    WriteSingleExtentInExtentGroup(egroup_id,
                                   primary_disk_id,
                                   secondary_disk_id,
                                   ii /* vblock_num */,
                                   transformation_type,
                                   vector<DataTransformation::Type>(),
                                   region_to_write_vec,
                                   notifications->at(ii).get(),
                                   slice_state_vec,
                                   extent_state_vec,
                                   data_to_write,
                                   expected_errors,
                                   wait_for_write,
                                   -1 /* latest_applied_intent_sequence */,
                                   Optional<bool>() /* is_sequential */,
                                   managed_by_aes);
  }
}

//-----------------------------------------------------------------------------

static void ReadMixedCompressionExtentGroupDone(
  StargateError::Type err,
  shared_ptr<string> err_detail,
  shared_ptr<ReadExtentGroupRet> ret,
  const IOBuffer::Ptr&& data_read,
  Notification *const notify,
  const IOBuffer::Ptr& data_written) {

  // Check for possible read errors.
  CHECK_EQ(err, StargateError::kNoError);

  // Verify if we get the right amount of data.
  CHECK_EQ(data_read->size(), data_written->size());
  // Verify the data read is the same as the data written.
  CHECK(IOBufferUtil::Memcmp(data_read.get(), data_written.get()));

  if (notify) {
    notify->Notify();
  }
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::ReadMixedCompressionExtentGroup(
  const int64 egroup_id,
  const int64 primary_disk_id,
  Notification *const notification,
  const vector<RegionDescriptor>& region_vec,
  const vector<shared_ptr<EgroupOpsTester::SliceState>>& slice_state_vec,
  const vector<shared_ptr<EgroupOpsTester::Extent>>& extent_state_vec,
  const IOBuffer::Ptr& data_written,
  const vector<RegionDescriptor>& region_to_write_vec,
  const int64 vblock_num,
  const bool managed_by_aes) {

  CHECK(notification);
  CHECK(data_written);

  // Get stargate interface associated with the primary disk.
  const StargateInterface::Ptr& iface =
    cluster_mgr_->stargate_mgr()->GetStargateInterfaceForDisk(primary_disk_id);
  CHECK(iface);

  // Prepare the read request.
  shared_ptr<ReadExtentGroupArg> arg = make_shared<ReadExtentGroupArg>();
  arg->set_extent_group_id(egroup_id);
  arg->set_qos_principal_name(string());
  arg->set_qos_priority(StargateQosPriority::kRead);
  arg->set_owner_vdisk_id(vdisk_id_);
  arg->set_owner_container_id(container_id_);
  arg->set_disk_id(primary_disk_id);
  arg->add_transformation_type_list(DataTransformation::kCompressionSnappy);
  if (!FLAGS_egroup_encryption_type.empty()) {
    arg->add_transformation_type_list(
      DataTransformation::kEncryptionAES256CTR);
  }
  arg->set_managed_by_aes(managed_by_aes);

  // Intent seqeunce should be the previous successful write intent sequence
  // number for this egroup.
  arg->set_min_expected_intent_sequence(egroup_id_intent_seq_map_[egroup_id]);
  if (managed_by_aes) {
    arg->set_slices_stored_by_id(true);
    arg->set_slice_group_size(32);
    arg->set_untransformed_slice_length(FLAGS_estore_regular_slice_size);

    ReadExtentGroupArg::Extent *ex = arg->add_extents();
    CHECK(ex->mutable_data_location());
    ex->mutable_extent_id()->set_vdisk_block(vblock_num);
    ex->mutable_extent_id()->set_owner_id(vdisk_id_);

    for (const RegionDescriptor& region : region_to_write_vec) {
      ex->add_region_offset(region.offset);
      ex->add_region_length(region.length);
    }
  } else {
    // Set the slice state to arg. This is populated with the necessary
    // information by the caller.
    for (uint ii = 0; ii < slice_state_vec.size(); ++ii) {
      CHECK(slice_state_vec[ii]->has_extent_group_offset());
      slice_state_vec[ii]->CopyTo(arg->add_slices());
    }

    // Set the extent information for each extent. This is provided by the
    // caller. For egroups not managed by AES, this will be provided in the
    // extent_state_vec.
    for (uint ii = 0; ii < extent_state_vec.size(); ++ii) {
      ReadExtentGroupArg::Extent *ex = arg->add_extents();
      for (const RegionDescriptor& region : region_vec) {
        ex->add_region_offset(region.offset);
        ex->add_region_length(region.length);
      }
      ex->mutable_data_location()->CopyFrom(
        extent_state_vec[ii]->diff_data_location());
      ex->mutable_extent_id()->CopyFrom(extent_state_vec[ii]->extent_id());
    }
  }


  Function<void(StargateError::Type, shared_ptr<string>,
                shared_ptr<ReadExtentGroupRet>, IOBuffer::Ptr&&)> done_cb =
    func_disabler_.Bind(&ReadMixedCompressionExtentGroupDone, _1, _2,
                        _3, _4, notification, data_written);
  notification->Reset();
  // Invoke the stargate read extent group request.
  iface->ReadExtentGroup(move(arg), move(done_cb));
  notification->Wait();
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::WriteParityExtentGroup(
  const int64 egroup_id,
  const int64 disk_id,
  const bool is_primary_parity,
  const vector<int64>& info_egroup_ids,
  const vector<int64>& parity_egroup_ids,
  const IOBuffer::Ptr& data_to_write,
  Notification *const notification,
  vector<SliceState::Ptr> *const slice_state_vec,
  const bool managed_by_aes,
  const unordered_map<int64, shared_ptr<ReplicaReadEgroupRet>>
    *info_egroup_map,
  const bool decode_request,
  const int32 inject_error_type) {

  // Perform the preliminary checks.
  CHECK(slice_state_vec);
  CHECK(notification);
  CHECK(data_to_write);

  slice_state_vec->clear();

  // Get stargate interface associated with the primary disk.
  const StargateInterface::Ptr& iface =
    cluster_mgr_->stargate_mgr()->GetStargateInterfaceForDisk(disk_id);
  CHECK(iface);

  // This should be initialized to zero by the caller. Also, we never expect
  // this method to be called after the first write.
  const int64 current_intent_sequence = egroup_id_intent_seq_map_[egroup_id];
  CHECK_EQ(current_intent_sequence, 0);

  // Add the necessary arguments to the write request.
  shared_ptr<WriteExtentGroupArg> arg = make_shared<WriteExtentGroupArg>();
  arg->set_extent_group_id(egroup_id);
  arg->set_qos_principal_name(string());
  arg->set_qos_priority(StargateQosPriority::kWrite);
  arg->set_owner_vdisk_id(vdisk_id_);
  arg->set_owner_container_id(container_id_);
  arg->set_managed_by_aes(managed_by_aes);
  if (managed_by_aes) {
    arg->set_writing_immutable_extents(true);
  }
  vector<DataTransformation::Type> tmp_transformation_type_vec;
  arg->set_owner_container_id(container_id_);
  arg->add_disk_ids(disk_id);

  arg->set_is_sequential(true);
  arg->set_egroup_type(WriteExtentGroupArg::kECParityEgroup);
  const int64 write_size = data_to_write->size();
  WriteExtentGroupArg::Primary *primary_state = arg->mutable_primary();
  WriteExtentGroupArg::Primary::Extent *ex = primary_state->add_extents();
  ExtentIdProto *extent_id_proto = ex->mutable_extent_id();
  extent_id_proto->set_sha1_hash(string(""));
  extent_id_proto->set_extent_size(write_size);
  extent_id_proto->set_owner_id(container_id_);
  ex->add_region_offset(0);
  ex->add_region_length(write_size);

  if (!managed_by_aes) {
    arg->set_intent_sequence(current_intent_sequence);
  } else {
    arg->set_vdisk_incarnation_id(0);
    arg->set_intent_sequence(-1);
    arg->set_global_metadata_intent_sequence(0);
    arg->set_slice_group_size(32);
    arg->set_slices_stored_by_id(true);

    if (decode_request) {
      arg->set_is_decoded_parity_egroup(true);
    }

    CHECK(info_egroup_map);
    CHECK_EQ(info_egroup_map->size(), info_egroup_ids.size());

    // Fill the info member extent group metadata.
    WriteExtentGroupArg::ErasureCodingInfo *ec_info =
      arg->mutable_erasure_coding_info();

    for (uint ii = 0; ii < info_egroup_ids.size(); ++ii) {
      const auto iter = info_egroup_map->find(info_egroup_ids.at(ii));
      CHECK(iter != info_egroup_map->end());
      const shared_ptr<ReplicaReadEgroupRet>& info_ret = iter->second;
      CHECK(info_ret && info_ret.get()) << "idx " << ii
        << " info egroup " << info_egroup_ids.at(ii);
      CHECK(info_ret->has_extent_group_metadata()) << "idx " << ii
        << " info egroup " << info_egroup_ids.at(ii);

      WriteExtentGroupArg::ErasureCodingInfo::InfoExtentGroup *info_egroup =
        ec_info->add_info_egroups();
      info_egroup->set_extent_group_id(info_egroup_ids.at(ii));
      info_egroup->mutable_extent_group_metadata()->CopyFrom(
        info_ret->extent_group_metadata());
      info_egroup->set_vdisk_incarnation_id(0);
      info_egroup->set_highest_committed_intent_sequence(
        info_ret->highest_committed_intent_sequence());
      info_egroup->set_largest_seen_intent_sequence(
        info_ret->largest_seen_intent_sequence());
      info_egroup->set_checksum_type(info_ret->checksum_type());
    }
  }

  arg->set_untransformed_slice_length(FLAGS_estore_regular_slice_size);

  // Make dummy tentative update for first write, else extent store usage
  // op might delete the egroup state if it doesn't find any entry for this
  // egroup in map 3.
  MakeParityExtentGroupTentativeUpdate(
    egroup_id, disk_id, is_primary_parity, info_egroup_ids, parity_egroup_ids);

  map<StargateError::Type, int> expected_error_map;
  if (inject_error_type == 6 /* kNoSliceWritesAfterTentativeUpdates */ ||
      inject_error_type == 9 /* kNoFinalizeTentativeUpdates */) {
    // We anticipate a crash at the stargate servicing this write. Ensure
    // that the returned error type is a kTransportError.
    expected_error_map[StargateError::kTransportError] = 0;
  } else {
    CHECK_EQ(inject_error_type, -1);
  }

  Function<void(StargateError::Type, shared_ptr<string>,
                shared_ptr<WriteExtentGroupRet>)> done_cb =
    func_disabler_.Bind(&WriteExtentGroupDone,
                        _1, _2, _3, notification,
                        slice_state_vec, nullptr /* extent_state_vec */,
                        !expected_error_map.empty() ?
                          &expected_error_map : nullptr,
                        data_to_write,
                        false /* managed_by_aes */,
                        true /* is_primary */);
  notification->Reset();
  iface->WriteExtentGroup(arg, data_to_write, move(done_cb),
                          FLAGS_egroup_test_write_eg_rpc_timeout * 1000);
  notification->Wait();

  if (!expected_error_map.empty()) {
    // Confirm no new error type was added, i.e. the write did fail with
    // the same error type as expected by the error injected.
    CHECK_EQ(expected_error_map.size(), 1);
    CHECK_EQ(expected_error_map.begin()->second, 1);
  }
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::WriteInfoExtentGroup(
  const int64 info_egroup_id,
  const int64 info_disk_id,
  const bool managed_by_aes,
  const bool decode_request,
  const IOBuffer::Ptr& data_to_write,
  const shared_ptr<interface::ReplicaReadEgroupRet>& remote_ret,
  const int32 inject_error_type) {

  unique_ptr<MedusaExtentGroupIdMapEntry> aes_egid_entry =
    make_unique<MedusaExtentGroupIdMapEntry>(
      &remote_ret->extent_group_metadata());
  const MedusaExtentGroupIdMapEntry *physical_egroup_metadata =
    aes_egid_entry.get();
  const auto& control_block = physical_egroup_metadata->control_block();
  shared_ptr<WriteExtentGroupArg> arg = make_shared<WriteExtentGroupArg>();
  arg->set_extent_group_id(info_egroup_id);

  arg->set_qos_principal_name(string());
  arg->set_qos_priority(StargateQosPriority::kDefault);
  arg->set_owner_vdisk_id(control_block->owner_vdisk_id());
  arg->set_owner_container_id(control_block->owner_container_id());
  arg->add_disk_ids(info_disk_id);
  arg->set_managed_by_aes(true);
  if (decode_request) {
    arg->set_writing_immutable_extents(true);
  }

  // Set the RPC arg parameters for letting the replica know that the write
  // RPC is creating AES-EC egroups and that the egroup write op must place
  // the EC markers for these egroups.
  arg->set_is_decoded_info_egroup(decode_request);

  arg->set_vdisk_incarnation_id(remote_ret->last_mutator_incarnation_id());
  arg->set_slice_group_size(control_block->slice_group_size());
  arg->set_slices_stored_by_id(
    control_block->slices_stored_by_id());

  // For info egroups, the RPC is sent as a secondary write. Therefore,
  // set the intent sequence based on the largest seen intent sequence.
  arg->set_intent_sequence(remote_ret->largest_seen_intent_sequence() + 1);
  arg->set_checksum_type(remote_ret->checksum_type());
  DCHECK_GE(control_block->latest_intent_sequence(), 0)
    << info_egroup_id;
  arg->set_global_metadata_intent_sequence(
    control_block->latest_intent_sequence());
  arg->set_untransformed_slice_length(
    control_block->default_slice_size());

  // Set transformation type of the egroup.
  if (control_block->has_transformation_type()) {
    arg->set_transformation_type(control_block->transformation_type());
  }
  arg->mutable_transformation_type_list()->CopyFrom(
    control_block->transformation_type_list());

  const int32 write_size = data_to_write->size();
  int untransformed_slice_length = 0;
  WriteExtentGroupArg::Secondary *secondary = arg->mutable_secondary();
  ScopedArenaMark am;
  for (MedusaExtentGroupIdMapEntry::ExtentIterator eiter(
         physical_egroup_metadata, am.arena(), false /* last_update_only */);
       !eiter.End(); eiter.Next()) {
    pair<ExtentId::PtrConst, ExtentState::ArenaUPtrConst> extent_pair =
      eiter.Get();
    DCHECK(extent_pair.second);
    const ExtentState& extent_state = *extent_pair.second;
    const ExtentId& extent_id = *extent_pair.first;

    CHECK(
      !extent_id.extent_id_proto()->has_egroup_mapping_in_eid_map())
      << extent_id.ToString();

    WriteExtentGroupArg::Secondary::Extent *extent =
      secondary->add_extents();
    extent->mutable_extent_id()->CopyFrom(*extent_id.extent_id_proto());

    DCHECK(extent_state.slice_indices_size() == 0 ||
           extent_state.slice_indices_size() ==
             extent_state.slice_ids_size());
    interface::ExtentDataLocation *const data_location =
      extent->mutable_diff_data_location();

    const bool contains_indices = extent_state.slice_indices_size() > 0;
    for (int xx = 0; xx < extent_state.slice_ids_size(); ++xx) {
      data_location->add_slice_ids(extent_state.slice_ids(xx));
      data_location->add_slice_indices(contains_indices ?
        extent_state.slice_indices(xx) : xx);
    }

    if (extent_state.has_first_slice_offset()) {
      data_location->set_first_slice_offset(extent_state.first_slice_offset());
    }
  }

  for (MedusaExtentGroupIdMapEntry::SliceIterator siter(
         physical_egroup_metadata, am.arena());
       !siter.End();
       siter.Next()) {
    const SliceState::UPtrConst& slice_state = siter.Second();
    if (secondary) {
      slice_state->CopyTo(secondary->add_end_slices());
    }
    if (untransformed_slice_length == 0) {
      untransformed_slice_length = slice_state->untransformed_length();
      DCHECK_GT(untransformed_slice_length, 0);
    } else {
      DCHECK_EQ(untransformed_slice_length,
                slice_state->untransformed_length());
    }
  }
  CHECK_GT(untransformed_slice_length, 0);
  arg->set_untransformed_slice_length(untransformed_slice_length);

  secondary->add_egroup_region_offset(0);
  secondary->add_egroup_region_length(write_size);

  Configuration::PtrConst config = cluster_mgr_->Config();
  StargateUtil::FetchExtentGroupDiskUsage(
    secondary->mutable_disk_usage_diff(), info_egroup_id,
    physical_egroup_metadata, config);

  map<StargateError::Type, int> expected_error_map;

  if (inject_error_type > -1) {
    // Set the stargate gflag to inject corruption on this write.
    const int64 info_svm_id =
      cluster_mgr_->stargate_mgr()->GetSvmIdForStargate(
        cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(
          info_disk_id));
    cluster_mgr_->stargate_mgr()->SetGFlag(
      "estore_experimental_slice_write_inject_crash",
      to_string(inject_error_type), info_svm_id);
    LOG(INFO) << "Injected error " << inject_error_type
              << " on info disk id " << info_disk_id
              << " svm id " << info_svm_id;
    expected_error_map[StargateError::kTransportError] = 0;
  }

  const StargateInterface::Ptr& iface =
    cluster_mgr_->stargate_mgr()->GetStargateInterfaceForDisk(info_disk_id);
  CHECK(iface);
  Notification notification;
  Function<void(StargateError::Type, shared_ptr<string>,
                shared_ptr<WriteExtentGroupRet>)> done_cb =
    func_disabler_.Bind(&WriteExtentGroupDone,
                        _1, _2, _3, &notification,
                        nullptr /* slice_state_vec */,
                        nullptr /* extent_state_vec */,
                        !expected_error_map.empty() ?
                          &expected_error_map : nullptr,
                        data_to_write,
                        false /* managed_by_aes */,
                        false /* is_primary */);
  iface->WriteExtentGroup(arg, data_to_write, move(done_cb),
                          FLAGS_egroup_test_write_eg_rpc_timeout * 1000);
  notification.Wait();

  if (inject_error_type > -1) {
    sleep(20);

    RestartStargateAtIndex(
      cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(
        info_disk_id));
  }
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::WriteSingleExtentInExtentGroup(
  const int64 egroup_id,
  const int64 primary_disk_id,
  const int64 secondary_disk_id,
  const int64 vblock_num,
  const string& transformation_type,
  const vector<DataTransformation::Type>& transformation_type_vec,
  const vector<RegionDescriptor>& region_to_write_vec,
  Notification *const notification,
  vector<SliceState::Ptr> *const slice_state_vec,
  vector<shared_ptr<EgroupOpsTester::Extent>> *const extent_state_vec,
  const IOBuffer::Ptr& data_to_write,
  map<StargateError::Type, int> *const expected_error_map,
  const bool wait_for_write,
  const int64 latest_applied_intent_sequence,
  const Optional<bool> is_sequential,
  const bool managed_by_aes,
  const int64 global_metadata_intent_sequence,
  const bool slices_stored_by_id) {

  // Perform the preliminary checks.
  CHECK(slice_state_vec);
  CHECK(extent_state_vec);
  CHECK(notification);
  CHECK(data_to_write);

  slice_state_vec->clear();
  extent_state_vec->clear();
  data_to_write->Clear();

  // This should be initialized to zero by the caller, except for overwrites
  // (e.g., in TestExtentStoreOverwrites).
  const int64 current_intent_sequence = egroup_id_intent_seq_map_[egroup_id];

  // Add the necessary arguments to the write request.
  shared_ptr<WriteExtentGroupArg> arg = WriteEgArg();
  arg->set_extent_group_id(egroup_id);
  arg->set_managed_by_aes(managed_by_aes);
  arg->set_owner_vdisk_id(vdisk_id_);
  arg->set_owner_container_id(container_id_);
  if (managed_by_aes) {
    arg->set_vdisk_incarnation_id(0);
  }
  vector<DataTransformation::Type> tmp_transformation_type_vec;
  if (!transformation_type.empty()) {
    arg->set_transformation_type(transformation_type);
    CHECK(StargateUtil::StringToEnumTransformationTypes(
            transformation_type, &tmp_transformation_type_vec))
      << transformation_type;
  } else {
    for (const DataTransformation::Type transform_type :
           transformation_type_vec) {
      arg->add_transformation_type_list(transform_type);
    }
    tmp_transformation_type_vec = transformation_type_vec;
  }
  VLOG(1) << "container id = " << container_id_;
  VLOG(1) << "primary disk id = " << primary_disk_id;
  VLOG(1) << "secondary disk id = " << secondary_disk_id;
  arg->add_disk_ids(primary_disk_id);
  if (secondary_disk_id > 0) {
    arg->add_disk_ids(secondary_disk_id);
  }
  if (!managed_by_aes) {
    arg->set_intent_sequence(current_intent_sequence);
  } else {
    arg->set_intent_sequence(-1);

    // Set the global metadata intent sequence using the value provided.
    arg->set_global_metadata_intent_sequence(global_metadata_intent_sequence);

    arg->set_slice_group_size(32);
    arg->set_slices_stored_by_id(slices_stored_by_id);
  }
  if (current_intent_sequence > 0) {
    arg->set_expected_intent_sequence(current_intent_sequence - 1);
  }
  if (latest_applied_intent_sequence >= 0) {
    arg->set_latest_applied_intent_sequence(latest_applied_intent_sequence);
  }

  if (is_sequential) {
    arg->set_is_sequential(*is_sequential);
  }

  WriteExtentGroupArg::Primary *primary_state = arg->mutable_primary();
  WriteExtentGroupArg::Primary::Extent *ex = primary_state->add_extents();
  ExtentIdProto *extent_id_proto = ex->mutable_extent_id();
  extent_id_proto->set_vdisk_block(vblock_num);
  extent_id_proto->set_owner_id(vdisk_id_);

  const int32 untransformed_slice_length =
    StargateUtil::GetSliceSizeForTransformation(
      tmp_transformation_type_vec);
  arg->set_untransformed_slice_length(untransformed_slice_length);

  // Iterate through the regions and generate the data.
  for (const RegionDescriptor& region : region_to_write_vec) {
    IOBuffer::Ptr offset_buf;
    if (region.compress_percentage > 0) {
      const int32 chunk_size =
        untransformed_slice_length > region.length ?
        region.length : untransformed_slice_length;
      offset_buf = Random::CreateCompressibleDataIOBuffer(
        region.length, region.compress_percentage, chunk_size);
    } else {
      offset_buf = Random::CreateRandomDataIOBuffer(region.length);
    }

    // Append the iobuffer for the offset data to data_to_write.
    CHECK(offset_buf);
    CHECK_EQ(offset_buf->size(), region.length);
    data_to_write->AppendIOBuffer(offset_buf.get());

    // Include the region to write.
    ex->add_region_offset(region.offset);
    ex->add_region_length(region.length);
  }

  const Function<void(StargateError::Type,
                      shared_ptr<string>,
                      shared_ptr<WriteExtentGroupRet>)> done_cb =
    func_disabler_.Bind(&WriteExtentGroupDone,
                        _1, _2, _3, notification,
                        slice_state_vec, extent_state_vec,
                        expected_error_map /* expected_errors */,
                        data_to_write, managed_by_aes,
                        true /* is_primary */);
  notification->Reset();

  WriteExtentGroup(arg, data_to_write, done_cb);

  if (wait_for_write) {
    notification->Wait();
    FilterUnAllocatedSlices(slice_state_vec);
  }
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::ComputeOffsetCountForReadByteTest(
  vector<pair<int64, int64>> *offset_byte_vec,
  vector<int64> *byte_count_vec) {

  // Possible scenarios covered here:
  // 1. Aligned offset and size : No subregion read case.
  // 2. Aligned offset but misaligned size : One subregion read case.
  // 3. Misaligned offset but aligned size : One subregion read case.
  // 4. Misaligned offset and misaligned size : Two subregion read case.
  int64 offset = 0;
  int64 size = FLAGS_estore_checksum_subregion_size;
  int64 end_offset = 0;
  const int32 num_subregions = FLAGS_estore_regular_slice_size /
    FLAGS_estore_checksum_subregion_size;

  // Subregions should equally divide the slice.
  CHECK_EQ(FLAGS_estore_regular_slice_size %
             FLAGS_estore_checksum_subregion_size, 0);

  // Aligned offset and size.
  for (int ii = 0; ii < num_subregions; ++ii) {
    offset = ii * FLAGS_estore_checksum_subregion_size;
    offset_byte_vec->emplace_back(make_pair(offset, size));
    byte_count_vec->emplace_back(0);
  }

  // Aligned offset but misaligned size.
  // (offset, size) = (16384, 24576 + x)
  // where x is a random number between 1 to 8192.
  offset = 2 * FLAGS_estore_checksum_subregion_size;
  end_offset = offset + FLAGS_estore_checksum_subregion_size +
    Random::TL()->Uniform(1, FLAGS_estore_checksum_subregion_size - 1);
  size = end_offset - offset;

  offset_byte_vec->emplace_back(make_pair(offset, size));
  byte_count_vec->emplace_back(FLAGS_estore_checksum_subregion_size);

  // Misaligned offset and aligned size.
  // (offset, size) = (x, 16384) where x is a random number between 1 and
  // 8191.
  end_offset = 2 * FLAGS_estore_checksum_subregion_size;
  offset = Random::TL()->Uniform(1, FLAGS_estore_checksum_subregion_size - 1);
  size = end_offset - offset;

  offset_byte_vec->emplace_back(make_pair(offset, size));
  byte_count_vec->emplace_back(FLAGS_estore_checksum_subregion_size);

  // Misaligned offset and size.
  // (offset, size) = (x, y) where x is between 1 and 8191 and y is between
  // 8193 and 24576.
  offset = Random::TL()->Uniform(1, FLAGS_estore_checksum_subregion_size - 1);
  end_offset = Random::TL()->Uniform(FLAGS_estore_checksum_subregion_size + 1,
                                     2 * FLAGS_estore_checksum_subregion_size);
  size = end_offset - offset;

  offset_byte_vec->emplace_back(make_pair(offset, size));
  byte_count_vec->emplace_back(2 * FLAGS_estore_checksum_subregion_size);
}

//-----------------------------------------------------------------------------

void test::EgroupOpsTester::ComputeRegionOffsetAndSizeForSubregionTest(
  vector<pair<int64, int64>> *region_offset_length_vec) {
  // Possible combinations to test:
  // 1. Entire slice length.
  // 2. Slice subregion boundary aligned offset/length.
  // 3. Misaligned offset/length.
  // 4. Read/Write of entire slice.

  // Some random offset/length pairs to test read/write op.
  region_offset_length_vec->emplace_back(make_pair(10000, 500));
  region_offset_length_vec->emplace_back(make_pair(0, 25000));
  region_offset_length_vec->emplace_back(make_pair(12289, 4096));
  region_offset_length_vec->emplace_back(make_pair(12288, 4096));
  region_offset_length_vec->emplace_back(make_pair(12287, 4096));
  region_offset_length_vec->emplace_back(make_pair(8192, 8193));

  int64 offset = 0;
  int64 size = FLAGS_estore_checksum_subregion_size;
  const int32 num_subregions = FLAGS_estore_regular_slice_size /
    FLAGS_estore_checksum_subregion_size;

  // Individual slice subregions.
  for (int ii = 0; ii < num_subregions; ++ii) {
    offset = ii * FLAGS_estore_checksum_subregion_size;
    region_offset_length_vec->emplace_back(make_pair(offset, size));
  }

  // Misaligned offset. We use a random offset within the subregion for this
  // purpose.
  const int rand_offset = Random::TL()->Uniform(
    1, FLAGS_estore_regular_slice_size - 2);
  const int rand_end_offset = Random::TL()->Uniform(
    rand_offset + 1, FLAGS_estore_regular_slice_size - 1);
  size = rand_end_offset - rand_offset;
  region_offset_length_vec->emplace_back(make_pair(rand_offset, size));

  region_offset_length_vec->emplace_back(
    make_pair(0, FLAGS_estore_regular_slice_size));
}

//-----------------------------------------------------------------------------

static void TestReadCountForEgroupWriteOpDone(
  StargateError::Type error,
  const shared_ptr<string>& error_detail,
  const shared_ptr<TestActionRet>& ret,
  const int64 read_bytes,
  Notification *notify) {

  CHECK_EQ(error, StargateError::kNoError);
  const EgroupSliceSubregionReadCountTestRet test_ret =
    ret->egroup_slice_subregion_read_count_test_ret();

  // Verify if the read bytes are the same.
  CHECK_EQ(read_bytes, test_ret.read_data_bytes());

  // Proceed to the next test case if present.
  if (notify) {
    notify->Notify();
  }
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::TestReadCountForEgroupWriteOp(
  const int64 disk_id,
  Notification::Ptr *notification,
  pair<int64, int64> offset_byte,
  const int64 read_bytes) {

  // In order to test the read bytes, we directly invoke the necessary library
  // functions available in stargate_util. Since these methods are directly
  // used by egroup_write_op to compute the ranges required for write, testing
  // those would be sufficient.

  // Compute the offset and size of write op.
  const int64 offset = offset_byte.first;
  const int64 size = offset_byte.second;
  shared_ptr<TestActionArg> rpc = make_shared<TestActionArg>();
  EgroupSliceSubregionReadCountTestArg *arg =
    rpc->mutable_egroup_slice_subregion_read_count_test_args();
  CHECK(arg);

  // Assign the necessary test arguments.
  const int32 num_subregions = FLAGS_estore_regular_slice_size /
    FLAGS_estore_checksum_subregion_size;
  arg->set_num_subregions(num_subregions);
  arg->set_subregion_size(FLAGS_estore_checksum_subregion_size);
  arg->set_offset(offset);
  arg->set_size(size);

  // Create the interface for test rpc.
  StargateInterface::Ptr iface =
    cluster_mgr_->stargate_mgr()->GetStargateInterfaceForDisk(disk_id);

  CHECK(iface);

  const Function<void(StargateError::Type,
                      shared_ptr<string>,
                      shared_ptr<TestActionRet>)> done_cb =
    func_disabler_.Bind(
      &TestReadCountForEgroupWriteOpDone,
      _1, _2, _3, read_bytes, notification->get());

  (*notification)->Reset();

  // Perform the op.
  iface->TestAction(rpc, done_cb);
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::TestReadBytes(const int64 disk_id) {
  vector<int64> read_bytes_vec;
  vector<pair<int64, int64>> offset_byte_vec;
  // Compute the various combinations of writes for the test.
  ComputeOffsetCountForReadByteTest(&offset_byte_vec, &read_bytes_vec);
  Notification::Ptr notification = make_shared<Notification>();
  for (uint ii = 0; ii < read_bytes_vec.size(); ++ii) {
    TestReadCountForEgroupWriteOp(
      disk_id,
      &notification,
      offset_byte_vec[ii],
      read_bytes_vec[ii]);

    // Wait for the op to complete.
    notification->Wait();
  }
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::CompressedSliceTestHelper(
  const int64 egroup_id,
  const int64 primary_disk_id) {

  // Test Compressed slice write.
  CompressedSliceWriteTest(egroup_id, primary_disk_id);

  // Test Compressed slice read.
  CompressedSliceReadTest(egroup_id, primary_disk_id);
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::CompressedSliceCorruptTestHelper(
  const int64 egroup_id,
  const int64 primary_disk_id,
  const set<StargateError::Type>& expected_errors) {

  // Test the case where we fail to uncompress a slice.
  cluster_mgr_->stargate_mgr()->SetGFlag(
    "estore_experimental_slice_write_inject_crash",
    "11" /* kCorruptCompressedSliceBeforeChecksum */);
  CompressedSliceReadTest(egroup_id, primary_disk_id, expected_errors);
  cluster_mgr_->stargate_mgr()->SetGFlag(
    "estore_experimental_slice_write_inject_crash", "-1");
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::CompressedSliceWriteTest(
  const int64 egroup_id,
  const int64 primary_disk_id) {

  vector<SliceState::Ptr> slice_state_vec;
  vector<shared_ptr<test::EgroupOpsTester::Extent>> extent_state_vec;

  // Perform the write with size slightly less than the untransformed slice
  // size. 3079 is a random number to substract from untransformed slice size.
  const int32 untransformed_slice_length =
    StargateUtil::GetSliceSizeForTransformation(
      vector<DataTransformation::Type>(DataTransformation::kCompressionLZ4));
  const int32 data_length = untransformed_slice_length - 3079;
  CHECK_GT(data_length, 0);
  vector<pair<int64, int64>> region_to_read;
  region_to_read.emplace_back(make_pair(0, data_length));

  vector<Notification::Ptr> notifications_write;
  const int num_compressed_writes = 2;
  for (int ii = 0; ii < num_compressed_writes; ++ii) {
    IOBuffer::Ptr data_to_write = make_shared<IOBuffer>();
    WriteExtentGroup(egroup_id,
                     primary_disk_id,
                     0 /* secondary_disk_id */,
                     1 /* num_vblocks_to_write */,
                     &notifications_write,
                     region_to_read,
                     &slice_state_vec,
                     &extent_state_vec,
                     data_to_write,
                     true /* compressible_data */);

    CHECK_GT(data_to_write->size(), 0);
    for (uint xx = 0; xx < notifications_write.size(); ++xx) {
      // Wait for partial writes from extent store.
      notifications_write[xx]->Wait();
    }

    // Verify if we have a single slice and one extent atleast.
    FilterUnAllocatedSlices(&slice_state_vec);
    CHECK_EQ(slice_state_vec.size(), 1);
    CHECK_GT(extent_state_vec.size(), 0);

    // Update the intent sequence number.
    ++egroup_id_intent_seq_map_[egroup_id];

    // Reset notifications, slice state and extent state before the next write
    // op.
    notifications_write.clear();
    slice_state_vec.clear();
    extent_state_vec.clear();
  }
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::CompressedSliceReadTest(
  const int64 egroup_id,
  const int64 primary_disk_id,
  const set<StargateError::Type>& expected_errors) {

  const bool compressed_slice = true;
  vector<SliceState::Ptr> slice_state_vec;
  vector<shared_ptr<test::EgroupOpsTester::Extent>> extent_state_vec;

  // Read the data written in CompressedSliceWriteTest().
  const int32 untransformed_slice_length =
    StargateUtil::GetSliceSizeForTransformation(
      vector<DataTransformation::Type>(DataTransformation::kCompressionLZ4));
  const int32 data_length = untransformed_slice_length - 3079;
  CHECK_GT(data_length, 0);
  vector<pair<int64, int64>> region_to_read;
  region_to_read.emplace_back(make_pair(0, data_length));

  vector<Notification::Ptr> notifications_write;
    IOBuffer::Ptr data_to_write = make_shared<IOBuffer>();
    WriteExtentGroup(egroup_id,
                     primary_disk_id,
                     0 /* secondary_disk_id */,
                     1 /* num_vblocks_to_write */,
                     &notifications_write,
                     region_to_read,
                     &slice_state_vec,
                     &extent_state_vec,
                     data_to_write,
                     compressed_slice);

    CHECK_GT(data_to_write->size(), 0);
    for (uint xx = 0; xx < notifications_write.size(); ++xx) {
      // Wait for partial writes from extent store.
      notifications_write[xx]->Wait();
    }

    // Verify if we have a single slice and one extent atleast.
    FilterUnAllocatedSlices(&slice_state_vec);
    CHECK_EQ(slice_state_vec.size(), 1);
    CHECK_GT(extent_state_vec.size(), 0);
    vector<Notification::Ptr> notifications_read;

    // Read the slice.
    ReadExtentGroup(egroup_id,
                    primary_disk_id,
                    notifications_read,
                    region_to_read,
                    slice_state_vec,
                    extent_state_vec,
                    data_to_write,
                    get<0>(region_to_read[0]),
                    compressed_slice,
                    true /* wait_for_read */,
                    expected_errors);
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::MixedCompressionEgroupVerifiedWrite(
  const int64 egroup_id,
  const int64 primary_disk_id,
  const int64 secondary_disk_id,
  const int64 vblock_num,
  const vector<RegionDescriptor>& region_vec,
  Notification *const notification,
  vector<SliceState::Ptr> *const slice_state_vec,
  vector<shared_ptr<EgroupOpsTester::Extent>> *const extent_state_vec,
  const IOBuffer::Ptr& data_to_write,
  map<StargateError::Type, int> *expected_error_map,
  const int64 global_metadata_intent_sequence) {

  vector<DataTransformation::Type> transformation_type_vec(
    1, DataTransformation::kCompressionSnappy);
  if (!FLAGS_egroup_encryption_type.empty()) {
    transformation_type_vec.push_back(
      DataTransformation::kEncryptionAES256CTR);
  }
  const bool managed_by_aes = FLAGS_egroup_test_estore_aes_enabled;
  WriteSingleExtentInExtentGroup(
    egroup_id,
    primary_disk_id,
    secondary_disk_id,
    vblock_num,
    "" /* transformation_type */,
    transformation_type_vec,
    region_vec,
    notification,
    slice_state_vec,
    extent_state_vec,
    data_to_write,
    expected_error_map,
    true /* wait_for_write */,
    -1 /* latest_applied_intent_sequence */,
    Optional<bool>() /* is_sequential */,
    managed_by_aes,
    global_metadata_intent_sequence);

  FilterUnAllocatedSlices(slice_state_vec);

  // If we are expecting errors from the write, let's skip the read.
  const bool perform_read = !expected_error_map;
  if (perform_read) {
    // Perform read to make sure the data matches.
    LOG(INFO) << "Perform read to verify the data written";
    ReadMixedCompressionExtentGroup(
      egroup_id,
      primary_disk_id,
      notification,
      region_vec,
      *slice_state_vec,
      *extent_state_vec,
      data_to_write,
      region_vec,
      vblock_num,
      managed_by_aes);
  }
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::MixedCompressionEgroupSanityTest(
  const int64 egroup_id,
  const int64 primary_disk_id,
  const int64 secondary_disk_id,
  const bool full_slice) {

  const string msg = full_slice ? "full slice" : "partial slice";
  LOG(INFO) << "Writing " << msg << " to egroup " << egroup_id
            << " on disk " << primary_disk_id;

  const int64 vblock_num = 0;
  Notification notification;
  vector<SliceState::Ptr> slice_state_vec;
  vector<shared_ptr<EgroupOpsTester::Extent>> extent_state_vec;
  IOBuffer::Ptr data_to_write = make_shared<IOBuffer>();

  // Let's lower the threshold for setting the
  // bytes_to_write_before_retrying_compression field in order to verify the
  // field after the write op completes. Since the sanity test updates just 2
  // slices (one of which is uncompressed), the field will not be updated.
  cluster_mgr_->stargate_mgr()->SetGFlag(
    "stargate_num_uncompressed_slices_for_retrying_compression", "2");
  int32 expected_bytes_to_write_before_retrying_compression = -1;

  // Write 2 slices { not compressible slice, highly compressible slice }.
  LOG(INFO) << "Write a non-compressed slice and a compressed slice";
  vector<int32> slice_compress_percentage_vec = { 0, 80 };
  vector<RegionDescriptor> region_vec;
  SetupRegions(slice_compress_percentage_vec, &region_vec, full_slice);
  MixedCompressionEgroupVerifiedWrite(egroup_id,
                                      primary_disk_id,
                                      secondary_disk_id,
                                      vblock_num,
                                      region_vec,
                                      &notification,
                                      &slice_state_vec,
                                      &extent_state_vec,
                                      data_to_write);

  // Slice state and extent state are not updated in the return values if the
  // egroup is managed by AES. We need to fetch the state and verify if the
  // states match with the expected value.
  const bool managed_by_aes = FLAGS_egroup_test_estore_aes_enabled;
  if (managed_by_aes) {
    CHECK(slice_state_vec.empty());
    GetEgroupStateRet egroup_state_secondary;
    GetEgroupState(egroup_id, secondary_disk_id,
                   0 /* latest_intent_sequence */,
                   &egroup_state_secondary, true /* managed_by_aes */,
                   false /* extent_based_format */, nullptr /* error_ret */,
                   false /* set_latest_applied_intent_sequence */,
                   true /* fetch_all_metadata */);

    GetEgroupStateRet egroup_state_primary;
    GetEgroupState(egroup_id, primary_disk_id, 0 /* latest_intent_sequence */,
                   &egroup_state_primary, true /* managed_by_aes */,
                   false /* extent_based_format */, nullptr /* error_ret */,
                   false /* set_latest_applied_intent_sequence */,
                   true /* fetch_all_metadata */,
                   20000 /* rpc_timeout_msecs */);
    MedusaExtentGroupIdMapEntry::Ptr physical_state_entry_primary =
      make_shared<MedusaExtentGroupIdMapEntry>(
        &egroup_state_primary.extent_group_metadata());
    MedusaExtentGroupIdMapEntry::Ptr physical_state_entry_secondary =
      make_shared<MedusaExtentGroupIdMapEntry>(
        &egroup_state_secondary.extent_group_metadata());

    ScopedArenaMark am;
    int32 num_slices = 0;
    for (MedusaExtentGroupIdMapEntry::SliceIterator siter(
           physical_state_entry_secondary.get(), am.arena());
         !siter.End(); siter.Next()) {
      SliceState::UPtrConst slice_state = siter.Second();

      if (!slice_state->has_extent_group_offset()) {
        continue;
      }
      ++num_slices;

      // Since we're going to write just a small amount of data i.e., 2 slices
      // worth of data, check if those match with expected transformation.
      // The first slice will not be compressed while the second slice is
      // compressed.
      CHECK_LE(num_slices, 2);
      if (num_slices == 1) {
        CHECK_EQ(slice_state->checksum_size(), 4)
          << slice_state->ShortDebugString();
        CHECK(IsUntransformedSlice(*slice_state))
          << slice_state->ShortDebugString();
      } else {
        CHECK_EQ(slice_state->checksum_size(), 1)
          << slice_state->ShortDebugString();
        CHECK(IsTransformedSlice(*slice_state))
          << slice_state->ShortDebugString();
      }
    }

    LOG(INFO) << "Testing the field bytes_to_write_before_retrying_compression"
              << " expected value="
              << expected_bytes_to_write_before_retrying_compression
              << ", num_slices=" << num_slices;
    TestBytesToWriteBeforeRetryingCompressionUpdateHelper(
      egroup_id, managed_by_aes, move(physical_state_entry_secondary),
      move(physical_state_entry_primary),
      expected_bytes_to_write_before_retrying_compression);
  } else {
    CHECK_EQ(extent_state_vec.size(), 1);
    CHECK_EQ(slice_state_vec.size(), 2);
    // Assert that the first slice is not compressed.
    CHECK(IsUntransformedSlice(*slice_state_vec[0]));
    CHECK_EQ(slice_state_vec[0]->checksum_size(), 4);
    // Assert that the second slice is compressed.
    CHECK(IsTransformedSlice(*slice_state_vec[1]));
    CHECK_EQ(slice_state_vec[1]->checksum_size(), 1);
  }

  // Perform subregion read on the second subregion of slice #0
  // (untransformed slice).
  LOG(INFO) << "Perform subregion read on untransformed slice";
  region_vec.clear();
  region_vec.emplace_back(FLAGS_estore_checksum_subregion_size /* offset */,
                          FLAGS_estore_checksum_subregion_size /* length */,
                          -1 /* compress_percentage */);
  IOBuffer::Ptr partial_data =
    data_to_write->Clone(FLAGS_estore_checksum_subregion_size /* offset */,
                         FLAGS_estore_checksum_subregion_size /* length */);
  ReadMixedCompressionExtentGroup(
    egroup_id,
    primary_disk_id,
    &notification,
    region_vec,
    slice_state_vec,
    extent_state_vec,
    partial_data,
    region_vec,
    vblock_num,
    managed_by_aes);

  // Perform partial read on slice #1 (transformed slice).
  LOG(INFO) << "Perform partial read on transformed slice";
  region_vec.clear();
  const int32 untransformed_compressed_slice_length =
    StargateUtil::GetSliceSizeForTransformation(
      vector<DataTransformation::Type>(DataTransformation::kCompressionLZ4));
  region_vec.emplace_back(untransformed_compressed_slice_length /* offset */,
                          FLAGS_estore_checksum_subregion_size /* length */,
                          -1 /* compress_percentage */);
  const int32 partial_data_offset = full_slice ?
    untransformed_compressed_slice_length :
    untransformed_compressed_slice_length / 2;
  partial_data =
    data_to_write->Clone(partial_data_offset,
                         FLAGS_estore_checksum_subregion_size /* length */);
  ReadMixedCompressionExtentGroup(
    egroup_id,
    primary_disk_id,
    &notification,
    region_vec,
    slice_state_vec,
    extent_state_vec,
    partial_data,
    region_vec,
    vblock_num,
    managed_by_aes);

  // Write the data in opposite compressibility. First slice will still be
  // non-compressed since it is created as non-compressed. The new data will
  // not fit into the second slice. Therefore, a new untransformed slice will
  // be created to replace the second slice.
  ++egroup_id_intent_seq_map_[egroup_id];
  LOG(INFO) << "Rewrite the slices in opposite compressibility";
  slice_compress_percentage_vec = { 70, 0 };
  SetupRegions(slice_compress_percentage_vec, &region_vec, full_slice);
  MixedCompressionEgroupVerifiedWrite(egroup_id,
                                      primary_disk_id,
                                      secondary_disk_id,
                                      vblock_num,
                                      region_vec,
                                      &notification,
                                      &slice_state_vec,
                                      &extent_state_vec,
                                      data_to_write);

  if (managed_by_aes) {
    CHECK(slice_state_vec.empty());
    GetEgroupStateRet egroup_state_secondary;
    GetEgroupState(egroup_id, secondary_disk_id,
                   0, &egroup_state_secondary, true /* managed_by_aes */,
                   false /* extent_based_format */, nullptr /* error_ret */,
                   false /* set_latest_applied_intent_sequence */,
                   true /* fetch_all_metadata */);

    GetEgroupStateRet egroup_state_primary;
    GetEgroupState(egroup_id, primary_disk_id, 0 /* latest_intent_sequence */,
                   &egroup_state_primary, true /* managed_by_aes */,
                   false /* extent_based_format */, nullptr /* error_ret */,
                   false /* set_latest_applied_intent_sequence */,
                   true /* fetch_all_metadata */,
                   20000 /* rpc_timeout_msecs */);
    MedusaExtentGroupIdMapEntry::Ptr physical_state_entry_primary =
      make_shared<MedusaExtentGroupIdMapEntry>(
        &egroup_state_primary.extent_group_metadata());
    MedusaExtentGroupIdMapEntry::Ptr physical_state_entry_secondary =
      make_shared<MedusaExtentGroupIdMapEntry>(
        &egroup_state_secondary.extent_group_metadata());
    ScopedArenaMark am;
    int32 num_slices = 0;
    for (MedusaExtentGroupIdMapEntry::SliceIterator siter(
           physical_state_entry_secondary.get(), am.arena());
         !siter.End(); siter.Next()) {
      SliceState::UPtrConst slice_state = siter.Second();

      if (!slice_state->has_extent_group_offset()) {
        continue;
      }
      ++num_slices;

      CHECK_LE(num_slices, 3);
      // Validate the first slice and replacement slice.
      if (num_slices == 1 || num_slices == 3) {
        CHECK_EQ(slice_state->checksum_size(), 4)
          << slice_state->ShortDebugString();
        CHECK(IsUntransformedSlice(*slice_state))
          << slice_state->ShortDebugString();
      } else {
        CHECK_EQ(slice_state->checksum_size(), 1)
          << slice_state->ShortDebugString();
        CHECK(IsTransformedSlice(*slice_state))
          << slice_state->ShortDebugString();
      }
    }

    expected_bytes_to_write_before_retrying_compression =
      untransformed_compressed_slice_length +
      (full_slice ? 0 : untransformed_compressed_slice_length / 2);
    LOG(INFO) << "Testing the field bytes_to_write_before_retrying_compression"
              << " expected value="
              << expected_bytes_to_write_before_retrying_compression
              << ", num_slices=" << num_slices;
    TestBytesToWriteBeforeRetryingCompressionUpdateHelper(
      egroup_id, managed_by_aes, move(physical_state_entry_secondary),
      move(physical_state_entry_primary),
      expected_bytes_to_write_before_retrying_compression);
  } else {
    CHECK_EQ(extent_state_vec.size(), 1);
    // 2 live slices and 1 dead slice.
    CHECK_EQ(slice_state_vec.size(), 3);

    // Assert that the first slice is not compressed.
    CHECK(IsUntransformedSlice(*slice_state_vec[0]));
    CHECK_EQ(slice_state_vec[0]->checksum_size(), 4);

    // Dead slice.
    CHECK(IsTransformedSlice(*slice_state_vec[1]));
    CHECK_EQ(slice_state_vec[1]->checksum_size(), 1);
    // The replacement slice is always uncompressed.
    CHECK(IsUntransformedSlice(*slice_state_vec[2]));
    CHECK_EQ(slice_state_vec[2]->checksum_size(), 4);

    // For non-AES egroups, since the field
    // 'bytes_to_write_before_retrying_compression' is updated in
    // vdisk_controller, we might need to perform a vdisk write(end-to-end)
    // write for the field to take effect.
  }
  cluster_mgr_->stargate_mgr()->SetGFlag(
    "stargate_num_uncompressed_slices_for_retrying_compression", "-1");
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::MixedCompressionEgroupCompressibilityTest(
  const int64 egroup_id,
  const int64 primary_disk_id,
  const int64 secondary_disk_id,
  const int32 start_percentage,
  const int32 end_percentage,
  const int32 test_index) {

  LOG(INFO) << "Running " << __FUNCTION__;
  const int64 vblock_num = 0;
  Notification notification;
  vector<SliceState::Ptr> slice_state_vec;
  vector<shared_ptr<EgroupOpsTester::Extent>> extent_state_vec;
  IOBuffer::Ptr data_to_write = make_shared<IOBuffer>();

  // Stargate computes 'bytes_to_write_before_retrying_compression' based on
  // the egroup size or a gflag. For each test, we control this gflag and check
  // if the value which is updated matches with the expectation.
  int32 slice_retry_compression_threshold = test_index * 5;
  cluster_mgr_->stargate_mgr()->SetGFlag(
    "stargate_num_uncompressed_slices_for_retrying_compression",
    StringJoin(slice_retry_compression_threshold));

  int64 expected_bytes_to_write_before_retrying_compression = -1;
  bool check_primary_copy = false;

  LOG(INFO) << "Write slices with " << start_percentage
            << " to " << end_percentage << " compress precentage";
  vector<int32> slice_compress_percentage_vec;
  for (int ii = start_percentage; ii <= end_percentage; ++ii) {
    slice_compress_percentage_vec.emplace_back(ii);
  }
  vector<RegionDescriptor> region_vec;
  SetupRegions(slice_compress_percentage_vec, &region_vec);

  const bool managed_by_aes = FLAGS_egroup_test_estore_aes_enabled;
  const int64 secondary_disk = managed_by_aes ? secondary_disk_id : -1;
  MixedCompressionEgroupVerifiedWrite(egroup_id,
                                      primary_disk_id,
                                      secondary_disk,
                                      vblock_num,
                                      region_vec,
                                      &notification,
                                      &slice_state_vec,
                                      &extent_state_vec,
                                      data_to_write);

  const int32 num_slices_expected = slice_compress_percentage_vec.size();
  int num_transformed_slices = 0;
  int64 num_existing_untransformed_slices = 0;
  if (managed_by_aes) {
    CHECK(slice_state_vec.empty());
    GetEgroupStateRet egroup_state_secondary;
    GetEgroupState(egroup_id, secondary_disk,
                   0, &egroup_state_secondary, true /* managed_by_aes */,
                   false /* extent_based_format */, nullptr /* error_ret */,
                   false /* set_latest_applied_intent_sequence */,
                   true /* fetch_all_metadata */);

    GetEgroupStateRet egroup_state_primary;
    GetEgroupState(egroup_id, primary_disk_id, 0 /* latest_intent_sequence */,
                   &egroup_state_primary, true /* managed_by_aes */,
                   false /* extent_based_format */, nullptr /* error_ret */,
                   false /* set_latest_applied_intent_sequence */,
                   true /* fetch_all_metadata */,
                   20000 /* rpc_timeout_msecs */);
    MedusaExtentGroupIdMapEntry::Ptr physical_state_entry_primary =
      make_shared<MedusaExtentGroupIdMapEntry>(
        &egroup_state_primary.extent_group_metadata());
    MedusaExtentGroupIdMapEntry::Ptr physical_state_entry_secondary =
      make_shared<MedusaExtentGroupIdMapEntry>(
        &egroup_state_secondary.extent_group_metadata());
    ScopedArenaMark am;
    int32 num_slices = 0;
    for (MedusaExtentGroupIdMapEntry::SliceIterator siter(
           physical_state_entry_secondary.get(), am.arena());
         !siter.End(); siter.Next()) {
      SliceState::UPtrConst slice_state = siter.Second();

      if (!slice_state->has_extent_group_offset()) {
        continue;
      }
      ++num_slices;

      if (IsTransformedSlice(*slice_state)) {
        ++num_transformed_slices;
      }
    }
    // Since this is a fresh write to an extent group, the eg_state_ will be
    // null. The first write updates the field based on the total amount of
    // bytes that is written to each slice. We also need to look at the
    // transformation state since the bytes are updated based on the total
    // number of compressed/uncompressed slices. This is done once we fetch the
    // extent group state.
    num_existing_untransformed_slices = num_slices - num_transformed_slices;
    if (num_existing_untransformed_slices >=
        slice_retry_compression_threshold) {
      expected_bytes_to_write_before_retrying_compression =
        slice_retry_compression_threshold *
        StargateUtil::kDefaultCompressedSliceSize;
    }
    CHECK_EQ(num_slices, num_slices_expected);
    LOG(INFO) << "Testing the field bytes_to_write_before_retrying_compression"
              << " expected value="
              << expected_bytes_to_write_before_retrying_compression
              << ", num_slices=" << num_slices << ", num_transformed_slices="
              << num_transformed_slices << ", "
              << "slice_retry_compression_threshold="
              << slice_retry_compression_threshold;
    TestBytesToWriteBeforeRetryingCompressionUpdateHelper(
      egroup_id, managed_by_aes, move(physical_state_entry_secondary),
      move(physical_state_entry_primary),
      expected_bytes_to_write_before_retrying_compression,
      check_primary_copy);
  } else {
    CHECK_EQ(extent_state_vec.size(), 1);
    CHECK_EQ(slice_state_vec.size(), num_slices_expected);
    for (int ii = 0; ii < num_slices_expected; ++ii) {
      if (IsTransformedSlice(*slice_state_vec[ii])) {
        ++num_transformed_slices;
      }
    }
  }
  LOG(INFO) << "Number of transformed slice is " << num_transformed_slices;

  if (managed_by_aes) {
    // Let's abort the secondary write and verify if the field
    // 'bytes_to_write_before_retrying_compression' matches for both primary
    // and secondary replica. We also need to ensure that the following write
    // op performs the computation by lowering the threshold. For the first few
    // batches (with low compressibility percentage), these are bound to force
    // an update in the field.
    SetupRegions(slice_compress_percentage_vec, &region_vec);
    LOG(INFO) << "Rewriting slices by injecting an error";
    uint primary_idx =
      cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(primary_disk_id);
    cluster_mgr_->stargate_mgr()->SetGFlag(
      "estore_experimental_slice_write_inject_crash",
      "9" /* kNoFinalizeTentativeUpdates */,
      cluster_mgr_->stargate_mgr()->GetSvmIdForStargate(primary_idx));
    map<StargateError::Type, int> expected_error_map;
    expected_error_map[StargateError::kTransportError] = 0;
    MixedCompressionEgroupVerifiedWrite(egroup_id,
                                        primary_disk_id,
                                        secondary_disk,
                                        vblock_num,
                                        region_vec,
                                        &notification,
                                        &slice_state_vec,
                                        &extent_state_vec,
                                        data_to_write,
                                        &expected_error_map);

    LOG(INFO) << "Restarting stargate at index " << primary_idx;
    RestartStargateAtIndex(primary_idx);
    cluster_mgr_->stargate_mgr()->SetGFlag(
      "estore_experimental_slice_write_inject_crash", "-1");
    num_transformed_slices = 0;
    GetEgroupStateRet egroup_state_primary;
    CHECK(slice_state_vec.empty());
    GetEgroupStateRet egroup_state_secondary;
    GetEgroupState(egroup_id, secondary_disk,
                   0, &egroup_state_secondary, true /* managed_by_aes */,
                   false /* extent_based_format */, nullptr /* error_ret */,
                   false /* set_latest_applied_intent_sequence */,
                   true /* fetch_all_metadata */);

    GetEgroupState(egroup_id, primary_disk_id, 0 /* latest_intent_sequence */,
                   &egroup_state_primary, true /* managed_by_aes */,
                   false /* extent_based_format */, nullptr /* error_ret */,
                   false /* set_latest_applied_intent_sequence */,
                   true /* fetch_all_metadata */,
                   20000 /* rpc_timeout_msecs */);
    MedusaExtentGroupIdMapEntry::Ptr physical_state_entry_primary =
      make_shared<MedusaExtentGroupIdMapEntry>(
        &egroup_state_primary.extent_group_metadata());
    MedusaExtentGroupIdMapEntry::Ptr physical_state_entry_secondary =
      make_shared<MedusaExtentGroupIdMapEntry>(
        &egroup_state_secondary.extent_group_metadata());
    ScopedArenaMark am;
    int32 num_slices = 0;
    for (MedusaExtentGroupIdMapEntry::SliceIterator siter(
           physical_state_entry_secondary.get(), am.arena());
         !siter.End(); siter.Next()) {
      SliceState::UPtrConst slice_state = siter.Second();

      if (!slice_state->has_extent_group_offset()) {
        continue;
      }
      ++num_slices;

      if (IsTransformedSlice(*slice_state)) {
        ++num_transformed_slices;
      }
    }
    // If the field is already set to zero, then the new write will not update
    // the field again, Otherwise it further decrements the value.
    if (expected_bytes_to_write_before_retrying_compression > 0) {
      // Since this is the second request to the same slice, we will deduct the
      // amount of bytes written from the existing field. Since we are
      // overwriting to existing slices, the total number of existing
      // uncompressed slices is computed by
      // total_num_slices - total_transformed_slice_for_first_write.
      // 'bytes_to_write_before_retrying_compression' field.
      expected_bytes_to_write_before_retrying_compression -=
        num_existing_untransformed_slices * FLAGS_estore_regular_slice_size;
      if (expected_bytes_to_write_before_retrying_compression < 0) {
        expected_bytes_to_write_before_retrying_compression = 0;
      }
    }

    LOG(INFO) << "Testing the field bytes_to_write_before_retrying_compression"
              << " expected value="
              << expected_bytes_to_write_before_retrying_compression
              << ", num_slices=" << num_slices << ", num_transformed_slices="
              << num_transformed_slices << ", "
              << "num_existing_untransformed_slices="
              << num_existing_untransformed_slices;
    TestBytesToWriteBeforeRetryingCompressionUpdateHelper(
      egroup_id, managed_by_aes, move(physical_state_entry_secondary),
      move(physical_state_entry_primary),
      expected_bytes_to_write_before_retrying_compression,
      check_primary_copy);
    num_existing_untransformed_slices = num_slices - num_transformed_slices;
    LOG(INFO) << "Number of transformed slice is " << num_transformed_slices;
    cluster_mgr_->stargate_mgr()->SetGFlag(
      "stargate_num_uncompressed_slices_for_retrying_compression", "-1");

    // Let's reset the 'needs_tentative_resolution' since we're going to retry
    // another write on the extent group which just failed above.
    shared_ptr<SetEgroupStateArg> set_egroup_state_arg =
      make_shared<SetEgroupStateArg>();
    set_egroup_state_arg->set_extent_group_id(egroup_id);
    set_egroup_state_arg->set_disk_id(primary_disk_id);
    set_egroup_state_arg->set_intent_sequence(
      egroup_id_intent_seq_map_[egroup_id] + 1);
    set_egroup_state_arg->set_expected_applied_intent_sequence(
      egroup_state_primary.highest_committed_intent_sequence() + 1);
    set_egroup_state_arg->set_largest_seen_intent_sequence(
      egroup_state_primary.largest_seen_intent_sequence());
    set_egroup_state_arg->set_highest_committed_intent_sequence(
      egroup_state_primary.highest_committed_intent_sequence());
    set_egroup_state_arg->set_needs_tentative_resolution(false);
    SetExtentGroupState(primary_disk_id, set_egroup_state_arg);
  }

  // Rewrite all the regions with uncompressed data to make sure stargate does
  // not hit any internal asserts.
  LOG(INFO) << "Rewrite slices with uncompressible data";
  ++egroup_id_intent_seq_map_[egroup_id];
  vector<int32> slice_compress_zero_percentage_vec;
  slice_compress_zero_percentage_vec.reserve(
    slice_compress_percentage_vec.size());
  slice_compress_zero_percentage_vec.resize(num_slices_expected,
                                            0 /* compress_percentage */);
  SetupRegions(slice_compress_zero_percentage_vec, &region_vec);
  MixedCompressionEgroupVerifiedWrite(egroup_id,
                                      primary_disk_id,
                                      -1 /* secondary_disk_id */,
                                      vblock_num,
                                      region_vec,
                                      &notification,
                                      &slice_state_vec,
                                      &extent_state_vec,
                                      data_to_write,
                                      nullptr /* expected_error_map */,
                                      1 /* global_metadata_intent_sequence */);
  ++egroup_id_intent_seq_map_[egroup_id];

  // We should expect at least num_slices_expected since the overwrites can fit
  // into existing slices or replacement slices are needed.
  if (managed_by_aes) {
    CHECK(slice_state_vec.empty());
    GetEgroupStateRet egroup_state_secondary;
    GetEgroupState(egroup_id, secondary_disk,
                   0 /* latest_intent_sequence */,
                   &egroup_state_secondary, true /* managed_by_aes */,
                   false /* extent_based_format */, nullptr /* error_ret */,
                   false /* set_latest_applied_intent_sequence */,
                   true /* fetch_all_metadata */,
                   20000 /* rpc_timeout_msecs */);

    GetEgroupStateRet egroup_state_primary;
    GetEgroupState(egroup_id, primary_disk_id,
                   1 /* latest_intent_sequence */,
                   &egroup_state_primary, true /* managed_by_aes */,
                   false /* extent_based_format */, nullptr /* error_ret */,
                   false /* set_latest_applied_intent_sequence */,
                   true /* fetch_all_metadata */,
                   20000 /* rpc_timeout_msecs */);
    MedusaExtentGroupIdMapEntry::Ptr physical_state_entry_primary =
      make_shared<MedusaExtentGroupIdMapEntry>(
        &egroup_state_primary.extent_group_metadata());
    MedusaExtentGroupIdMapEntry::Ptr physical_state_entry_secondary =
      make_shared<MedusaExtentGroupIdMapEntry>(
        &egroup_state_secondary.extent_group_metadata());
    ScopedArenaMark am;
    int32 num_slices = 0;
    for (MedusaExtentGroupIdMapEntry::SliceIterator siter(
           physical_state_entry_secondary.get(), am.arena());
         !siter.End(); siter.Next()) {
      SliceState::UPtrConst slice_state = siter.Second();

      if (!slice_state->has_extent_group_offset()) {
        continue;
      }
      ++num_slices;

      if (IsTransformedSlice(*slice_state)) {
        ++num_transformed_slices;
      }
    }
    CHECK_GE(num_slices, num_slices_expected);

    // If the field is already set to zero, then the new write will not update
    // the field again, Otherwise it further decrements the value.
    if (expected_bytes_to_write_before_retrying_compression > 0) {
      const int64 bytes_written_to_existing_uncompressed_slices =
        expected_bytes_to_write_before_retrying_compression -
        num_existing_untransformed_slices * FLAGS_estore_regular_slice_size;
      expected_bytes_to_write_before_retrying_compression =
        bytes_written_to_existing_uncompressed_slices > 0 ?
        bytes_written_to_existing_uncompressed_slices : 0;
    }
    LOG(INFO) << "Testing the field bytes_to_write_before_retrying_compression"
              << " expected value="
              << expected_bytes_to_write_before_retrying_compression
              << ", num_slices=" << num_slices << ", num_transformed_slices="
              << num_transformed_slices;
    TestBytesToWriteBeforeRetryingCompressionUpdateHelper(
      egroup_id, managed_by_aes, move(physical_state_entry_secondary),
      move(physical_state_entry_primary),
      expected_bytes_to_write_before_retrying_compression,
      check_primary_copy);
  } else {
    CHECK_EQ(extent_state_vec.size(), 1);
    CHECK_GE(slice_state_vec.size(), num_slices_expected);
  }
  cluster_mgr_->stargate_mgr()->SetGFlag(
    "stargate_num_uncompressed_slices_for_retrying_compression", "-1");
}

void EgroupOpsTester::MixedCompressionEgroupWriteHelper(
  const int64 egroup_id,
  const int64 primary_disk_id,
  const int64 secondary_disk_id,
  const int64 num_vblocks,
  const bool full_slice,
  vector<SliceState::Ptr> *slice_state_vec) {

  CHECK(slice_state_vec);
  const string msg = full_slice ? "full slice" : "partial slice";
  LOG(INFO) << "Writing " << msg << " to egroup " << egroup_id
            << " on disk " << primary_disk_id
            << " for " << num_vblocks << " vblocks";

  const int64 base_vblock_num = 0;
  Notification notification;
  vector<shared_ptr<EgroupOpsTester::Extent>> extent_state_vec;
  IOBuffer::Ptr data_to_write = make_shared<IOBuffer>();

  // Write 2 slices { not compressible slice, highly compressible slice }.
  LOG(INFO) << "Write a non-compressed slice and a compressed slice";
  vector<int32> slice_compress_percentage_vec = {0, 80};
  vector<RegionDescriptor> region_vec;
  SetupRegions(slice_compress_percentage_vec, &region_vec, full_slice);
  vector<DataTransformation::Type> transformation_type_vec(
    1, DataTransformation::kCompressionSnappy);
  if (!FLAGS_egroup_encryption_type.empty()) {
    transformation_type_vec.push_back(
      DataTransformation::kEncryptionAES256CTR);
  }

  for (int ii = 0; ii < num_vblocks; ++ii) {
    int64 vblock_num = base_vblock_num + ii;
    WriteSingleExtentInExtentGroup(egroup_id,
                                   primary_disk_id,
                                   secondary_disk_id,
                                   vblock_num,
                                   "" /* transformation_type */,
                                   transformation_type_vec,
                                   region_vec,
                                   &notification,
                                   slice_state_vec,
                                   &extent_state_vec,
                                   data_to_write);
    ++egroup_id_intent_seq_map_[egroup_id];
  }
  FilterUnAllocatedSlices(slice_state_vec);
}

void EgroupOpsTester::SetupRegions(
  const vector<int32>& slice_compress_percentage_vec,
  vector<RegionDescriptor> *region_vec,
  const bool full_slice) {

  CHECK(region_vec);
  region_vec->clear();

  const int32 slice_length =
    StargateUtil::GetSliceSizeForTransformation(
      vector<DataTransformation::Type>(DataTransformation::kCompressionLZ4));
  for (uint ii = 0; ii < slice_compress_percentage_vec.size(); ++ii) {
    const int32 data_length =
      full_slice ? slice_length : slice_length / 2;
    region_vec->emplace_back(ii * slice_length /* offset */,
                             data_length /* length */,
                             slice_compress_percentage_vec[ii]);
  }
}

bool EgroupOpsTester::IsTransformedSlice(const SliceState& slice_state) {
  const int32 untransformed_compressed_slice_length =
    StargateUtil::GetSliceSizeForTransformation(
      vector<DataTransformation::Type>(DataTransformation::kCompressionLZ4));
  CHECK_EQ(slice_state.untransformed_length(),
           untransformed_compressed_slice_length);
  return slice_state.transformed_length() <
    slice_state.untransformed_length();
}

bool EgroupOpsTester::IsUntransformedSlice(const SliceState& slice_state) {
  const int32 untransformed_compressed_slice_length =
    StargateUtil::GetSliceSizeForTransformation(
      vector<DataTransformation::Type>(DataTransformation::kCompressionLZ4));
  CHECK_EQ(slice_state.untransformed_length(),
           untransformed_compressed_slice_length);
  return slice_state.transformed_length() ==
    slice_state.untransformed_length();
}

void EgroupOpsTester::SingleSliceTestHelper(
  const int64 egroup_id,
  const int64 primary_disk_id,
  const bool test_write) {

  if (test_write) {
    // This is a test for write op.
    TestSingleSliceWriteHelper(egroup_id, primary_disk_id);
  } else {
    // Test for read op.
    TestSingleSliceReadHelper(egroup_id, primary_disk_id);
  }
}

void EgroupOpsTester::TestSingleSliceWriteHelper(
  const int64 egroup_id,
  const int64 primary_disk_id) {

  vector<SliceState::Ptr> slice_state_vec;
  vector<shared_ptr<test::EgroupOpsTester::Extent>> extent_state_vec;
  vector<pair<int64, int64>> region_offset_length_read;
  ComputeRegionOffsetAndSizeForSubregionTest(&region_offset_length_read);

  // Perform the write for every region computed in the above method.
  for (uint ii = 0; ii < region_offset_length_read.size(); ++ii) {
    vector<pair<int64, int64>> region_to_read;
    region_to_read.emplace_back(
      make_pair(region_offset_length_read[ii].first,
                region_offset_length_read[ii].second));

    vector<Notification::Ptr> notifications_write;
    IOBuffer::Ptr data_to_write = make_shared<IOBuffer>();
    WriteExtentGroup(egroup_id,
                     primary_disk_id,
                     0 /* secondary_disk_id */,
                     1 /* num_vblocks_to_write */,
                     &notifications_write,
                     region_to_read,
                     &slice_state_vec,
                     &extent_state_vec,
                     data_to_write);

    CHECK_GT(data_to_write->size(), 0);
    for (uint xx = 0; xx < notifications_write.size(); ++xx) {
      // Wait for partial writes from extent store.
      notifications_write[xx]->Wait();
    }

    // Verify if we have a single slice and one extent atleast.
    FilterUnAllocatedSlices(&slice_state_vec);
    CHECK_EQ(slice_state_vec.size(), 1);
    CHECK_GT(extent_state_vec.size(), 0);
    vector<Notification::Ptr> notifications_read;

    // Read the slice to verify if the write op is successful.
    ReadExtentGroup(egroup_id,
                    primary_disk_id,
                    notifications_read,
                    region_to_read,
                    slice_state_vec,
                    extent_state_vec,
                    data_to_write,
                    get<0>(region_to_read[0]));


    // Update the intent sequence number.
    ++egroup_id_intent_seq_map_[egroup_id];
    slice_state_vec.clear();
    extent_state_vec.clear();
  }
}

void EgroupOpsTester::TestSingleSliceReadHelper(
  const int64 egroup_id,
  const int64 primary_disk_id) {

  vector<SliceState::Ptr> slice_state_vec;
  vector<shared_ptr<test::EgroupOpsTester::Extent>> extent_state_vec;
  vector<pair<int64, int64>> region_offset_length_read;

  vector<Notification::Ptr> notifications_write;
  region_offset_length_read.emplace_back(
    make_pair(0, FLAGS_estore_regular_slice_size));

  IOBuffer::Ptr data_to_write = make_shared<IOBuffer>();
  // Write an entire slice. This slice will be used for read purposes.
  WriteExtentGroup(egroup_id,
                   primary_disk_id,
                   0 /* secondary_disk_id */,
                   1 /* num_vblocks_to_write */,
                   &notifications_write,
                   region_offset_length_read,
                   &slice_state_vec,
                   &extent_state_vec,
                   data_to_write);

  CHECK_GT(data_to_write->size(), 0);
  for (uint xx = 0; xx < notifications_write.size(); ++xx) {
    // Wait for writes from extent store to complete.
    notifications_write[xx]->Wait();
  }

  FilterUnAllocatedSlices(&slice_state_vec);
  CHECK_EQ(slice_state_vec.size(), 1);
  CHECK_GT(extent_state_vec.size(), 0);

  vector<Notification::Ptr> notifications_read;

  // Compute the various combination of reads to test.
  ComputeRegionOffsetAndSizeForSubregionTest(&region_offset_length_read);
  ReadExtentGroup(egroup_id,
                  primary_disk_id,
                  notifications_read,
                  region_offset_length_read,
                  slice_state_vec,
                  extent_state_vec,
                  data_to_write,
                  get<0>(region_offset_length_read[0]));
}

void EgroupOpsTester::MultiSliceTestHelper(
  const int64 egroup_id,
  const int64 vblock_num,
  const int64 primary_disk_id,
  const int64 secondary_disk_id,
  const bool misalign_write,
  const bool partial_subregion_write,
  const bool compressible_data) {

  vector<shared_ptr<SliceState>> applied_slice_state_vec;
  vector<shared_ptr<SliceState>> slice_state_vec;
  vector<shared_ptr<EgroupOpsTester::Extent>> extent_state_vec;
  const bool managed_by_aes = FLAGS_egroup_test_estore_aes_enabled;

  // Test all possible subregion write combinations.
  vector<RegionDescriptor> region_to_write_vec;
  const int num_subregions =
    FLAGS_estore_regular_slice_size / FLAGS_estore_checksum_subregion_size;
  int region_write_size =
    partial_subregion_write ? FLAGS_estore_checksum_subregion_size / 2 :
                              FLAGS_estore_checksum_subregion_size;
  const int compress_percentage = compressible_data ? 50 : 0;
  for (uint8 ii = 1; ii <= 16; ++ii) {
    const int slice_offset = FLAGS_estore_regular_slice_size * ii;
    CHECK_LE(num_subregions, sizeof(ii) * 8);
    for (int kk = 0; kk < num_subregions; ++kk) {
      // Use ii as a bitmask that indicates which subregions to write.
      if (ii & (1 << kk)) {
        int write_offset = slice_offset + kk * region_write_size;
        if (misalign_write) {
          write_offset += FLAGS_estore_checksum_subregion_size / 2;
        }
        // If the write is misaligned, but write size is equal to
        // subregion_size, then it is possible that the write doesn't fit the
        // slice.
        if (write_offset + region_write_size <
            slice_offset + FLAGS_estore_regular_slice_size) {
          region_to_write_vec.emplace_back(
            write_offset, region_write_size, compress_percentage);
        }
      }
    }
  }

  IOBuffer::Ptr write_buf = make_shared<IOBuffer>();
  Notification notification;
  vector<DataTransformation::Type> transformation_type_vec;
  if (compressible_data) {
    transformation_type_vec.push_back(
      DataTransformation::kCompressionSnappy);
  }
  if (!FLAGS_egroup_encryption_type.empty()) {
    transformation_type_vec.push_back(
      DataTransformation::kEncryptionAES256CTR);
  }
  WriteSingleExtentInExtentGroup(egroup_id,
                                 primary_disk_id,
                                 secondary_disk_id,
                                 vblock_num,
                                 "",  // transformation_type
                                 transformation_type_vec,
                                 region_to_write_vec,
                                 &notification,
                                 &applied_slice_state_vec,
                                 &extent_state_vec,
                                 write_buf,
                                 nullptr /* expected_error_map */,
                                 true /* wait_for_write */,
                                 -1 /* latest_applied_intent_sequence */,
                                 Optional<bool>() /* is_sequential */,
                                 managed_by_aes);
  notification.Wait();

  // Read all the slices written and verify. Since partial slices may have been
  // written, fill the expected data with zeros for the ranges that have not
  // been written.
  vector<pair<int64, int64>> region_to_read_vec;
  IOBuffer::Ptr expected_data = make_shared<IOBuffer>();
  int prev_end_offset = 0, cur_offset = 0;
  for (const RegionDescriptor& region_written : region_to_write_vec) {
    if (region_written.offset > prev_end_offset) {
      expected_data->AppendIOBuffer(
        IOBufferUtil::CreateZerosIOBuffer(
          region_written.offset - prev_end_offset));
    }
    expected_data->AppendIOBuffer(
      write_buf->Clone(cur_offset, region_written.length));
    prev_end_offset = region_written.offset + region_written.length;
    cur_offset += region_written.length;
  }
  if (prev_end_offset < 16 * FLAGS_estore_regular_slice_size) {
    expected_data->AppendIOBuffer(
      IOBufferUtil::CreateZerosIOBuffer(
        16 * FLAGS_estore_regular_slice_size - prev_end_offset));
  }
  region_to_read_vec.emplace_back(
    make_pair(0, 16 * FLAGS_estore_regular_slice_size));

  ReadExtentGroupMultiRegion(
    egroup_id,
    primary_disk_id,
    region_to_read_vec,
    applied_slice_state_vec,
    extent_state_vec[0],
    expected_data,
    "" /* transformation_type */,
    transformation_type_vec,
    set<StargateError::Type>({ StargateError::kNoError })
      /* expected_errors */,
    managed_by_aes,
    32768 /* untransformed_slice_length */,
    true /* slices_stored_by_id */);
}


void EgroupOpsTester::CompressedSliceReplacementTestHelper(
  const int64 egroup_id,
  const int64 primary_disk_id) {

  // In this test the data is written in an egroup with different compression
  // levels to simulate different cases for the calculation of cushion size
  // when slice is being replaced.
  vector<pair<int64, int64>> offset_size_to_write;
  offset_size_to_write.emplace_back(make_pair(0, 1024 * 1024 /* 1 MB */));
  vector<SliceState::Ptr> slice_state_vec;
  vector<shared_ptr<EgroupOpsTester::Extent>> extent_state_vec;

  // Write 1 MB of 80% compressible data.
  WriteExtentGroupHelper(egroup_id,
                         primary_disk_id,
                         0 /* secondary_disk_id */,
                         1 /* num_vblocks_to_write */,
                         offset_size_to_write,
                         &slice_state_vec,
                         &extent_state_vec,
                         true /* notify  */,
                         true /* compressible_data */,
                         80 /* compress_percentage */);

  CHECK_EQ(extent_state_vec.size(), 1);
  vector<SliceState::Ptr> extent_slice_state_vec_1;
  FindSlicesForAnExtent(0 /* extent_index */,
                        slice_state_vec,
                        extent_state_vec,
                        &extent_slice_state_vec_1);
  CHECK_EQ(extent_slice_state_vec_1.size(), slice_state_vec.size());
  slice_state_vec.clear();
  extent_state_vec.clear();

  // Increment the intent sequence number.
  ++egroup_id_intent_seq_map_[egroup_id];

  // Write 1 MB of 40% compressible data.
  WriteExtentGroupHelper(egroup_id,
                         primary_disk_id,
                         0 /* secondary_disk_id */,
                         1 /* num_vblocks_to_write */,
                         offset_size_to_write,
                         &slice_state_vec,
                         &extent_state_vec,
                         true /* notify  */,
                         true /* compressible_data */,
                         40 /* compress_percentage */);

  CHECK_EQ(extent_state_vec.size(), 1);
  vector<SliceState::Ptr> extent_slice_state_vec_2;
  FindSlicesForAnExtent(0 /* extent_index */,
                        slice_state_vec,
                        extent_state_vec,
                        &extent_slice_state_vec_2);
  slice_state_vec.clear();
  extent_state_vec.clear();

  VerifyCushionSizeHelper(extent_slice_state_vec_1, extent_slice_state_vec_2);
  extent_slice_state_vec_1.clear();

  // Increment the intent sequence number.
  ++egroup_id_intent_seq_map_[egroup_id];

  // Write 1 MB of 25% compressible data.
  WriteExtentGroupHelper(egroup_id,
                         primary_disk_id,
                         0 /* secondary_disk_id */,
                         1 /* num_vblocks_to_write */,
                         offset_size_to_write,
                         &slice_state_vec,
                         &extent_state_vec,
                         true /* notify  */,
                         true /* compressible_data */,
                         25 /* compress_percentage */);

  CHECK_EQ(extent_state_vec.size(), 1);
  FindSlicesForAnExtent(0 /* extent_index */,
                        slice_state_vec,
                        extent_state_vec,
                        &extent_slice_state_vec_1);
  slice_state_vec.clear();
  extent_state_vec.clear();

  VerifyCushionSizeHelper(extent_slice_state_vec_2, extent_slice_state_vec_1);
  extent_slice_state_vec_2.clear();

  // Increment the intent sequence number.
  ++egroup_id_intent_seq_map_[egroup_id];

  // Write 1 MB of random data.
  WriteExtentGroupHelper(egroup_id,
                         primary_disk_id,
                         0 /* secondary_disk_id */,
                         1 /* num_vblocks_to_write */,
                         offset_size_to_write,
                         &slice_state_vec,
                         &extent_state_vec,
                         true /* notify  */,
                         true /* compressible_data */);

  CHECK_EQ(extent_state_vec.size(), 1);
  FindSlicesForAnExtent(0 /* extent_index */,
                        slice_state_vec,
                        extent_state_vec,
                        &extent_slice_state_vec_2);
  slice_state_vec.clear();
  extent_state_vec.clear();

  VerifyCushionSizeHelper(extent_slice_state_vec_1, extent_slice_state_vec_2);
  extent_slice_state_vec_1.clear();
}

void EgroupOpsTester::FindSlicesForAnExtent(
  const int32 extent_index,
  const vector<SliceState::Ptr>& slice_state_vec,
  const vector<shared_ptr<EgroupOpsTester::Extent>>& extent_state_vec,
  vector<SliceState::Ptr> *extent_slice_state_vec) {
  map<int32, SliceState::Ptr> slice_id_2_slice_state_map;

  // Update the slice id map.
  for (uint32 ii = 0; ii < slice_state_vec.size(); ii++) {
    slice_id_2_slice_state_map[slice_state_vec[ii]->slice_id()] =
      slice_state_vec[ii];
    VLOG(1) << "Slice Id: " << slice_state_vec[ii]->slice_id()
            << " Transformed length: "
            << slice_state_vec[ii]->transformed_length()
            << " Cushion: " << slice_state_vec[ii]->cushion();
  }

  // Find the corresponding slices based on slice ids in the extent.
  const google::protobuf::RepeatedField<int32> slice_ids =
    extent_state_vec[extent_index]->diff_data_location().slice_ids();
  for (int32 ii = 0; ii < slice_ids.size(); ii++) {
    map<int32, SliceState::Ptr>::iterator iter =
      slice_id_2_slice_state_map.find(slice_ids.Get(ii));
    CHECK(iter != slice_id_2_slice_state_map.end());
    extent_slice_state_vec->emplace_back(iter->second);
  }
}

void EgroupOpsTester::VerifyCushionSizeHelper(
  const vector<SliceState::Ptr>& old_slice_state_vec,
  const vector<SliceState::Ptr>& new_data_slice_state_vec) {

  const int32 untransformed_compressed_slice_length =
    StargateUtil::GetSliceSizeForTransformation(
      vector<DataTransformation::Type>(DataTransformation::kCompressionLZ4));

  CHECK_EQ(old_slice_state_vec.size(), new_data_slice_state_vec.size());
  for (uint32 ii = 0; ii < new_data_slice_state_vec.size(); ii++) {
    SliceState *old_slice_state = old_slice_state_vec[ii].get();
    SliceState *new_slice_state = new_data_slice_state_vec[ii].get();

    if (new_slice_state->transformed_length() >
        old_slice_state->transformed_length() + old_slice_state->cushion()) {
      // Slice replacement case.
      CHECK_NE(old_slice_state->slice_id(), new_slice_state->slice_id());

      // Calculate the expected cushion.
      const int32 rem = new_slice_state->transformed_length() %
        FLAGS_estore_slice_alignment_size;
      int32 expected_cushion =
        (rem ? (FLAGS_estore_slice_alignment_size - rem) : 0);

      expected_cushion = max(2 * old_slice_state->cushion(),
                             expected_cushion);
      expected_cushion = min(2 * FLAGS_estore_slice_alignment_size,
                             expected_cushion);

      const int32 total_slice_size = AlignUp(
        new_slice_state->transformed_length() + expected_cushion,
        FLAGS_estore_slice_alignment_size);
      if (untransformed_compressed_slice_length >
            new_slice_state->transformed_length() &&
          total_slice_size * 100 > untransformed_compressed_slice_length *
            FLAGS_stargate_migrate_extents_bad_compression_pct) {
        expected_cushion = untransformed_compressed_slice_length -
                           new_slice_state->transformed_length();
      } else {
        expected_cushion = total_slice_size -
                           new_slice_state->transformed_length();
      }
      CHECK_EQ(new_slice_state->cushion(), expected_cushion);
    } else {
      CHECK_EQ(old_slice_state->slice_id(), new_slice_state->slice_id());
      CHECK_GE(old_slice_state->cushion(), new_slice_state->cushion());
    }
  }
}

void EgroupOpsTester::WriteExtentGroupHelper(
  const int64 egroup_id,
  const int64 primary_disk_id,
  const int64 secondary_disk_id,
  const int num_vblocks_to_write,
  vector<SliceState::Ptr> *slice_state_vec,
  const bool managed_by_aes,
  bool notify,
  const bool wait_for_write) {

  vector<pair<int64, int64>> region_offset_length_read;
  vector<shared_ptr<EgroupOpsTester::Extent>> extent_state_vec;
  const int64 test_chunk_size = FLAGS_egroup_test_chunk_size;
  region_offset_length_read.emplace_back(make_pair(0, test_chunk_size));

  SetStargateGFlag(
    "estore_compute_secondary_disk_usage_from_primary", "false");
  vector<Notification::Ptr> notifications;
  IOBuffer::Ptr data_to_write = make_shared<IOBuffer>();
  WriteExtentGroup(egroup_id,
                   primary_disk_id,
                   secondary_disk_id,
                   num_vblocks_to_write,
                   &notifications,
                   region_offset_length_read,
                   slice_state_vec,
                   &extent_state_vec,
                   data_to_write,
                   false /* compressible_data */,
                   0 /* compress_percentage */,
                   managed_by_aes,
                   nullptr /* *expected_errors */,
                   wait_for_write);

  // Wait for write op to complete.
  if (notify) {
    CHECK_EQ(notifications.size(), num_vblocks_to_write);
    for (int xx = 0; xx < num_vblocks_to_write; ++xx) {
      // Wait for response from extent store.
      notifications[xx]->Wait();
    }
  }
  FilterUnAllocatedSlices(slice_state_vec);
}

void EgroupOpsTester::WriteExtentGroupHelper(
  const int64 egroup_id,
  const int64 primary_disk_id,
  const int64 secondary_disk_id,
  const int num_vblocks_to_write,
  const vector<pair<int64, int64>>& offset_size_to_write,
  vector<SliceState::Ptr> *slice_state_vec,
  vector<shared_ptr<EgroupOpsTester::Extent>> *extent_state_vec,
  bool notify,
  const bool compressible_data,
  int32 compress_percentage) {


  vector<Notification::Ptr> notifications;
  IOBuffer::Ptr data_to_write = make_shared<IOBuffer>();
  WriteExtentGroup(egroup_id,
                   primary_disk_id,
                   secondary_disk_id,
                   num_vblocks_to_write,
                   &notifications,
                   offset_size_to_write,
                   slice_state_vec,
                   extent_state_vec,
                   data_to_write,
                   compressible_data,
                   compress_percentage);

  // Wait for write op to complete.
  if (notify) {
    CHECK_EQ(notifications.size(), num_vblocks_to_write);
    for (int xx = 0; xx < num_vblocks_to_write; ++xx) {
      // Wait for response from extent store.
      notifications[xx]->Wait();
    }
  }
  FilterUnAllocatedSlices(slice_state_vec);
}

// Callback for ReplicaReadEgroup RPC.
static void ReplicaReadEgroupDone(
  StargateError::Type err,
  shared_ptr<string> err_detail,
  shared_ptr<ReplicaReadEgroupRet> remote_ret,
  IOBuffer::Ptr&& payload,
  Notification *notify,
  IOBuffer::Ptr *data,
  shared_ptr<ReplicaReadEgroupRet> *verify_ret,
  const StargateError::Type expected_err,
  const bool managed_by_aes,
  const shared_ptr<ReplicaReadEgroupArg>& remote_arg) {

  CHECK_EQ(err, expected_err);
  CHECK(notify);

  if (err == StargateError::kNoError) {
    CHECK(data);
    *data = move(payload);
    if (verify_ret) {
      DCHECK(managed_by_aes);
      CHECK(remote_ret->has_extent_group_metadata());
      *verify_ret = move(remote_ret);
    }
  } else {
    LOG(INFO) << "Received expected failure error " << err;
  }
  notify->Notify();
}

void EgroupOpsTester::ReplicaReadEgroup(
  const int64 egroup_id,
  const int64 disk_id,
  const vector<shared_ptr<EgroupOpsTester::SliceState>>& slice_state_vec,
  IOBuffer::Ptr *const data,
  const StargateError::Type expected_error,
  const bool managed_by_aes,
  Notification *const user_notification,
  shared_ptr<ReplicaReadEgroupRet> *const remote_ret,
  const int64 expected_intent_sequence,
  const bool extent_based_metadata_format,
  const bool ignore_corrupt_flag) {

  // Get stargate interface associated with the primary disk.
  const StargateInterface::Ptr& iface =
    cluster_mgr_->stargate_mgr()->GetStargateInterfaceForDisk(disk_id);
  CHECK(iface);

  map<int64, vector<uint>> slice_id_cksum_map;
  shared_ptr<ReplicaReadEgroupArg> remote_arg =
    make_shared<ReplicaReadEgroupArg>();
  remote_arg->set_extent_group_id(egroup_id);
  remote_arg->set_disk_id(disk_id);
  remote_arg->set_qos_principal_name(string());
  remote_arg->set_qos_priority(StargateQosPriority::kDefault);
  remote_arg->set_managed_by_aes(managed_by_aes);
  remote_arg->set_extent_based_metadata_format(extent_based_metadata_format);
  remote_arg->set_ignore_corrupt_flag(ignore_corrupt_flag);
  unordered_map<int64, int64>::iterator iter =
    egroup_id_intent_seq_map_.find(egroup_id);
  CHECK(iter != egroup_id_intent_seq_map_.end());
  remote_arg->set_expected_intent_sequence(
    managed_by_aes ?
      (expected_intent_sequence >= 0 ?
        expected_intent_sequence : -1) : iter->second);
  remote_arg->set_global_metadata_intent_sequence(iter->second);
  for (uint ii = 0; ii < slice_state_vec.size(); ++ii) {
    CHECK(slice_state_vec[ii]->has_extent_group_offset());
    slice_state_vec[ii]->CopyTo(remote_arg->add_slices());
  }
  Notification *notification = user_notification;
  Notification local_notification;
  if (user_notification == nullptr) {
    notification = &local_notification;
    notification->Reset();
  }
  Function<void(StargateError::Type,
                shared_ptr<string>,
                shared_ptr<ReplicaReadEgroupRet>,
                IOBuffer::Ptr&&)> done_cb =
    func_disabler_.Bind(&ReplicaReadEgroupDone,
                        _1, _2, _3, _4, notification, data,
                        remote_ret, expected_error, managed_by_aes,
                        remote_arg);
  iface->ReplicaReadEgroup(move(remote_arg), move(done_cb),
    FLAGS_egroup_test_replica_read_eg_rpc_timeout * 1000);
  if (user_notification == nullptr) {
    notification->Wait();
  }
}

// Callback for ReplicaReadEgroup RPC.
static void ReplicaReadEgroupVerifyDone(
  StargateError::Type err,
  shared_ptr<string> err_detail,
  shared_ptr<ReplicaReadEgroupRet> remote_ret,
  IOBuffer::Ptr&& payload,
  Notification *notify,
  map<int64, vector<uint>> *slice_id_cksum_map,
  vector<shared_ptr<EgroupOpsTester::SliceState>> slice_state_vec,
  const StargateError::Type expected_error,
  const bool is_ec_egroup,
  vector<int> *unreadable_slices,
  IOBuffer::Ptr *payload_rec) {


  

  CHECK_EQ(err, expected_error);
  if (err != StargateError::kNoError) {
    if (notify) {
      notify->Notify();
    }
    return;
  }

  DCHECK_EQ(is_ec_egroup, remote_ret->is_erasure_coded());
  const int slice_size = 32 * 1024;

  //checking the return args for corrupt slices
if(FLAGS_make_the_slices_corrupt_){
  LOG(INFO)<<"ReplicaReadEgroupRet came , number of unreadable slices :"
           << remote_ret->corrupt_slices_size();
  if (remote_ret->corrupt_slices_size()){
    LOG(INFO)<<"Corrupt slices : ";
    for (int ii = 0; ii < remote_ret->corrupt_slices_size(); ++ii) {
        LOG(INFO)<<remote_ret->corrupt_slices().Get(ii)<<" ";
        unreadable_slices->push_back(remote_ret->corrupt_slices().Get(ii));
    }

  }
  
  LOG(INFO)<<"recieved payload size : "<<payload->size();
  LOG(INFO)<<"num slices in recieved payload : "<<payload->size()/slice_size;

}
  
  // Ensure that the size of the payload is a multiple of
  // slice size.
  CHECK_GT(payload->size(), 0);
  CHECK_EQ(payload->size() % slice_size, 0) << payload->size();
  const int num_slices = payload->size() / slice_size;
  if(!FLAGS_make_the_slices_corrupt_){
    
    CHECK_EQ(slice_state_vec.size(), num_slices);

  }

int payload_ind=0;
  bool is_unreadable;
if(FLAGS_make_the_slices_corrupt_){
  LOG(INFO)<<"In ReplicaReadEgroupVerifyDone";
  
  LOG(INFO)<<"unreadable_size:"<<unreadable_slices->size();}

  // Checksum computation for every slice.
  for (size_t ii = 0; ii < slice_state_vec.size(); ++ii) {
      if (FLAGS_make_the_slices_corrupt_) {
            is_unreadable=(find(unreadable_slices->begin(), unreadable_slices->end(), slice_state_vec[ii]->slice_id()) 
                                                                                            != unreadable_slices->end());
            LOG(INFO)<<"ii:"<<ii<<"  is_unreadable:"<<is_unreadable;
            if(is_unreadable) 
                continue;

          }


    IOBuffer::Ptr iobuf = payload->Clone(payload_ind * slice_size, slice_size);
    payload_ind+=1;

    // Compute the checksum for every slice subregion.
    const int subregion_size = FLAGS_estore_regular_slice_size /
      slice_state_vec[ii]->checksum_size();
    for (int jj = 0; jj < slice_state_vec[ii]->checksum_size(); ++jj) {
      // Compute the checksum on the subregion.
      (*slice_id_cksum_map)[slice_state_vec[ii]->slice_id()].emplace_back(
        (remote_ret->checksum_type() ==
           static_cast<int>(ChecksumType::kCrc32c)) ?
          IOBufferUtil::Crc32(
            iobuf.get(), jj * subregion_size, subregion_size) :
          IOBufferUtil::Adler32(
            iobuf.get(), jj * subregion_size, subregion_size));
    }
  }
  *payload_rec=move(payload);
  if (notify) {
    notify->Notify();
  }
}

void EgroupOpsTester::ReplicaReadEgroupAndVerify(
  int64 egroup_id,
  int64 disk_id,
  vector<shared_ptr<EgroupOpsTester::SliceState>> *const slice_state_vec,
  const bool managed_by_aes,
  const int64 expected_applied_intent_sequence,
  const StargateError::Type expected_error,
  const bool is_ec_egroup,
  vector<int> *unreadable_slices,
  IOBuffer::Ptr *payload_rec
  ) {



  // Sort slice state vector in increasing order of extent group offset.
  sort(slice_state_vec->begin(), slice_state_vec->end(),
       SliceState::SliceStatePtrOrderIncreasingFileOffset);

  // Get stargate interface associated with the primary disk.
  const StargateInterface::Ptr& iface =
    cluster_mgr_->stargate_mgr()->GetStargateInterfaceForDisk(disk_id);
  CHECK(iface);

  map<int64, vector<uint>> slice_id_cksum_map;
  shared_ptr<ReplicaReadEgroupArg> remote_arg =
    make_shared<ReplicaReadEgroupArg>();
  remote_arg->set_extent_group_id(egroup_id);
  remote_arg->set_disk_id(disk_id);
  remote_arg->set_qos_principal_name(string());
  remote_arg->set_qos_priority(StargateQosPriority::kDefault);
  remote_arg->set_managed_by_aes(managed_by_aes);
  unordered_map<int64, int64>::iterator iter =
    egroup_id_intent_seq_map_.find(egroup_id);
  CHECK(iter != egroup_id_intent_seq_map_.end());
  remote_arg->set_expected_intent_sequence(
    managed_by_aes ?
      (expected_applied_intent_sequence >= 0 ?
        expected_applied_intent_sequence : -1) : iter->second);
  remote_arg->set_global_metadata_intent_sequence(iter->second);
  for (uint ii = 0; ii < slice_state_vec->size(); ++ii) {
    (*slice_state_vec)[ii]->CopyTo(remote_arg->add_slices());
  }
  Notification notification;
  Function<void(StargateError::Type,
                shared_ptr<string>,
                shared_ptr<ReplicaReadEgroupRet>,
                IOBuffer::Ptr&&)> done_cb =
    func_disabler_.Bind(&ReplicaReadEgroupVerifyDone,
                        _1, _2, _3, _4, &notification, &slice_id_cksum_map,
                        *slice_state_vec, expected_error,
                        is_ec_egroup, unreadable_slices,
                        payload_rec
                        );
  notification.Reset();
  iface->ReplicaReadEgroup(move(remote_arg), move(done_cb),
    FLAGS_egroup_test_replica_read_eg_rpc_timeout * 1000);
  notification.Wait();

  if (expected_error != StargateError::kNoError) {
    return;
  }

  //checking the recieved payload
  const int slice_size = 32 * 1024;
  if (FLAGS_make_the_slices_corrupt_) {
  LOG(INFO)<<"recieved payload size _: "<<(*payload_rec)->size();
  LOG(INFO)<<"num slices in recieved payload _: "<<(*payload_rec)->size()/slice_size;

  LOG(INFO)<<"unreadable_size:"<<unreadable_slices->size();
  }
  bool is_unreadable;


  // Verify checksums.
  if(!FLAGS_make_the_slices_corrupt_){
    CHECK_EQ(slice_state_vec->size(), slice_id_cksum_map.size());
  }


  for (uint ii = 0; ii < slice_state_vec->size(); ++ii) {


    if (FLAGS_make_the_slices_corrupt_) {


      is_unreadable=(find(unreadable_slices->begin(), unreadable_slices->end(), (*slice_state_vec)[ii]->slice_id()) 
                                                                                      != unreadable_slices->end());
      LOG(INFO)<<"checksum verification for slice id"<<(*slice_state_vec)[ii]->slice_id()
               <<", is unreadable : "<<is_unreadable<<" ,ii:"<<ii;
      if(is_unreadable) 
          continue;
    }
    

    const EgroupOpsTester::SliceState::PtrConst& slice_state =
      (*slice_state_vec)[ii];

    LOG(INFO)<<"checksum verification for slice id"<<slice_state->slice_id();
    const vector<uint> cksum_vec = slice_id_cksum_map[slice_state->slice_id()];
    for (int jj = 0; jj < slice_state->checksum_size(); ++jj) {
      CHECK_EQ(slice_state->checksum(jj), cksum_vec[jj]);
    }
  }



}

static void VerifyEgroupMetadata(
  const int64 egroup_id,
  const shared_ptr<ReplicaReadEgroupRet>& remote_ret,
  const int64 info_egroup_id,
  const vector<shared_ptr<EgroupOpsTester::SliceState>>&
    slice_state_vec,
  const vector<shared_ptr<EgroupOpsTester::Extent>>& extent_state_vec,
  const bool should_contain_marker) {

  ScopedArenaMark am;
  CHECK(remote_ret->has_extent_group_metadata());
  const bool verify_associated_info_egroup = info_egroup_id >= 0;
  CHECK(!verify_associated_info_egroup ||
        remote_ret->info_egroups_size() > 0)
    << egroup_id << ":" << info_egroup_id;
  MedusaExtentGroupIdMapEntryProto *extent_group_metadata = nullptr;
  if (verify_associated_info_egroup) {
    for (int ii = 0; ii < remote_ret->info_egroups_size(); ++ii) {
      const auto& info_egroup = remote_ret->info_egroups().Get(ii);
      if (info_egroup_id == info_egroup.extent_group_id()) {
        extent_group_metadata =
          const_cast<MedusaExtentGroupIdMapEntryProto *>(
            &info_egroup.extent_group_metadata());
        break;
      }
    }
  } else {
    extent_group_metadata =
      const_cast<MedusaExtentGroupIdMapEntryProto *>(
        &remote_ret->extent_group_metadata());
  }
  CHECK(extent_group_metadata) << egroup_id << ":" << info_egroup_id;

  CHECK_EQ(remote_ret->is_erasure_coded(), should_contain_marker)
    << egroup_id << ":" << info_egroup_id;

  // Verify that the extents and slice metadata are indeed present
  // in the result received.
  // TODO(Hema): Add checks to validate the individual entries of every
  // extent and slice.
  MedusaExtentGroupIdMapEntry egid_entry(extent_group_metadata);

  CHECK_EQ(extent_state_vec.size(),
           extent_group_metadata->extents_size())
    << egroup_id << ":" << info_egroup_id;
  for (const auto& expected_extent : extent_state_vec) {
    const ExtentId::PtrConst extent_id = make_shared<ExtentId>(
      &expected_extent->extent_id(), true);

    ExtentState::ArenaUPtrConst extent_state =
      egid_entry.LookupExtent(extent_id, am.arena());
    CHECK(extent_state) << egroup_id << ":" << info_egroup_id;
  }

  CHECK_EQ(slice_state_vec.size(),
           extent_group_metadata->slices_size());
  for (const auto& expected_slice : slice_state_vec) {
    SliceState::UPtrConst slice_state =
      egid_entry.LookupSlice(expected_slice->slice_id(), am.arena());
    CHECK(slice_state) << egroup_id << ":" << info_egroup_id;
  }
}

void EgroupOpsTester::ReplicaReadEgroupAndVerifyMetadata(
  int64 egroup_id,
  int64 disk_id,
  const vector<shared_ptr<EgroupOpsTester::SliceState>>&
    slice_state_vec,
  const vector<shared_ptr<EgroupOpsTester::Extent>>& extent_state_vec,
  IOBuffer::Ptr *const data,
  const bool managed_by_aes,
  const bool should_contain_marker,
  const int32 inject_error_type,
  const int64 expected_global_intent_sequence,
  const int64 info_egroup_id) {

  // This function is applicable for AES egroups only.
  DCHECK(managed_by_aes) << egroup_id;

  // Get stargate interface associated with the primary disk.
  const StargateInterface::Ptr& iface =
    cluster_mgr_->stargate_mgr()->GetStargateInterfaceForDisk(disk_id);
  CHECK(iface);

  shared_ptr<ReplicaReadEgroupArg> remote_arg =
    make_shared<ReplicaReadEgroupArg>();
  remote_arg->set_extent_group_id(egroup_id);
  remote_arg->set_disk_id(disk_id);
  remote_arg->set_qos_principal_name(string());
  remote_arg->set_qos_priority(StargateQosPriority::kDefault);
  remote_arg->set_managed_by_aes(managed_by_aes);
  int64 expected_intent_sequence = -1;
  if (!managed_by_aes) {
    unordered_map<int64, int64>::iterator iter =
      egroup_id_intent_seq_map_.find(egroup_id);
    CHECK(iter != egroup_id_intent_seq_map_.end());
    expected_intent_sequence = iter->second;
  }
  remote_arg->set_expected_intent_sequence(expected_intent_sequence);
  remote_arg->set_global_metadata_intent_sequence(
    expected_global_intent_sequence);
  Notification notification;
  shared_ptr<ReplicaReadEgroupRet> verify_remote_ret;

  // Set the expected error as kNoError if we are expecting no error to
  // occur or a roll forward. Otherwise we should not see the extent
  // group present, provided this was the first write to the extent group.
  // Currently this assumption is held as the test only issues initial
  // writes to AES parity egroups.
  const StargateError::Type expected_error =
    (inject_error_type == -1 ||
     inject_error_type == 9 /* kNoFinalizeTentativeUpdates */) ?
       StargateError::kNoError : StargateError::kExtentGroupNonExistent;
  Function<void(StargateError::Type,
                shared_ptr<string>,
                shared_ptr<ReplicaReadEgroupRet>,
                IOBuffer::Ptr&&)> done_cb =
    func_disabler_.Bind(&ReplicaReadEgroupDone,
                        _1, _2, _3, _4, &notification, data,
                        &verify_remote_ret, expected_error,
                        managed_by_aes, remote_arg);
  iface->ReplicaReadEgroup(
    move(remote_arg), move(done_cb),
    FLAGS_egroup_test_replica_read_eg_rpc_timeout * 1000);
  notification.Wait();

  if (expected_error == StargateError::kNoError) {
    CHECK(verify_remote_ret);
    CHECK(verify_remote_ret.get());

    VerifyEgroupMetadata(egroup_id, verify_remote_ret, info_egroup_id,
                         slice_state_vec, extent_state_vec,
                         should_contain_marker);
  }
}

void EgroupOpsTester::VerifyAESECStripMetadata(
  const vector<int64>& info_egroup_ids,
  const vector<int64>& info_disk_ids,
  const vector<int64>& parity_egroup_ids,
  const vector<int64>& parity_disk_ids,
  vector<vector<SliceState::Ptr>>& info_slice_state_vecs,
  vector<vector<shared_ptr<EgroupOpsTester::Extent>>>&
    info_extent_state_vecs,
  const int32 primary_parity_inject_error_type,
  const int32 secondary_parity_inject_error_type,
  const bool should_contain_marker,
  const bool read_from_info_also) {

  DCHECK_EQ(parity_egroup_ids.size(), 2);
  const int64 primary_parity_egroup_id = parity_egroup_ids.at(0);
  const int64 secondary_parity_egroup_id = parity_egroup_ids.at(1);
  const int64 primary_parity_disk_id = parity_disk_ids.at(0);
  const int64 secondary_parity_disk_id = parity_disk_ids.at(1);

  // Now that the strip is written with AES egroups, verify that the
  // metadata of info egroups is indeed present on the parity replicas.
  for (uint ii = 0; ii < info_egroup_ids.size(); ++ii) {
    const int64 info_egroup_id = info_egroup_ids.at(ii);

    // Sort the slice states, if not already.
    sort(info_slice_state_vecs.at(ii).begin(),
         info_slice_state_vecs.at(ii).end(),
         medusa::SliceState::SliceStatePtrOrderIncreasingFileOffset);

    if (read_from_info_also) {
      const int64 info_disk_id = info_disk_ids.at(ii);
      LOG(INFO) << "Reading info egroup " << info_egroup_id
                << " from the data replica " << info_disk_id;
      IOBuffer::Ptr info_transformed_data = make_shared<IOBuffer>();
      ReplicaReadEgroupAndVerifyMetadata(
        info_egroup_id, info_disk_id,
        info_slice_state_vecs.at(ii),
        info_extent_state_vecs.at(ii), &info_transformed_data,
        true /* managed_by_aes */, should_contain_marker,
        -1 /* primary_parity_inject_error_type */,
        0 /* expected_global_intent_sequence */, -1 /* info_egroup_id */);
    }

    // Read replica on primary parity for this info egroup and verify
    // the metadata is consistent with its primary replica.
    if (primary_parity_inject_error_type < 0) {
      LOG(INFO) << "Reading primary parity for info egroup "
                << info_egroup_id;
      IOBuffer::Ptr transformed_data = make_shared<IOBuffer>();
      ReplicaReadEgroupAndVerifyMetadata(
        primary_parity_egroup_id, primary_parity_disk_id,
        info_slice_state_vecs.at(ii),
        info_extent_state_vecs.at(ii), &transformed_data,
        true /* managed_by_aes */, true /* should_contain_marker */,
        primary_parity_inject_error_type,
        0 /* expected_global_intent_sequence */, info_egroup_id);
    }

    // Read replica on secondary parity for this info egroup and verify
    // the metadata is consistent with its primary replica.
    if (secondary_parity_inject_error_type < 0) {
      LOG(INFO) << "Reading secondary parity for info egroup "
                << info_egroup_id;
      IOBuffer::Ptr transformed_data = make_shared<IOBuffer>();
      ReplicaReadEgroupAndVerifyMetadata(
        secondary_parity_egroup_id, secondary_parity_disk_id,
        info_slice_state_vecs.at(ii),
        info_extent_state_vecs.at(ii), &transformed_data,
        true /* managed_by_aes */, true /* should_contain_marker */,
        secondary_parity_inject_error_type,
        0 /* expected_global_intent_sequence */, info_egroup_id);
    }
  }
}

// Callback for Medusa::LookupExtentGroupIdMap().
static void LookupExtentGroupIdMapCompleted(
  MedusaError::Type err,
  shared_ptr<vector<Medusa::MedusaValue::PtrConst>> mvalue_vec,
  vector<Medusa::MedusaValue::PtrConst> *mvalue_vec_ret,
  Notification *notify) {

  CHECK_EQ(err, MedusaError::kNoError);
  CHECK(mvalue_vec_ret);
  CHECK(mvalue_vec);

  for (uint ii = 0; ii < mvalue_vec->size(); ++ii) {
    mvalue_vec_ret->emplace_back((*mvalue_vec)[ii]);
  }

  if (notify) {
    notify->Notify();
  }
}

// Callback for Medusa::LookupVDiskBlockMap().
static void LookupVDiskBlockMapCompleted(
  MedusaError::Type err,
  shared_ptr<vector<Medusa::MedusaValue::PtrConst>> mvalue_vec,
  vector<Medusa::MedusaValue::PtrConst> *mvalue_vec_ret,
  Notification *notify) {

  CHECK_EQ(err, MedusaError::kNoError);
  CHECK(mvalue_vec_ret);
  CHECK(mvalue_vec);

  for (uint ii = 0; ii < mvalue_vec->size(); ++ii) {
    mvalue_vec_ret->emplace_back((*mvalue_vec)[ii]);
  }

  if (notify) {
    notify->Notify();
  }
}

// Callback for ReplicateExtentGroup RPC.
static void ReplicateExtentGroupDone(StargateError::Type err,
                                     shared_ptr<string> err_detail,
                                     shared_ptr<ReplicateExtentGroupRet> ret,
                                     const StargateError::Type expected_error,
                                     Notification *notify) {
  CHECK_EQ(err, expected_error);
  if (notify) {
    notify->Notify();
  }
}

// Callback for Medusa::UpdateExtentGroupIdMap().
static void TentativeUpdateCompleted(
  shared_ptr<vector<MedusaError::Type> > err_vec,
  Notification *notify) {
  if (notify) {
    notify->Notify();
  }
}

void EgroupOpsTester::ReplicateExtentGroup(
  const int64 egroup_id,
  const int64 source_disk_id,
  const int64 dest_disk_id,
  const vector<shared_ptr<EgroupOpsTester::SliceState>>& slice_state_vec,
  const int64 latest_intent_sequence,
  const bool managed_by_aes,
  const bool extent_based_format,
  const int64 owner_container_id,
  const vector<ExtentIdProto> *const extent_id_list,
  const StargateError::Type expected_error,
  Notification *const user_notification,
  const bool is_primary) {

  // Get stargate interface associated with the destination disk.
  const StargateInterface::Ptr& iface =
    cluster_mgr_->stargate_mgr()->GetStargateInterfaceForDisk(dest_disk_id);
  CHECK(iface);

  shared_ptr<ReplicateExtentGroupArg> arg =
    make_shared<ReplicateExtentGroupArg>();
  arg->set_extent_group_id(egroup_id);
  arg->set_disk_id(dest_disk_id);
  arg->set_remote_disk_id(source_disk_id);
  arg->set_qos_principal_name(string());
  arg->set_qos_priority(StargateQosPriority::kDefault);
  arg->set_managed_by_aes(managed_by_aes);
  arg->set_extent_based_metadata_format(extent_based_format);
  arg->set_migration_reason(MedusaExtentGroupIdMapEntryProto::kILM);
  unordered_map<int64, int64>::iterator iter =
    egroup_id_intent_seq_map_.find(egroup_id);
  CHECK(iter != egroup_id_intent_seq_map_.end());
  if (managed_by_aes) {
    CHECK_GE(owner_container_id, 0);
    CHECK(extent_id_list);
    arg->set_expected_intent_sequence(-1);
    arg->set_owner_container_id(owner_container_id);
    for (uint ii = 0; ii < extent_id_list->size(); ++ii) {
      arg->add_extent_id_list()->CopyFrom((*extent_id_list)[ii]);
    }
    arg->set_is_primary(is_primary);
  } else {
    arg->set_expected_intent_sequence(iter->second);
    for (uint ii = 0; ii < slice_state_vec.size(); ++ii) {
      CHECK(slice_state_vec[ii]->has_extent_group_offset());
      slice_state_vec[ii]->CopyTo(arg->add_slices());
    }
  }
  if (latest_intent_sequence >= 0) {
    arg->set_intent_sequence(latest_intent_sequence);
  } else {
    arg->set_intent_sequence(iter->second + 1);
  }

  LOG(INFO) << "Replicating egroup " << egroup_id
            << " from disk " << source_disk_id << " to "
            << dest_disk_id << " expected intent sequence: "
            << iter->second << " managed_by_aes: " << managed_by_aes
            << " arg: " << arg->ShortDebugString();

  // Save a copy to the configuration as recommended in stargate.h.
  Configuration::PtrConst config = cluster_mgr_->Config();
  // Read the metadata of the extent group from Medusa's extent group id map.
  shared_ptr<vector<int64>> extent_group_id_vec = make_shared<vector<int64>>();
  extent_group_id_vec->push_back(egroup_id);
  vector<Medusa::MedusaValue::PtrConst> mvalue_vec_ret;
  Notification notification;
  const Function<void(MedusaError::Type,
                      shared_ptr<vector<Medusa::MedusaValue::PtrConst>>)>
    done_cb =
      func_disabler_.Bind(&LookupExtentGroupIdMapCompleted,
                          _1, _2, &mvalue_vec_ret, &notification);
  cluster_mgr_->medusa()->LookupExtentGroupIdMap(extent_group_id_vec,
                                                 false /* cached_ok */,
                                                 done_cb);
  notification.Wait();
  CHECK_EQ(mvalue_vec_ret.size(), 1);
  Medusa::MedusaValue::PtrConst mvalue = move(mvalue_vec_ret[0]);
  CHECK(mvalue && mvalue->extent_groupid_map_entry);
  const MedusaExtentGroupIdMapEntry *egroup_metadata =
    mvalue->extent_groupid_map_entry.get();

  StargateUtil::FetchExtentGroupDiskUsage(
    arg->mutable_disk_usage(), egroup_id, egroup_metadata, config);

  // Add dest_disk_id to the egroup metadata in medusa.
  Medusa::MedusaValue::Ptr new_mvalue = make_shared<Medusa::MedusaValue>();
  new_mvalue->epoch = mvalue->epoch;
  new_mvalue->timestamp = mvalue->timestamp + 1;

  unique_ptr<MedusaExtentGroupIdMapEntryProto::ControlBlock>
    new_control_block =
    make_unique<MedusaExtentGroupIdMapEntryProto::ControlBlock>();
  new_control_block->CopyFrom(*egroup_metadata->control_block());
  MedusaExtentGroupIdMapEntryProto::Replica *replica =
    new_control_block->add_replicas();
  replica->set_disk_id(dest_disk_id);
  new_mvalue->extent_groupid_map_entry =
    make_shared<MedusaExtentGroupIdMapEntry>(
      move(new_control_block),
      nullptr /* diff_slice_map */, nullptr /* diff_extent_map */);

  // Schedule new_mvalue to be written to Medusa.
  vector<int64> egid_vec(1, egroup_id);
  vector<Medusa::MedusaValue::Ptr> mvalue_vec(1, move(new_mvalue));

  notification.Reset();
  const Function<void(shared_ptr<vector<MedusaError::Type> >)>
    update_done_cb = func_disabler_.Bind(&TentativeUpdateCompleted, _1,
                                         &notification);
  cluster_mgr_->medusa()->UpdateExtentGroupIdMap(
    make_shared<vector<int64>>(move(egid_vec)),
    true /* use_CAS */,
    make_shared<vector<Medusa::MedusaValue::Ptr>>(move(mvalue_vec)),
    update_done_cb);
  notification.Wait();

  LOG(INFO) << "Added disk " << dest_disk_id
            << " in medusa metadata of egroup " << egroup_id
            << " arg: " << arg->ShortDebugString();

  notification.Reset();
  const Function<void(StargateError::Type,
                      shared_ptr<string>,
                      shared_ptr<ReplicateExtentGroupRet>)> replicate_done_cb =
    func_disabler_.Bind(&ReplicateExtentGroupDone, _1, _2, _3, expected_error,
                        (user_notification ?
                         user_notification : &notification));
  iface->ReplicateExtentGroup(arg, replicate_done_cb);
  if (!user_notification) {
    notification.Wait();
  }
}

// Convert MedusaExtentGroupIdMapEntry to MedusaExtentGroupIdMapEntryProto.
static void ConvertEGroupEntryToProto(
  shared_ptr<const MedusaExtentGroupIdMapEntry> egroup_entry,
  MedusaExtentGroupIdMapEntryProto *egroup_proto) {

  CHECK(egroup_entry);
  CHECK(egroup_proto);

  MedusaExtentGroupIdMapEntryProto_ControlBlock *cblock =
    egroup_proto->mutable_control_block();
  cblock->CopyFrom(*egroup_entry->control_block());

  // Fill slice state.
  ScopedArenaMark am;
  for (MedusaExtentGroupIdMapEntry::SliceIterator siter(
         egroup_entry.get(), am.arena());
       !siter.End(); siter.Next()) {
    siter.Second()->CopyTo(egroup_proto->add_slices());
  }

  // Fill extent state.
  for (MedusaExtentGroupIdMapEntry::ExtentIterator eiter(
         egroup_entry.get(), am.arena());
      !eiter.End(); eiter.Next()) {
    eiter.Second()->CopyTo(egroup_proto->add_extents());
  }
}

static void ConvertEgroupToAesDone(const StargateError::Type err,
                                   const shared_ptr<string> err_detail,
                                   const shared_ptr<ConvertEgroupToAESRet> ret,
                                   const StargateError::Type expected_error,
                                   Notification *notify) {
  CHECK_EQ(err, expected_error);

  if (notify) {
    notify->Notify();
  }
}

void EgroupOpsTester::ConvertEgroupToAES(
  const int64 egroup_id,
  const int64 disk_id,
  const int64 intent_sequence,
  const bool is_primary,
  const StargateError::Type expected_error) {

  Medusa::MedusaValue::PtrConst mvalue =
    medusa_connector_->LookupExtentGroupIdMapEntry(egroup_id);
  CHECK(mvalue && mvalue->extent_groupid_map_entry);

  shared_ptr<ConvertEgroupToAESArg> arg =
    make_shared<ConvertEgroupToAESArg>();
  arg->set_extent_group_id(egroup_id);
  arg->set_disk_id(disk_id);
  arg->set_intent_sequence(intent_sequence);
  arg->set_expected_intent_sequence(
    egroup_id_intent_seq_map_[egroup_id]);
  arg->set_is_primary(is_primary);
  arg->set_slices_stored_by_id(false);
  arg->set_slice_group_size(32);
  MedusaExtentGroupIdMapEntryProto *const eg_entry =
    arg->mutable_extent_group_metadata();
  ConvertEGroupEntryToProto(mvalue->extent_groupid_map_entry, eg_entry);

  Notification notification;
  const Function<void(StargateError::Type,
                      shared_ptr<string>,
                      shared_ptr<ConvertEgroupToAESRet>)>
    convert_done_cb = func_disabler_.Bind(
      &ConvertEgroupToAesDone, _1, _2, _3, expected_error, &notification);
  LOG(INFO) << "Perform convert on egroup " << egroup_id << " "
            << arg->ShortDebugString();
  const StargateInterface::Ptr& iface =
    cluster_mgr_->stargate_mgr()->GetStargateInterfaceForDisk(disk_id);
  iface->ConvertEgroupToAES(arg, move(convert_done_cb));
  notification.Wait();
}

static void TruncateEgroupsDone(
  StargateError::Type err,
  shared_ptr<string> err_detail,
  shared_ptr<TruncateEgroupsRet> remote_ret,
  Notification *notify,
  const shared_ptr<vector<int64>>& expected_err,
  bool error_injected) {

  if (!error_injected) {
    if (expected_err->at(0) == StargateError::kTransportError) {
      // The RPC was expected to fail due to a stargate restart.
      CHECK_EQ(err, StargateError::kTransportError) << err_detail;
    } else {
      CHECK_EQ(err, StargateError::kNoError) << err_detail;
      CHECK_EQ(remote_ret->err_status_size(), expected_err->size());

      for (int ii = 0; ii < remote_ret->err_status_size(); ++ii) {
        CHECK_EQ(remote_ret->err_status(ii), expected_err->at(ii)) << ii;
      }
    }
  } else {
    CHECK_NE(err, StargateError::kNoError) << "Truncate RPC succeeded, but it"
                                           << " was expected to fail";
  }
  if (notify) {
    notify->Notify();
  }
}

void EgroupOpsTester::TruncateEgroups(
  int64 disk_id,
  const shared_ptr<vector<int64>>& egroup_ids,
  const shared_ptr<vector<int64>>& expected_err,
  const shared_ptr<vector<int>>& transformed_size_vec,
  bool inject_error,
  Notification *const user_notification) {

  CHECK(egroup_ids) << disk_id;
  CHECK(expected_err) << disk_id;
  CHECK(transformed_size_vec) << disk_id;
  CHECK_EQ(egroup_ids->size(), expected_err->size()) << disk_id;
  CHECK_EQ(egroup_ids->size(), transformed_size_vec->size()) << disk_id;

  // Select the Stargate interface corresponding to the disk_id.
  const StargateInterface::Ptr& iface =
    cluster_mgr_->stargate_mgr()->GetStargateInterfaceForDisk(disk_id);
  CHECK(iface) << "Stargate interface not found for disk " << disk_id;

  shared_ptr<TruncateEgroupsArg> arg = make_shared<TruncateEgroupsArg>();

  if (inject_error == true) {
    // Set a bad disk_id.
    arg->set_disk_id(-1);
  } else {
    arg->set_disk_id(disk_id);
  }

  arg->set_qos_priority(StargateQosPriority::kDefault);

  for (uint ii = 0 ; ii < egroup_ids->size(); ++ii) {
    TruncateEgroupsArg::ExtentGroup *eg = arg->add_extent_groups();
    eg->set_extent_group_id(egroup_ids->at(ii));
    eg->set_egroup_intent_sequence(
      egroup_id_intent_seq_map_[egroup_ids->at(ii)]);
    eg->set_container_id(container_id_);
    eg->set_transformed_size_in_medusa((*transformed_size_vec)[ii]);
  }

  Notification notification;
  const Function<void(StargateError::Type,
                      shared_ptr<string>,
                      shared_ptr<TruncateEgroupsRet>)> done_cb =
    func_disabler_.Bind(&TruncateEgroupsDone,
                        _1, _2, _3,
                        (user_notification ? user_notification :
                                             &notification),
                        expected_err, inject_error);

  notification.Reset();
  iface->TruncateExtentGroups(arg, &done_cb);
  if (!user_notification) {
    notification.Wait();
  }
}

static void RelocateEgroupsDone(
  StargateError::Type err,
  shared_ptr<string> err_detail,
  shared_ptr<RelocateEgroupsRet> remote_ret,
  Notification *notify,
  const shared_ptr<vector<int64>>& expected_err,
  bool error_injected) {

  if (!error_injected) {
    if (expected_err->at(0) == StargateError::kTransportError) {
      // The RPC was expected to fail due to a stargate restart.
      CHECK_EQ(err, StargateError::kTransportError) << err_detail;
    } else {
      CHECK_EQ(err, StargateError::kNoError) << err_detail;
      CHECK_EQ(remote_ret->err_status_size(), expected_err->size());

      for (int ii = 0; ii < remote_ret->err_status_size(); ++ii) {
        CHECK_EQ(remote_ret->err_status(ii), expected_err->at(ii)) << ii;
      }
    }
  } else {
    CHECK_NE(err, StargateError::kNoError) << "Relocate RPC succeeded, but it"
                                           << " was expected to fail";
  }
  if (notify) {
    notify->Notify();
  }
}

void EgroupOpsTester::RelocateEgroups(
  int64 disk_id,
  const shared_ptr<vector<int64>>& egroup_ids,
  const shared_ptr<vector<int64>>& expected_err,
  bool inject_error,
  Notification *const user_notification) {

  CHECK(egroup_ids) << disk_id;
  CHECK(expected_err) << disk_id;
  CHECK_EQ(egroup_ids->size(), expected_err->size()) << disk_id;

  // Select the Stargate interface corresponding to the disk_id.
  const StargateInterface::Ptr& iface =
    cluster_mgr_->stargate_mgr()->GetStargateInterfaceForDisk(disk_id);
  CHECK(iface) << "Stargate interface not found for disk " << disk_id;

  shared_ptr<RelocateEgroupsArg> arg = make_shared<RelocateEgroupsArg>();

  if (inject_error == true) {
    // Set a bad disk_id.
    arg->set_disk_id(-1);
  } else {
    arg->set_disk_id(disk_id);
  }

  arg->set_qos_priority(StargateQosPriority::kDefault);

  for (uint ii = 0 ; ii < egroup_ids->size(); ++ii) {
    arg->add_extent_group_id(egroup_ids->at(ii));
  }

  Notification notification;
  const Function<void(StargateError::Type,
                      shared_ptr<string>,
                      shared_ptr<RelocateEgroupsRet>)> done_cb =
    func_disabler_.Bind(&RelocateEgroupsDone,
                        _1, _2, _3,
                        (user_notification ? user_notification :
                                             &notification),
                        expected_err, inject_error);

  notification.Reset();
  iface->RelocateExtentGroups(arg, &done_cb);
  if (!user_notification) {
    notification.Wait();
  }
}

void EgroupOpsTester::UpdateErasureCodingConfiguration(
  const int32 info_size,
  const int32 parity_size) {

  int update_config_count = 0;
  while (true) {
    LOG(INFO) << "update_config_count: " << update_config_count
              << " change container EC config "
              << info_size << "/" << parity_size;
    shared_ptr<ConfigurationProto> mutable_proto =
      make_shared<ConfigurationProto>();
    mutable_proto->CopyFrom(*(cluster_mgr_->Config()->config_proto()));
    ConfigurationProto::Container *ctr = NULL;
    for (int ii = 0; ii < mutable_proto->container_list_size(); ++ii) {
      if (mutable_proto->container_list(ii).container_id() ==
          container_id_) {
        ctr = mutable_proto->mutable_container_list(ii);
        break;
      }
    }
    CHECK(ctr) << "Container " << container_id_ << " not found in Zeus config";
    ConfigurationProto::ContainerParams *params = ctr->mutable_params();
    const int current_replication_factor = params->replication_factor();
    const int new_replication_factor = parity_size + 1;
    params->set_replication_factor(new_replication_factor);
    LOG(INFO) << "Old replication factor " << current_replication_factor
              << " new replication factor " << new_replication_factor;
    if (parity_size > 0) {
      CHECK_GT(info_size, 0);
      string current_erasure_code = params->erasure_code();
      current_erasure_code = StringJoin(info_size, "/", parity_size);
      LOG(INFO) << "Old erasure_code: " << params->erasure_code()
                << " updated erasure_code: " << current_erasure_code;
      params->set_erasure_code(current_erasure_code);
    } else {
      CHECK_LE(info_size, 0);
      CHECK(params->has_erasure_code()) << ctr->ShortDebugString();
      params->clear_erasure_code();
    }
    mutable_proto->set_logical_timestamp(
      mutable_proto->logical_timestamp() + 1);
    ++update_config_count;
    if (cluster_mgr_->zeus_helper()->UpdateConfigurationSync(*mutable_proto)) {
      return;
    }
  }
}

void EgroupOpsTester::ValidateAESECConfig(
  const vector<int64>& info_egroup_ids,
  const vector<int64>& info_disk_ids,
  const int64 error_inject_egid) {

  for (uint ii = 0; ii < info_egroup_ids.size(); ++ii) {
    const int64 info_egroup_id = info_egroup_ids.at(ii);
    const int64 info_disk_id = info_disk_ids.at(ii);

    // Issue a ReplicaRead rpc to fetch the AES egroup metadata and confirm
    // the expected erasure code status.
    IOBuffer::Ptr transformed_data = make_shared<IOBuffer>();
    shared_ptr<ReplicaReadEgroupRet> remote_ret;
    ReplicaReadEgroup(
      info_egroup_id, info_disk_id,
      vector<shared_ptr<EgroupOpsTester::SliceState>>() /* slice_state_vec */,
      &transformed_data,
      StargateError::kNoError /* expected_error */,
      true /* managed_by_aes */, nullptr /* user_notification */,
      &remote_ret);

    // Confirm that we have successfully marked the info egroup as erasure
    // coded at the extent store only if there was no error injected.
    CHECK_GT(transformed_data->size(), 0);
    CHECK(remote_ret && remote_ret.get());
    CHECK(remote_ret->has_extent_group_metadata());
    CHECK_EQ(error_inject_egid == info_egroup_id,
             !remote_ret->is_erasure_coded())
      << OUTVARS(error_inject_egid, info_egroup_id, info_disk_id,
                 remote_ret->is_erasure_coded());
    LOG(INFO) << "Info egroup " << info_egroup_id
              << " on primary disk id " << info_disk_id
              << " error_inject_egid " << error_inject_egid
              << " ec_enabled " << remote_ret->is_erasure_coded();
  }
}

static void ErasureEncodeDone(
  const StargateError::Type err,
  const shared_ptr<string>& err_detail,
  const shared_ptr<ErasureCodeEncodeEgroupsRet>& ret,
  const bool inject_error,
  Notification *const notification) {

  CHECK((err == StargateError::kNoError && ret) ||
        (inject_error && err == StargateError::kCanceled));
  notification->Notify();
}

void EgroupOpsTester::ErasureEncodeAESEgroups(
  const vector<int64>& egroup_ids,
  const vector<int64>& primary_replica_disk_ids,
  vector<int64> *parity_egroup_ids,
  vector<int64> *parity_disk_ids,
  const bool inject_error,
  const int parity_size,
  int64 *const error_inject_egid) {

  DCHECK_EQ(egroup_ids.size(),
            primary_replica_disk_ids.size());

  UpdateErasureCodingConfiguration(egroup_ids.size(), parity_size);

  // Read the metadata of the extent group from Medusa's extent group id map.
  shared_ptr<vector<int64>> extent_group_id_vec = make_shared<vector<int64>>();
  for (const int egroup_id : egroup_ids) {
    extent_group_id_vec->push_back(egroup_id);
  }
  vector<Medusa::MedusaValue::PtrConst> mvalue_vec_ret;

  Notification notification;
  const Function<void(MedusaError::Type,
                      shared_ptr<vector<Medusa::MedusaValue::PtrConst>>)>
    done_cb1 =
      func_disabler_.Bind(&LookupExtentGroupIdMapCompleted,
                          _1, _2, &mvalue_vec_ret, &notification);
  cluster_mgr_->medusa()->LookupExtentGroupIdMap(extent_group_id_vec,
                                                 false /* cached_ok */,
                                                 move(done_cb1));
  notification.Wait();
  CHECK_EQ(mvalue_vec_ret.size(), egroup_ids.size());

  shared_ptr<ErasureCodeArg> arg = make_shared<ErasureCodeArg>();
  arg->set_qos_priority(StargateQosPriority::kDefault);
  arg->set_qos_principal_name(StargateUtil::kBackgroundQosPrincipal);
  for (uint ii = 0; ii < egroup_ids.size(); ++ii) {
    ErasureCodeArg::ExtentGroup*
      extent_group = arg->add_extent_groups();
    extent_group->set_extent_group_id(egroup_ids.at(ii));
    extent_group->set_disk_id(primary_replica_disk_ids.at(ii));
  }
  arg->set_task_type(ErasureCodeArg::kEncode);

  int64 failed_svm_id = -1;
  if (inject_error) {
    // Find a random info egroup and inject error in extent store for the same,
    // such that SetExtentGroup RPC updates will fail for that info egroup.
    const int32 failed_info_egroup_index =
      Random::TL()->Uniform(0, static_cast<int32>(egroup_ids.size() - 1));
    const int64 failed_info_disk_id = primary_replica_disk_ids.at(
      failed_info_egroup_index);
    failed_svm_id = cluster_mgr_->stargate_mgr()->GetSvmIdForStargate(
      cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(
        failed_info_disk_id));
    LOG(INFO) << "Marking egroup " << egroup_ids.at(failed_info_egroup_index)
              << " in svm " << failed_svm_id << " as failed egroup";
    cluster_mgr_->stargate_mgr()->SetGFlag(
      "stargate_experimental_fail_updates_for_egroup_id",
      to_string(egroup_ids.at(failed_info_egroup_index)), failed_svm_id);

    if (error_inject_egid) {
      *error_inject_egid = egroup_ids.at(failed_info_egroup_index);
    }
  }

  // We can send to any stargate, just send to stargate at index 0.
  interface::StargateInterface::Ptr stargate_ifc =
    cluster_mgr_->stargate_mgr()->stargate_ifc(0);
  CHECK(stargate_ifc);

  notification.Reset();
  stargate_ifc->ErasureCodeEncodeEgroups(
    arg, func_disabler_.Bind(&ErasureEncodeDone, _1, _2, _3, inject_error,
                             &notification));
  notification.Wait();

  // Reset the gflag back to default for further tests.
  if (failed_svm_id >= 0) {
    DCHECK(inject_error) << failed_svm_id;
    cluster_mgr_->stargate_mgr()->SetGFlag(
      "stargate_experimental_fail_updates_for_egroup_id",
      to_string(-1), failed_svm_id);
  }

  // Now that the erasure coding is successfully complete,
  // lookup the egid map entries for all the info egroups and confirm
  // that the ec update was made.

  // Read the metadata of the extent group from Medusa's extent group id map.
  mvalue_vec_ret.clear();
  notification.Reset();
  const Function<void(MedusaError::Type,
                      shared_ptr<vector<Medusa::MedusaValue::PtrConst>>)>
    done_cb2 =
      func_disabler_.Bind(&LookupExtentGroupIdMapCompleted,
                          _1, _2, &mvalue_vec_ret, &notification);
  cluster_mgr_->medusa()->LookupExtentGroupIdMap(extent_group_id_vec,
                                                 false /* cached_ok */,
                                                 move(done_cb2));
  notification.Wait();
  CHECK_EQ(mvalue_vec_ret.size(), egroup_ids.size());

  // Retrieve the parity info and set in the out params.
  Medusa::MedusaValue::PtrConst mvalue = mvalue_vec_ret[0];
  CHECK(mvalue && mvalue->extent_groupid_map_entry);
  const MedusaExtentGroupIdMapEntry *egroup_metadata =
    mvalue->extent_groupid_map_entry.get();
  const auto *control_block = egroup_metadata->control_block();
  if (!inject_error) {
    CHECK_EQ(control_block->erasure_code_parity_egroup_ids_size(),
             parity_size);
    for (int ii = 0; ii < parity_size; ++ii) {
      parity_egroup_ids->emplace_back(
        control_block->erasure_code_parity_egroup_ids(ii));
    }
  } else {
    CHECK(!StargateUtil::IsErasureCodedEgroup(*control_block));
    return;
  }

  // Verify the parity info is consistent across all the info
  // egroups.
  for (uint ii = 1; ii < mvalue_vec_ret.size(); ++ii) {
    Medusa::MedusaValue::PtrConst mvalue = mvalue_vec_ret[0];
    CHECK(mvalue && mvalue->extent_groupid_map_entry);
    const MedusaExtentGroupIdMapEntry *egroup_metadata =
      mvalue->extent_groupid_map_entry.get();
    const auto *control_block = egroup_metadata->control_block();
    CHECK_EQ(control_block->erasure_code_parity_egroup_ids_size(),
             parity_size);
    for (int ii = 0; ii < parity_size; ++ii) {
      CHECK_EQ(parity_egroup_ids->at(ii),
               control_block->erasure_code_parity_egroup_ids(ii));
    }
  }

  mvalue_vec_ret.clear();
  notification.Reset();

  // Lookup the parity egid metadata and retrieve their primary
  // replica disk ids.
  shared_ptr<vector<int64>> parity_extent_group_id_vec =
    make_shared<vector<int64>>();
  for (const int egroup_id : *parity_egroup_ids) {
    parity_extent_group_id_vec->push_back(egroup_id);
  }
  const Function<void(MedusaError::Type,
                      shared_ptr<vector<Medusa::MedusaValue::PtrConst>>)>
    parity_done_cb =
      func_disabler_.Bind(&LookupExtentGroupIdMapCompleted,
                          _1, _2, &mvalue_vec_ret, &notification);
  cluster_mgr_->medusa()->LookupExtentGroupIdMap(parity_extent_group_id_vec,
                                                 false /* cached_ok */,
                                                 move(parity_done_cb));
  notification.Wait();

  CHECK_EQ(mvalue_vec_ret.size(), parity_egroup_ids->size());

  // Retrieve the parity info and set in the out params.
  for (const auto& mvalue : mvalue_vec_ret) {
    CHECK(mvalue && mvalue->extent_groupid_map_entry);
    const MedusaExtentGroupIdMapEntry *egroup_metadata =
      mvalue->extent_groupid_map_entry.get();
    const auto *control_block = egroup_metadata->control_block();
    CHECK_EQ(control_block->replicas_size(), 1);
    parity_disk_ids->emplace_back(control_block->replicas(0).disk_id());
  }
}

static void DeleteEgroupDone(
  const StargateError::Type err,
  const shared_ptr<string>& err_detail,
  const shared_ptr<DeleteExtentGroupsRet>& ret,
  const StargateError::Type expected_error,
  Notification *const notification) {

  CHECK_EQ(err, expected_error);
  if (expected_error == StargateError::kNoError) {
    CHECK(ret);
    for (int ii = 0; ii < ret->err_status_size(); ++ii) {
      CHECK_EQ(ret->err_status(ii), StargateError::kNoError);
    }
  }
  notification->Notify();
}

void EgroupOpsTester::DeleteExtentGroup(
  const int64 egroup_id, const int64 disk_id, const bool managed_by_aes,
  const bool extent_based_format, const StargateError::Type expected_error,
  Notification *const user_notification,
  bool delete_stale_replica_for_migration) {

  // Get stargate interface associated with the specified disk.
  const StargateInterface::Ptr& iface =
    cluster_mgr_->stargate_mgr()->GetStargateInterfaceForDisk(disk_id);
  CHECK(iface);

  LOG(INFO) << "Deleting extent group " << egroup_id
            << " from disk " << disk_id
            << " with latest intent sequence "
            << egroup_id_intent_seq_map_[egroup_id] + 1
            << ": " << OUTVARS(delete_stale_replica_for_migration);
  shared_ptr<DeleteExtentGroupsArg> arg = make_shared<DeleteExtentGroupsArg>();
  arg->add_extent_group_id(egroup_id);
  arg->add_intent_sequence(egroup_id_intent_seq_map_[egroup_id] + 1);
  arg->set_disk_id(disk_id);
  arg->set_qos_principal_name(string());
  arg->set_qos_priority(StargateQosPriority::kDefault);
  arg->add_managed_by_aes(managed_by_aes);
  arg->add_extent_based_metadata_format(extent_based_format);
  arg->set_delete_stale_replica_for_migration(
    delete_stale_replica_for_migration);

  // Save a copy to the configuration as recommended in stargate.h.
  Configuration::PtrConst config = cluster_mgr_->Config();

  // Read the metadata of the extent group from Medusa's extent group id map.
  shared_ptr<vector<int64>> extent_group_id_vec = make_shared<vector<int64>>();
  extent_group_id_vec->push_back(egroup_id);
  vector<Medusa::MedusaValue::PtrConst> mvalue_vec_ret;
  Notification notification;
  const Function<void(MedusaError::Type,
                      shared_ptr<vector<Medusa::MedusaValue::PtrConst>>)>
    done_cb =
      func_disabler_.Bind(&LookupExtentGroupIdMapCompleted,
                          _1, _2, &mvalue_vec_ret, &notification);
  notification.Reset();
  cluster_mgr_->medusa()->LookupExtentGroupIdMap(extent_group_id_vec,
                                                 false /* cached_ok */,
                                                 done_cb);
  notification.Wait();
  CHECK_EQ(mvalue_vec_ret.size(), 1);
  Medusa::MedusaValue::PtrConst mvalue = move(mvalue_vec_ret[0]);
  if (mvalue && mvalue->extent_groupid_map_entry) {
    const MedusaExtentGroupIdMapEntry *egroup_metadata =
      mvalue->extent_groupid_map_entry.get();

    StargateUtil::FetchExtentGroupDiskUsage(
      arg->add_disk_usage(), egroup_id, egroup_metadata, config);

    // Remove disk_id from the egroup metadata in Medusa or the entire entry if
    // we are deleting all the replicas.
    Medusa::MedusaValue::Ptr new_mvalue = make_shared<Medusa::MedusaValue>();
    new_mvalue->epoch = mvalue->epoch;
    new_mvalue->timestamp = mvalue->timestamp + 1;

    unique_ptr<MedusaExtentGroupIdMapEntryProto::ControlBlock>
      new_control_block =
      make_unique<MedusaExtentGroupIdMapEntryProto::ControlBlock>();
    new_control_block->CopyFrom(*egroup_metadata->control_block());
    new_control_block->clear_replicas();

    for (int ii = 0; ii < egroup_metadata->control_block()->replicas_size();
         ++ii) {
      if (egroup_metadata->control_block()->replicas(ii).disk_id() ==
          disk_id) {
        continue;
      }

      new_control_block->add_replicas()->CopyFrom(
        egroup_metadata->control_block()->replicas(ii));
    }
    if (new_control_block->replicas_size() == 0) {
      new_mvalue->extent_groupid_map_entry.reset();
    } else {
      new_mvalue->extent_groupid_map_entry =
        make_shared<MedusaExtentGroupIdMapEntry>(
          move(new_control_block), nullptr /* diff_slice_map */,
          nullptr /* diff_extent_map */);
    }

    // Schedule new_mvalue to be written to Medusa.
    vector<int64> egid_vec(1, egroup_id);
    vector<Medusa::MedusaValue::Ptr> mvalue_vec(1, move(new_mvalue));

    notification.Reset();
    const Function<void(shared_ptr<vector<MedusaError::Type> >)>
      update_done_cb = func_disabler_.Bind(&TentativeUpdateCompleted, _1,
                                           &notification);
    cluster_mgr_->medusa()->UpdateExtentGroupIdMap(
      make_shared<vector<int64>>(move(egid_vec)),
      true /* use_CAS */,
      make_shared<vector<Medusa::MedusaValue::Ptr>>(move(mvalue_vec)),
      update_done_cb);
    notification.Wait();

    LOG(INFO) << "Deleted disk " << disk_id
              << " from medusa metadata of egroup " << egroup_id;
  } else {
    LOG(INFO) << "Extent group id map entry not found for egroup "
              << egroup_id;
    arg->add_disk_usage();
  }

  notification.Reset();
  const Function<void(StargateError::Type,
                      shared_ptr<string>,
                      shared_ptr<DeleteExtentGroupsRet>)> delete_done_cb =
    func_disabler_.Bind(&DeleteEgroupDone, _1, _2, _3, expected_error,
                        (user_notification ? user_notification :
                                             &notification));
  iface->DeleteExtentGroups(arg, &delete_done_cb);
  if (user_notification) {
    LOG(INFO) << "Issued RPC to delete extent group " << egroup_id
              << " from disk " << disk_id
              << " with latest intent sequence "
              << egroup_id_intent_seq_map_[egroup_id] + 1;
  } else {
    notification.Wait();
    LOG(INFO) << "Deleted extent group " << egroup_id
              << " from disk " << disk_id
              << " with latest intent sequence "
              << egroup_id_intent_seq_map_[egroup_id] + 1;
  }
}

void EgroupOpsTester::DeleteExtentGroups(
  const vector<int64>& extent_group_id_vec,
  const int64 disk_id, const vector<bool>& managed_by_aes_vec) {

  CHECK_EQ(extent_group_id_vec.size(), managed_by_aes_vec.size());

  // Get stargate interface associated with the specified disk.
  const StargateInterface::Ptr& iface =
    cluster_mgr_->stargate_mgr()->GetStargateInterfaceForDisk(disk_id);
  CHECK(iface);

  // Save a copy to the configuration as recommended in stargate.h.
  Configuration::PtrConst config = cluster_mgr_->Config();

  // Read the metadata of the extent group from Medusa's extent group id map.
  Notification notification;
  vector<Medusa::MedusaValue::PtrConst> mvalue_vec_ret;
  const Function<void(MedusaError::Type,
                      shared_ptr<vector<Medusa::MedusaValue::PtrConst>>)>
    done_cb =
      func_disabler_.Bind(&LookupExtentGroupIdMapCompleted,
                          _1, _2, &mvalue_vec_ret, &notification);
  notification.Reset();
  cluster_mgr_->medusa()->LookupExtentGroupIdMap(
    make_shared<vector<int64>>(extent_group_id_vec),
    false /* cached_ok */,
    done_cb);
  notification.Wait();

  shared_ptr<DeleteExtentGroupsArg> arg = make_shared<DeleteExtentGroupsArg>();
  arg->set_disk_id(disk_id);
  arg->set_qos_principal_name(string());
  arg->set_qos_priority(StargateQosPriority::kDefault);
  int64 egroup_id = 0;
  vector<int64> update_egid_vec;
  vector<Medusa::MedusaValue::Ptr> mvalue_vec;
  update_egid_vec.reserve(extent_group_id_vec.size());
  mvalue_vec.reserve(extent_group_id_vec.size());
  for (size_t ii = 0; ii < extent_group_id_vec.size(); ++ii) {
    egroup_id = extent_group_id_vec[ii];
    LOG(INFO) << "Deleting extent group " << egroup_id
              << " from disk " << disk_id
              << " with latest intent sequence "
              << egroup_id_intent_seq_map_[egroup_id] + 1
              << " managed_by_aes " << boolalpha << managed_by_aes_vec[ii];
    arg->add_extent_group_id(egroup_id);
    arg->add_intent_sequence(egroup_id_intent_seq_map_[egroup_id] + 1);
    arg->add_managed_by_aes(managed_by_aes_vec[ii]);

    Medusa::MedusaValue::PtrConst mvalue = move(mvalue_vec_ret[ii]);
    CHECK(mvalue);
    if (mvalue->timestamp < 0) {
      LOG(INFO) << "Skipping Medusa update for extent group " << egroup_id
                << " as it is not present in Medusa";
      arg->add_disk_usage();
      continue;
    }

    CHECK(mvalue->extent_groupid_map_entry);
    const MedusaExtentGroupIdMapEntry *egroup_metadata =
      mvalue->extent_groupid_map_entry.get();

    StargateUtil::FetchExtentGroupDiskUsage(
        arg->add_disk_usage(), egroup_id, egroup_metadata, config);

    // Remove disk_id from the egroup metadata in medusa.
    Medusa::MedusaValue::Ptr new_mvalue = make_shared<Medusa::MedusaValue>();
    new_mvalue->epoch = mvalue->epoch;
    new_mvalue->timestamp = mvalue->timestamp + 1;

    unique_ptr<MedusaExtentGroupIdMapEntryProto::ControlBlock>
      new_control_block =
      make_unique<MedusaExtentGroupIdMapEntryProto::ControlBlock>();
    new_control_block->CopyFrom(*egroup_metadata->control_block());
    new_control_block->clear_replicas();

    for (int jj = 0; jj < egroup_metadata->control_block()->replicas_size();
         ++jj) {
      if (egroup_metadata->control_block()->replicas(jj).disk_id() ==
          disk_id) {
        continue;
      }

      new_control_block->add_replicas()->CopyFrom(
        egroup_metadata->control_block()->replicas(jj));
    }
    new_mvalue->extent_groupid_map_entry =
      make_shared<MedusaExtentGroupIdMapEntry>(
        move(new_control_block),
        nullptr /* diff_slice_map */, nullptr /* diff_extent_map */);

    update_egid_vec.emplace_back(egroup_id);
    mvalue_vec.emplace_back(move(new_mvalue));
  }

  // Schedule new_mvalues to be written to Medusa.
  notification.Reset();
  const Function<void(shared_ptr<vector<MedusaError::Type> >)>
    update_done_cb = func_disabler_.Bind(&TentativeUpdateCompleted, _1,
                                         &notification);
  cluster_mgr_->medusa()->UpdateExtentGroupIdMap(
    make_shared<vector<int64>>(move(update_egid_vec)),
    true /* use_CAS */,
    make_shared<vector<Medusa::MedusaValue::Ptr>>(move(mvalue_vec)),
    update_done_cb);
  notification.Wait();

  LOG(INFO) << "Deleted disk " << disk_id
            << " from medusa metadata of egroups";

  notification.Reset();
  const Function<void(StargateError::Type,
                      shared_ptr<string>,
                      shared_ptr<DeleteExtentGroupsRet>)> delete_done_cb =
    func_disabler_.Bind(&DeleteEgroupDone, _1, _2, _3, StargateError::kNoError,
                        &notification);
  iface->DeleteExtentGroups(arg, &delete_done_cb);
  notification.Wait();
  for (size_t ii = 0; ii < extent_group_id_vec.size(); ++ii) {
    LOG(INFO) << "Deleted extent groups " << extent_group_id_vec[ii]
              << " from disk " << disk_id << " with latest intent sequence "
              << egroup_id_intent_seq_map_[egroup_id] + 1
              << " managed_by_aes " << boolalpha << managed_by_aes_vec[ii];
  }
}

static void GetEgroupStateDone(
  const StargateError::Type err,
  const shared_ptr<string>& err_detail,
  const shared_ptr<GetEgroupStateRet>& ret,
  GetEgroupStateRet *const egroup_state_ret,
  StargateError::Type *const error_ret,
  Notification *const notification) {

  if (error_ret != nullptr) {
    *error_ret = err;
  } else {
    CHECK_EQ(err, StargateError::kNoError);
  }
  egroup_state_ret->CopyFrom(*ret);
  notification->Notify();
}

void EgroupOpsTester::GetEgroupState(
  const int64 egroup_id,
  const int64 disk_id,
  const int64 latest_intent_sequence,
  GetEgroupStateRet *const egroup_state_ret,
  const bool managed_by_aes,
  const bool extent_based_format,
  StargateError::Type *const error_ret,
  const bool set_latest_applied_intent_sequence,
  const bool fetch_all_metadata,
  const int32 rpc_timeout_msecs,
  StargateInterface::Ptr iface) {

  // Get stargate interface associated with the specified disk.
  if (!iface) {
    iface =
      cluster_mgr_->stargate_mgr()->GetStargateInterfaceForDisk(disk_id);
  }
  CHECK(iface);

  LOG(INFO) << "Fetching state of extent group " << egroup_id
            << " from disk " << disk_id
            << " with latest intent sequence "
            << latest_intent_sequence;
  shared_ptr<GetEgroupStateArg> arg = make_shared<GetEgroupStateArg>();
  arg->set_extent_group_id(egroup_id);
  arg->set_disk_id(disk_id);
  arg->set_intent_sequence(latest_intent_sequence);
  if (!managed_by_aes && set_latest_applied_intent_sequence) {
    arg->set_latest_applied_intent_sequence(
      egroup_id_intent_seq_map_[egroup_id]);
  }
  arg->set_extent_based_metadata_format(extent_based_format);
  arg->set_qos_principal_name(string());
  arg->set_qos_priority(StargateQosPriority::kDefault);
  arg->set_managed_by_aes(managed_by_aes);
  arg->set_fetch_all_metadata(fetch_all_metadata);

  // Save a copy to the configuration as recommended in stargate.h.
  Configuration::PtrConst config = cluster_mgr_->Config();

  Notification notification;
  const Function<void(StargateError::Type,
                      shared_ptr<string>,
                      shared_ptr<GetEgroupStateRet>)> done_cb =
    func_disabler_.Bind(&GetEgroupStateDone, _1, _2, _3, egroup_state_ret,
                        error_ret, &notification);
  iface->GetEgroupState(arg, done_cb, rpc_timeout_msecs);
  notification.Wait();
  LOG(INFO) << "Fetched state of extent group " << egroup_id
            << " from disk " << disk_id;
}

// Callback for SyncEgroupSlices RPC.
static void SyncEgroupSlicesDone(StargateError::Type err,
                                 shared_ptr<string> err_detail,
                                 shared_ptr<SyncEgroupSlicesRet> ret,
                                 SyncEgroupSlicesRet *mutable_ret,
                                 Notification *notify) {
  CHECK_EQ(err, StargateError::kNoError);
  CHECK(mutable_ret);
  *mutable_ret = *ret;
  if (notify) {
    notify->Notify();
  }
}

void EgroupOpsTester::SyncEgroupSlicesAndVerify(
  int64 egroup_id,
  int64 disk_id,
  const vector<shared_ptr<EgroupOpsTester::SliceState>>& slice_state_vec,
  const string& transformation_type,
  const vector<DataTransformation::Type>& transformation_type_vec,
  const bool mark_not_corrupt) {

  // Get stargate interface associated with the destination disk.
  const StargateInterface::Ptr& iface =
    cluster_mgr_->stargate_mgr()->GetStargateInterfaceForDisk(disk_id);
  CHECK(iface);

  shared_ptr<SyncEgroupSlicesArg> arg =
    make_shared<SyncEgroupSlicesArg>();
  arg->set_extent_group_id(egroup_id);
  arg->set_qos_principal_name(string());
  arg->set_qos_priority(StargateQosPriority::kDefault);
  arg->set_disk_id(disk_id);
  arg->set_managed_by_aes(false);
  if (mark_not_corrupt) {
    arg->set_mark_not_corrupt(true);
    arg->set_update_latest_applied_intent_sequence(true);
  }
  unordered_map<int64, int64>::iterator iter =
    egroup_id_intent_seq_map_.find(egroup_id);
  CHECK(iter != egroup_id_intent_seq_map_.end());
  arg->set_intent_sequence(iter->second);
  if (!transformation_type.empty()) {
    arg->set_transformation_type(transformation_type);
  } else {
    for (const DataTransformation::Type transform_type :
          transformation_type_vec) {
      arg->add_transformation_type_list(transform_type);
    }
  }
  for (uint ii = 0; ii < slice_state_vec.size(); ++ii) {
    slice_state_vec[ii]->CopyTo(arg->add_slices());
  }
  Notification notification;
  SyncEgroupSlicesRet ret;
  const Function<void(StargateError::Type, shared_ptr<string>,
                        shared_ptr<SyncEgroupSlicesRet>)>& done_cb =
    func_disabler_.Bind(&SyncEgroupSlicesDone,
                        _1, _2, _3, &ret, &notification);
  notification.Reset();
  iface->SyncEgroupSlices(arg, done_cb);
  notification.Wait();

  // Verify checksum values.
  CHECK_EQ(slice_state_vec.size(), ret.slices().size());
  map<int64, vector<int64>> slice_id_checksum_map;
  for (uint ii = 0; ii < slice_state_vec.size(); ++ii) {
    vector<int64> cksums;
    for (int jj = 0; jj < slice_state_vec[ii]->checksum_size(); ++jj) {
      cksums.push_back(slice_state_vec[ii]->checksum(jj));
    }
    slice_id_checksum_map[slice_state_vec[ii]->slice_id()] = cksums;
  }
  for (int ii = 0; ii < ret.slices().size(); ++ii) {
    for (int jj = 0; jj < ret.slices(ii).checksum_size(); ++jj) {
      const vector<int64>& cksum =
        slice_id_checksum_map[ret.slices(ii).slice_id()];
      CHECK_EQ(cksum[jj], ret.slices(ii).checksum(jj));
    }
  }
}
//-----------------------------------------------------------------------------

void EgroupOpsTester::VerifyMigrateTentativeUpdate(
  const int64 eg_id, const int64 source_disk_id, const int64 target_disk_id) {

  Medusa::MedusaValue::PtrConst mval =
    medusa_connector_->LookupExtentGroupIdMapEntry(eg_id);
  CHECK(mval);
  CHECK(mval->extent_groupid_map_entry);
  const MedusaExtentGroupIdMapEntry& egroup_metadata =
    *mval->extent_groupid_map_entry;
  const MedusaExtentGroupIdMapEntryProto::ControlBlock *const
    control_block = egroup_metadata.control_block();

  LOG(INFO) << "Verify migrate tentative update of egroup " << eg_id
            << " control block: " << control_block->ShortDebugString();

  bool found = false;
  for (int ii = 0; ii < control_block->replicas_size(); ++ii) {
    const MedusaExtentGroupIdMapEntryProto::Replica& replica =
      control_block->replicas(ii);
    const int64 disk_id = replica.disk_id();
    if (disk_id == target_disk_id) {
      CHECK_GT(replica.tentative_updates_size(), 0);
      CHECK(replica.tentative_updates(0).has_replication_source_disk_id());
      CHECK_EQ(source_disk_id,
               replica.tentative_updates(0).replication_source_disk_id());
      CHECK_EQ(disk_id, target_disk_id);
      found = true;
      break;
    } else {
      CHECK_EQ(replica.tentative_updates_size(), 0);
    }
  }
  CHECK(found);
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::MakeParityExtentGroupTentativeUpdate(
  const int64 egroup_id,
  const int64 disk_id,
  const bool is_primary_parity,
  const vector<int64>& info_egroup_ids,
  const vector<int64>& parity_egroup_ids) {

  Medusa::MedusaValue::Ptr new_mvalue = make_shared<Medusa::MedusaValue>();
  unique_ptr<MedusaExtentGroupIdMapEntryProto::ControlBlock>
    new_control_block =
    make_unique<MedusaExtentGroupIdMapEntryProto::ControlBlock>();
  new_mvalue->epoch = WallTime::NowUsecs();
  new_mvalue->timestamp = 0;

  // Set the owner vdisk id.
  new_control_block->set_owner_vdisk_id(vdisk_id_);
  new_control_block->set_owner_container_id(container_id_);
  new_control_block->set_latest_intent_sequence(0);
  new_control_block->set_write_time_usecs(new_mvalue->epoch);

  // Add a replica to the control block.
  MedusaExtentGroupIdMapEntryProto::Replica *replica =
    new_control_block->add_replicas();
  replica->set_disk_id(disk_id);

  // Add the info egroups if this is a primary parity extent group.
  if (is_primary_parity) {
    CHECK_EQ(parity_egroup_ids.at(0), egroup_id);
    for (uint ii = 0; ii < info_egroup_ids.size(); ++ii) {
      new_control_block->add_erasure_code_info_egroup_ids(
        info_egroup_ids.at(ii));
    }
  }

  // Add parity egroup ids to the control block.
  for (uint ii = 0; ii < parity_egroup_ids.size(); ++ii) {
    new_control_block->add_erasure_code_parity_egroup_ids(
      parity_egroup_ids.at(ii));
  }


  LogicalOperationClock *new_loc = new_control_block->mutable_locs()->Add();
  // Dummy ids are good enough for the unit test.
  new_loc->set_component_id(1);
  new_loc->set_incarnation_id(2);
  new_loc->set_operation_id(3);

  // Put in the tentative update component id and incarnation id.
  new_control_block->set_tentative_update_component_id(1);
  new_control_block->set_tentative_update_incarnation_id(1);

  StargateUtil::SetEgroupProperty(
    new_control_block.get(),
    MedusaExtentGroupIdMapEntryProto::ControlBlock::kManagedByAes, true);

  new_mvalue->extent_groupid_map_entry =
    make_shared<MedusaExtentGroupIdMapEntry>(
      move(new_control_block),
      shared_ptr<MedusaExtentGroupIdMapEntry::SliceMap>(),
      shared_ptr<MedusaExtentGroupIdMapEntry::ExtentMap>());

  // Schedule new_mvalue to be written to Medusa.
  vector<int64> egid_vec;
  egid_vec.push_back(egroup_id);
  vector<Medusa::MedusaValue::Ptr> mvalue_vec;
  mvalue_vec.push_back(new_mvalue);

  Notification notification;
  const Function<void(shared_ptr<vector<MedusaError::Type> >)> done_cb =
    func_disabler_.Bind(&TentativeUpdateCompleted, _1, &notification);

  notification.Reset();
  cluster_mgr_->medusa()->UpdateExtentGroupIdMap(
    make_shared<vector<int64>>(egid_vec),
    true /* use_CAS */,
    make_shared<vector<Medusa::MedusaValue::Ptr>>(mvalue_vec),
    done_cb);
  notification.Wait();
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::MakeTentativeUpdate(
  int64 egroup_id,
  int64 primary_disk_id,
  int64 secondary_disk_id,
  const string& transformation_type,
  const vector<DataTransformation::Type>& transformation_type_vec,
  ExtentIdProto *const eid_proto,
  const bool managed_by_aes) {

  Medusa::MedusaValue::Ptr new_mvalue = make_shared<Medusa::MedusaValue>();
  unique_ptr<MedusaExtentGroupIdMapEntryProto::ControlBlock>
    new_control_block =
    make_unique<MedusaExtentGroupIdMapEntryProto::ControlBlock>();
  new_mvalue->epoch = WallTime::NowUsecs();
  new_mvalue->timestamp = 0;

  // Set the owner vdisk id.
  new_control_block->set_owner_vdisk_id(vdisk_id_);
  new_control_block->set_owner_container_id(container_id_);
  new_control_block->set_write_time_usecs(new_mvalue->epoch);
  new_control_block->set_latest_intent_sequence(0);
  if (!transformation_type.empty()) {
    new_control_block->set_transformation_type(transformation_type);
  }
  for (const DataTransformation::Type transformation_type :
         transformation_type_vec) {
    new_control_block->add_transformation_type_list(transformation_type);
  }

  if (managed_by_aes) {
    StargateUtil::SetEgroupProperty(
      new_control_block.get(),
      MedusaExtentGroupIdMapEntryProto::ControlBlock::kManagedByAes, true);
  }

  // Set information about each replica.
  // The disk ids corresponding to our replicas.
  int num_replicas = 2;
  if (secondary_disk_id < 0) {
    num_replicas = 1;
  }
  for (int xx = 0; xx < num_replicas; ++xx) {
    // Add a replica to the control block.
    MedusaExtentGroupIdMapEntryProto::Replica *replica =
      new_control_block->add_replicas();
    replica->set_disk_id((xx == 0) ? primary_disk_id : secondary_disk_id);
    if (!managed_by_aes) {
      MedusaExtentGroupIdMapEntryProto::TentativeUpdate *tu =
          replica->add_tentative_updates();

      // Set the intent sequence. Hardcoded to 0, since this function only gets
      // called for the first write op.
      tu->set_intent_sequence(0);

      // Add all the extents we're writing to this tentative update.
      ExtentIdProto *extent_id_proto = tu->add_extent_id();
      extent_id_proto->CopyFrom(*eid_proto);
      tu->add_refcount(1);
    }
  }

  LogicalOperationClock *new_loc = new_control_block->mutable_locs()->Add();
  // Dummy ids are good enough for the unit test.
  new_loc->set_component_id(1);
  new_loc->set_incarnation_id(2);
  new_loc->set_operation_id(3);

  // Put in the tentative update component id and incarnation id.
  if (!managed_by_aes) {
    new_control_block->set_tentative_update_component_id(1);
    new_control_block->set_tentative_update_incarnation_id(1);
  }
  // The extent map that provides the delta between the extents in old_mvalue
  // and new_mvalue. Since we're making only tentative changes right now, this
  // would be empty.
  shared_ptr<MedusaExtentGroupIdMapEntry::ExtentMap> diff_extent_map =
    make_shared<MedusaExtentGroupIdMapEntry::ExtentMap>();

  // The slice information remains unchanged. So create a NULL slice map.
  shared_ptr<MedusaExtentGroupIdMapEntry::SliceMap> diff_slice_map;

  // Create the new metadata.
  new_mvalue->extent_groupid_map_entry =
    make_shared<MedusaExtentGroupIdMapEntry>(
      move(new_control_block), diff_slice_map, diff_extent_map);

  // Schedule new_mvalue to be written to Medusa.
  vector<int64> egid_vec;
  egid_vec.push_back(egroup_id);
  vector<Medusa::MedusaValue::Ptr> mvalue_vec;
  mvalue_vec.push_back(new_mvalue);

  Notification notification;
  const Function<void(shared_ptr<vector<MedusaError::Type> >)> done_cb =
    func_disabler_.Bind(&TentativeUpdateCompleted, _1, &notification);

  notification.Reset();
  cluster_mgr_->medusa()->UpdateExtentGroupIdMap(
    make_shared<vector<int64>>(egid_vec),
    true /* use_CAS */,
    make_shared<vector<Medusa::MedusaValue::Ptr>>(mvalue_vec),
    done_cb);
  notification.Wait();
}

static void FetchEgroupFallocateLengthHelper(
  const FetchEgroupsRet *const ret,
  unordered_map<int64, int32> *egid_2_falloc_len_map) {
  LOG(INFO) << "FetchEgroupFallocateLengthHelper invoked with "
            << ret->extent_group_id_size() << " egroups";

  for (int ii = 0; ii < ret->extent_group_id_size(); ++ii) {
    egid_2_falloc_len_map->emplace(
      ret->extent_group_id(ii),
      ret->fallocate_size_bytes(ii));
  }
}

void EgroupOpsTester::FetchEgroupFallocateLength(
  const int64 disk_id,
  unordered_map<int64, int32> *egid_2_falloc_len_map) {

  FetchAllEgroupsCb user_notification_cb =
    bind(FetchEgroupFallocateLengthHelper, _1, egid_2_falloc_len_map);
  FetchAllEgroups(disk_id, &user_notification_cb);
}

MedusaValue::Ptr EgroupOpsTester::LookupExtentGroupSummaryView(
  const int64 disk_id, const int64 egroup_id) {

  MedusaValue::Ptr mvalue;

  FetchAllEgroupSVCb cb =
    [&mvalue, egroup_id](
      const FetchEgroupSummaryViewRet *const ret,
      const vector<MedusaValue::Ptr>& mvalue_vec) {

    for (int ii = 0; ii < ret->extent_group_id_size(); ++ii) {
      if (ret->extent_group_id(ii) == egroup_id) {
        LOG(INFO) << "Found egroup " << egroup_id << " in FetchEgroupSVRet";
        CHECK(!mvalue) << egroup_id;
        mvalue = mvalue_vec[ii];
        break;
      }
    }
  };

  FetchAllEgroupSummaryView(disk_id, &cb);
  if (!mvalue) {
    LOG(INFO) << "Egroup " << egroup_id << " not found on disk " << disk_id;
    return Medusa::NegativeCacheEntry();
  }

  return mvalue;
}

MedusaValue::Ptr EgroupOpsTester::LookupExtentGroupPhysicalState(
  const int64 disk_id, const int64 egroup_id) {

  MedusaValue::Ptr mvalue;

  FetchAllEgroupsCb cb =
    [&mvalue, egroup_id](
      const FetchEgroupsRet *const ret,
      const vector<MedusaValue::Ptr>& mvalue_vec) {

    if (ret->source() != FetchEgroupsRet::kMedusa) {
      return;
    }

    for (int ii = 0; ii < ret->extent_group_id_size(); ++ii) {
      if (ret->extent_group_id(ii) == egroup_id) {
        LOG(INFO) << "Found egroup " << egroup_id << " in FetchEgroupsRet";
        CHECK(!mvalue) << egroup_id;
        mvalue = mvalue_vec[ii];
        break;
      }
    }
  };

  FetchAllEgroups(disk_id, &cb);
  if (!mvalue) {
    LOG(INFO) << "Egroup " << egroup_id << " not found on disk " << disk_id;
    return Medusa::NegativeCacheEntry();
  }

  return mvalue;
}

static void FetchEgroupSVDone(
  const int64 disk_id,
  Notification *notification,
  StargateError::Type error,
  const shared_ptr<string>& error_detail,
  const shared_ptr<FetchEgroupSummaryViewRet>& ret,
  const IOBuffer::Ptr& iobuf,
  string *cookie,
  EgroupOpsTester::FetchAllEgroupSVCb *const user_notification_cb) {

  CHECK_EQ(error, StargateError::kNoError);
  CHECK((ret->extent_group_id_size() ==
          ret->summary_view_payload_length_size()) ||
        (ret->extent_group_id_size() ==
          ret->summary_view_ext_payload_length_size()));
  LOG(INFO) << "Received FetchEgroupSV response from disk " << disk_id;

  vector<MedusaValue::Ptr> mvalue_vec;
  mvalue_vec.resize(ret->extent_group_id_size(), Medusa::NegativeCacheEntry());

  IOBuffer::Accessor ac(iobuf.get());
  CharArray::PtrConst egroup_sv = nullptr;
  CharArray::PtrConst egroup_sv_ext = nullptr;

  for (int ii = 0; ii < ret->extent_group_id_size(); ++ii) {
    int64 egroup_id = ret->extent_group_id(ii);
    MedusaValue::Ptr mvalue = make_shared<MedusaValue>();
    if (ii < ret->summary_view_payload_length_size()) {
      int sv_payload_size = ret->summary_view_payload_length(ii);
      egroup_sv = ac.CopyToCharArray(sv_payload_size);
      ac.Advance(sv_payload_size);
    }
    if (ii < ret->summary_view_ext_payload_length_size()) {
      int sv_ext_payload_size = ret->summary_view_ext_payload_length(ii);
      egroup_sv_ext = ac.CopyToCharArray(sv_ext_payload_size);
      ac.Advance(sv_ext_payload_size);
    }

    mvalue->timestamp = 0;
    mvalue->extent_group_summary_view_entry =
      make_shared<MedusaExtentGroupSummaryViewEntry>(
        egroup_id,
        egroup_sv->size() ? egroup_sv : nullptr,
        egroup_sv_ext->size() ? egroup_sv_ext : nullptr);
    mvalue_vec[ii] = move(mvalue);
  }

  if (user_notification_cb && *user_notification_cb) {
    (*user_notification_cb)(ret.get(), move(mvalue_vec));
  }

  *cookie = ret->cookie();
  notification->Notify();
}

void EgroupOpsTester::FetchEgroupDone(
  const int64 disk_id,
  Notification *notification,
  StargateError::Type error,
  const shared_ptr<string>& error_detail,
  const shared_ptr<FetchEgroupsRet>& ret,
  const IOBuffer::Ptr& iobuf,
  string *cookie,
  EgroupOpsTester::FetchAllEgroupsCb *const user_notification_cb) {

  CHECK_EQ(error, StargateError::kNoError);
  LOG(INFO) << "Received FetchEgroups response from disk " << disk_id;

  vector<MedusaValue::Ptr> mvalue_vec;
  mvalue_vec.resize(ret->extent_group_id_size(), Medusa::NegativeCacheEntry());

  if (ret->extent_group_id_size() == 0) {
    CHECK(!iobuf) << ret->ShortDebugString();
  } else if (ret->source() == FetchEgroupsRet::kMemory) {
    CHECK_EQ(ret->physical_state_payload_descriptor_size(), 0)
      << ret->ShortDebugString();
    CHECK(!iobuf) << ret->ShortDebugString();
  } else {
    CHECK_EQ(ret->extent_group_id_size(),
             ret->physical_state_payload_descriptor_size())
      << ret->ShortDebugString();
    CHECK_EQ(ret->extent_group_metadata_vec_size(), 0)
      << ret->ShortDebugString();
    CHECK(iobuf) << ret->ShortDebugString();

    int iobuf_offset = 0;
    for (int ii = 0; ii < ret->extent_group_id_size(); ++ii) {
      const int64 extent_group_id = ret->extent_group_id(ii);
      CHECK_GE(extent_group_id, 0) << ret->ShortDebugString();
      MedusaExtentGroupIdMapEntryProto *const entry_proto =
        ret->add_extent_group_metadata_vec();

      const MedusaExtentGroupPhysicalStatePayloadDescriptor& desc =
        ret->physical_state_payload_descriptor(ii);

      // Create the serialized buffer using the descriptor and associated
      // flatbuffers. We can then construct the mvalue by parsing this entry.
      // The format used here is based on
      // MedusaExtentGroupPhysicalStateEntry::Serialize().
      // MedusaExtentGroupPhysicalStateEntry can be updated to include a
      // constructor that takes the descriptor as input to avoid this
      // workaround.
      IOBuffer egroup_buf;
      egroup_buf.Append("00000000", 8);
      IOBufferOutputStream ostream(&egroup_buf);
      desc.SerializeToZeroCopyStream(&ostream);
      const int descriptor_size = htonl(egroup_buf.size() - 8);
      egroup_buf.Replace(
        4, reinterpret_cast<const char *>(&descriptor_size), 4);
      int total_payload_size = desc.control_block_payload_length();
      for (const auto& sdesc : desc.slice_group_list()) {
        total_payload_size += sdesc.payload_length();
      }
      for (const auto& edesc : desc.extent_id_list()) {
        total_payload_size += edesc.payload_length();
      }
      egroup_buf.AppendIOBuffer(
        iobuf->Clone(iobuf_offset, total_payload_size));
      iobuf_offset += total_payload_size;
      const int buf_size = htonl(egroup_buf.size());
      egroup_buf.Replace(0, reinterpret_cast<const char *>(&buf_size), 4);

      MedusaValue::Ptr mvalue = make_shared<MedusaValue>();
      mvalue->timestamp = 0;
      mvalue->extent_group_physical_state_entry =
        make_shared<MedusaExtentGroupPhysicalStateEntry>(
          extent_group_id, egroup_buf);
      mvalue->extent_group_physical_state_entry->FillExtentGroupIdMapEntry(
        disk_id, entry_proto);
      mvalue_vec[ii] = move(mvalue);

      if (entry_proto->has_control_block()) {
        const auto& cb = entry_proto->control_block();
        CHECK_EQ(cb.locs_size(), 1U) << entry_proto->ShortDebugString();
        CHECK(cb.locs(0).has_operation_timestamp())
          << entry_proto->ShortDebugString();
      }
    }
  }

  if (user_notification_cb && *user_notification_cb) {
    (*user_notification_cb)(ret.get(), move(mvalue_vec));
  }

  *cookie = ret->cookie();
  notification->Notify();
}

void EgroupOpsTester::FetchAllEgroupSummaryView(
  const int64 disk_id,
  FetchAllEgroupSVCb *const user_notification_cb) {

  const StargateInterface::Ptr& iface =
    cluster_mgr_->stargate_mgr()->GetStargateInterfaceForDisk(disk_id);
  CHECK(iface) << disk_id;

  Notification notification;
  string cookie;
  LOG(INFO) << "Fetching egroups summary view from disk " << disk_id;
  while (1) {
    shared_ptr<FetchEgroupSummaryViewArg> arg =
      make_shared<FetchEgroupSummaryViewArg>();
    arg->set_disk_id(disk_id);

    if (!cookie.empty()) {
      arg->set_cookie(cookie);
    }

    LOG(INFO) << "Sending FetchEgroup SV RPC with cookie: " << cookie;

    Function<void(StargateError::Type, shared_ptr<string>,
                  shared_ptr<FetchEgroupSummaryViewRet>,
                  IOBuffer::Ptr&&)> done_cb =
      func_disabler_.Bind(&FetchEgroupSVDone, disk_id, &notification,
                          _1, _2, _3, _4, &cookie, user_notification_cb);
    iface->FetchEgroupSummaryView(arg, move(done_cb));
    notification.Wait();
    notification.Reset();

    if (cookie.empty()) {
      break;
    }
  }
  LOG(INFO) << "Done fetching egroup's summary view from disk " << disk_id;
}

void EgroupOpsTester::FetchAllEgroups(
  const int64 disk_id,
  FetchAllEgroupsCb *const user_notification_cb,
  const bool memory_state_only,
  const bool flush_aes_egroup_state) {

  const StargateInterface::Ptr& iface =
    cluster_mgr_->stargate_mgr()->GetStargateInterfaceForDisk(disk_id);
  CHECK(iface) << disk_id;

  Notification notification;
  string cookie;
  LOG(INFO) << "Fetching egroups from disk " << disk_id;
  while (1) {
    shared_ptr<FetchEgroupsArg> arg = make_shared<FetchEgroupsArg>();
    arg->set_disk_id(disk_id);
    arg->set_qos_principal_name(string());
    arg->set_memory_state_only(memory_state_only);
    arg->set_flush_aes_egroup_state(flush_aes_egroup_state);

    if (!cookie.empty()) {
      arg->set_cookie(cookie);
    }

    LOG(INFO) << "Sending FetchEgroups RPC with cookie: " << cookie;

    Function<void(StargateError::Type, shared_ptr<string>,
                  shared_ptr<FetchEgroupsRet>,
                  IOBuffer::Ptr&&)> done_cb =
      func_disabler_.Bind(&EgroupOpsTester::FetchEgroupDone, this, disk_id,
                          &notification, _1, _2, _3, _4, &cookie,
                          user_notification_cb);
    iface->FetchEgroups(arg, move(done_cb));
    notification.Wait();
    notification.Reset();

    if (cookie.empty()) {
      break;
    }
  }
  LOG(INFO) << "Done fetching egroups from disk " << disk_id;
}

string EgroupOpsTester::GetStargateGFlag(
  const string& gflag, const int64 stargate_idx) {

  int64 svm_id = -1;
  if (stargate_idx > -1) {
    svm_id = cluster_mgr_->stargate_mgr()->GetSvmIdForStargate(stargate_idx);
  }
  return cluster_mgr_->stargate_mgr()->GetGFlag(gflag, svm_id);
}

string EgroupOpsTester::GetStargateGflagStripped(const string& gflag,
                                                 const int64 stargate_idx) {
  string val = GetStargateGFlag(gflag, stargate_idx);
  return val.substr(0, val.find(" "));
}

void EgroupOpsTester::SetStargateGFlag(
  const string& gflag, const string& val, const int64 stargate_idx) {

  int64 svm_id = -1;
  if (stargate_idx > -1) {
    svm_id = cluster_mgr_->stargate_mgr()->GetSvmIdForStargate(stargate_idx);
  }
  cluster_mgr_->stargate_mgr()->SetGFlag(gflag, val, svm_id);
}

void EgroupOpsTester::RestartStargateAtIndex(
  const int32 index,
  const string additional_gflags,
  const bool wait_for_stargate_restart) {

  vector<int64> svm_ids = cluster_mgr_->stargate_mgr()->svm_ids();
  CHECK_GE(index, 0);
  CHECK_LT(index, svm_ids.size());

  const int64 svm_id = svm_ids[index];
  LOG(INFO) << "Stopping stargate at index " << index;
  cluster_mgr_->stargate_mgr()->StopStargateWithSvmId(svm_id);
  sleep(5);

  vector<string> cmdlines = cluster_mgr_->stargate_mgr()->get_cmdlines();
  for (uint32 ii = 0; ii < cmdlines.size(); ++ii) {
    cmdlines[ii] = StringJoin(cmdlines[ii], additional_gflags);
    DVLOG(5) << "Stargate cmd line: " << cmdlines[ii];
  }
  cluster_mgr_->stargate_mgr()->set_cmdlines(cmdlines);

  LOG(INFO) << "Starting stargate at index " << index;
  cluster_mgr_->stargate_mgr()->StartStargateWithSvmId(svm_id);

  if (!wait_for_stargate_restart) {
    return;
  }

  StargateError::Type error_ret;
  bool stargate_started = false;
  // We wait for maximum 30s for Stargate to start.
  for (int ii = 0; ii < 6; ++ii) {
    sleep(5);
    // Send a dummy GetEgroupState RPC to Stargate to probe if it started or
    // not. Since we are sending the GetEgroupState request with invalid disk
    // id, it will respond with kDiskNonExistent once Stargate initialization
    // is complete.
    interface::GetEgroupStateRet egroup_state;
    GetEgroupState(
      0 /* egroup_id */,
      -1 /* disk_id */,
      -1 /* latest_intent_sequence */,
      &egroup_state,
      false /* managed_by_aes */,
      false /* extent_based_format */,
      &error_ret,
      true /* latest_intent_sequence */,
      false /* fetch_all_metadata */,
      -1 /* rpc_timeout_msecs */,
      cluster_mgr_->stargate_mgr()->GetStargateInterfaceForSvm(svm_id));
    LOG(INFO) << "Stargate at index " << index
              << " responded to GetEgroupState request with: " << error_ret;
    if (error_ret == StargateError::kDiskNonExistent) {
      stargate_started = true;
      break;
    }
  }
  LOG_IF(FATAL, !stargate_started)
    << "Stargate at index " << index << " failed to start";
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::SetCmdlineArgAndGflag(
  const string& gflag, const string& old_value, const string& new_value) {

  vector<string> current_cmdlines =
    cluster_mgr_->stargate_mgr()->get_cmdlines();

  for (string& cmdline : current_cmdlines) {
    ReplaceAll(&cmdline,
               StringJoin(gflag, "=", old_value),
               StringJoin(gflag, "=", new_value));
  }

  cluster_mgr_->stargate_mgr()->set_cmdlines(current_cmdlines);
  cluster_mgr_->stargate_mgr()->SetGFlag(gflag, new_value);
  sleep(2);
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::ReplaceAll(
  string *const subject,
  const string& search,
  const string& replace) {

  CHECK(subject);

  size_t pos = 0;
  while ((pos = subject->find(search, pos)) != string::npos) {
    subject->replace(pos, search.length(), replace);
    pos += replace.length();
  }
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::RestartStargate(const string& cmdline) {

  cluster_mgr_->stargate_mgr()->Stop(true /* use_force */);
  sleep(5);
  vector<string> cmdlines = cluster_mgr_->stargate_mgr()->get_cmdlines();
  for (size_t ii = 0; ii < cmdlines.size(); ++ii) {
    cmdlines[ii] = StringJoin(cmdlines[ii], " ", cmdline);
  }
  cluster_mgr_->stargate_mgr()->set_cmdlines(cmdlines);
  cluster_mgr_->stargate_mgr()->Start();
  sleep(10);
}

//-----------------------------------------------------------------------------

static void CheckpointDiskWALDone(
  const StargateError::Type err,
  const shared_ptr<string>& error_detail,
  const shared_ptr<TestActionRet>& ret,
  const int64 disk_id,
  const Function<void(bool)>& done_cb) {

  bool success = true;
  if (err != StargateError::kNoError) {
    LOG(ERROR) << "Failed to checkpoint disk WAL for disk " << disk_id
               << " error=" << err << " reason: " << error_detail;
    success = false;
  } else {
    LOG(INFO) << "Disk WAL checkpoint for disk " << disk_id
              << " successfully completed";
  }

  done_cb(success);
}

void EgroupOpsTester::CheckpointDiskWAL(
  const int64 disk_id, const Function<void(bool)>& done_cb) {

  const StargateInterface::Ptr& iface =
    cluster_mgr_->stargate_mgr()->GetStargateInterfaceForDisk(disk_id);
  if (!iface) {
    LOG(ERROR) << "Unable to fetch interface for disk " << disk_id;
    done_cb(false);
    return;
  }

  LOG(INFO) << "Checkpoint disk WAL for disk " << disk_id;
  shared_ptr<TestActionArg> test_action_arg = make_shared<TestActionArg>();
  test_action_arg->mutable_checkpoint_disk_wal_arg()->set_disk_id(disk_id);
  Function<void(StargateError::Type,
                shared_ptr<string>,
                shared_ptr<TestActionRet>)> rpc_done_cb =
    func_disabler_.Bind(&CheckpointDiskWALDone,
                        _1, _2, _3, disk_id, done_cb);

  iface->TestAction(move(test_action_arg), move(rpc_done_cb));
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::TestNextSliceAllocationOffsetHintUpdateHelper() {
  // Lets initialize the space usage class so we can fetch the stats that are
  // required to validate.
  SpaceUsage usage(cluster_mgr_);

  // Fetch the container id.
  const ConfigurationProto::Container *ctr =
    zeus_->FetchZeusConfig()->LookupContainer(FLAGS_container);
  const int32 rf = ctr->params().replication_factor();

  // Let's create a NFS file to perform the write.
  CHECK(nfs_connector_->Mount(FLAGS_container));
  const string filename = "test-next-slice-allocation-offset-hint";
  CHECK(nfs_connector_->CreateFile("/", filename));
  const string filepath = StringJoin('/', filename);

  NfsFh::PtrConst nfs_fh = nfs_connector_->ResolvePath(filepath);
  const int128 inode_id = nfs_connector_->Path2Inode(filepath);
  const string vdisk_name = nfs_connector_->Inode2VDiskName(inode_id);

  // Disable oplog. This is to ensure that all writes go to extent store.
  cluster_mgr_->stargate_mgr()->SetGFlag(
    "stargate_vdisk_oplog_enabled", "false");

  // Perform a small write to create the vdisk. We already set the vdisk file
  // threshold to a low value while creating the test, so this write op will
  // create a new extent group on the disk.
  const int32 slice_size = FLAGS_estore_regular_slice_size;
  int32 expected_next_slice_allocation_offset = slice_size;
  LOG(INFO) << "Writing one slice of random data";
  CHECK(nfs_connector_->Write(nfs_fh, 0 /* offset */,
                              Random::CreateRandomDataIOBuffer(slice_size)));

  cluster_mgr_->stargate_mgr()->SetGFlag(
    "stargate_stats_dump_interval_secs", "1");

  // Let us fetch the vdisk id from the vdisk name.
  const int64 vdisk_id = pithos_->GetVDiskId(vdisk_name);
  usage.RegisterVDiskId(vdisk_id);
  Notification notification;
  vector<Medusa::MedusaValue::PtrConst> mvalue_vec_ret;
  CHECK_GE(vdisk_id, 0) << vdisk_name;

  // Perform the lookup.
  const Function<void(MedusaError::Type,
                      shared_ptr<vector<Medusa::MedusaValue::PtrConst>>)>
    vblock_lookup_done_cb =
      func_disabler_.Bind(&LookupVDiskBlockMapCompleted,
                          _1, _2, &mvalue_vec_ret, &notification);

  // For simplicity let's consider the first vblock. The test also ensures
  // that it performs the write on the first offset.
  cluster_mgr_->medusa()->LookupVDiskBlockMap(vdisk_id, 0 /* block_num */,
                                              1 /* num_blocks */,
                                              false /* cached_ok */,
                                              vblock_lookup_done_cb);

  notification.Wait();
  const MedusaVDiskBlockMapEntryInterface *blk_map_entry =
    (mvalue_vec_ret[0]->block_map_entry).get();
  CHECK(blk_map_entry);
  CHECK_GE(blk_map_entry->regions(0).extent_group_id(), 0);
  const int64 egroup_id = blk_map_entry->regions(0).extent_group_id();
  CHECK_GT(egroup_id, 0);

  // Lookup and verify if the hint is updated in medusa.
  LookupAndVerifyNextSliceAllocationOffsetHint(
    egroup_id, expected_next_slice_allocation_offset);
  LOG(INFO) << "Verifying VDiskUsageStats";
  usage.VerifyVDiskUsageStat(
    expected_next_slice_allocation_offset,
    expected_next_slice_allocation_offset * rf);

  // Let us overwrite the first slice in the file.
  IOBuffer::Ptr iobuf = Random::CreateRandomDataIOBuffer(slice_size);
  LOG(INFO) << "Performing overwrite on the first slice";
  CHECK(nfs_connector_->Write(nfs_fh, 0 /* offset */, iobuf));

  // For overwrites, the hint should not be updated.
  LookupAndVerifyNextSliceAllocationOffsetHint(
    egroup_id, expected_next_slice_allocation_offset);
  LOG(INFO) << "Verifying VDiskUsageStats";
  usage.VerifyVDiskUsageStat(
    expected_next_slice_allocation_offset,
    expected_next_slice_allocation_offset * rf);

  // Let us write another slice on the egroup.
  iobuf = Random::CreateRandomDataIOBuffer(slice_size);
  LOG(INFO) << "Writing second slice of random data";
  CHECK(nfs_connector_->Write(nfs_fh, slice_size /* offset */, iobuf));
  expected_next_slice_allocation_offset += slice_size;

  // The hint should be updated with the new value.
  LookupAndVerifyNextSliceAllocationOffsetHint(
    egroup_id, expected_next_slice_allocation_offset);
  LOG(INFO) << "Verifying VDiskUsageStats";
  usage.VerifyVDiskUsageStat(
    expected_next_slice_allocation_offset,
    expected_next_slice_allocation_offset * rf);

  // Perform an overwrite again on the second slice and verify that the hint is
  // not updated.
  LOG(INFO) << "Performing overwrite on the second slice";
  CHECK(nfs_connector_->Write(nfs_fh, slice_size /* offset */, iobuf));
  LookupAndVerifyNextSliceAllocationOffsetHint(
    egroup_id, expected_next_slice_allocation_offset);
  LOG(INFO) << "Verifying VDiskUsageStats";
  usage.VerifyVDiskUsageStat(
    expected_next_slice_allocation_offset,
    expected_next_slice_allocation_offset * rf);

  cluster_mgr_->stargate_mgr()->SetGFlag(
    "stargate_stats_dump_interval_secs", "30");
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::LookupExtentGroupIdMapHelper(
  const int64 egroup_id,
  MedusaExtentGroupIdMapEntry::Ptr *egroup_idmap_entry) {

  // Now fetch the egroup metadata entries for this egroup id.
  Notification notification;
  vector<Medusa::MedusaValue::PtrConst> mvalue_vec_ret;
  const Function<void(MedusaError::Type,
                      shared_ptr<vector<Medusa::MedusaValue::PtrConst>>)>
    egroupidmap_lookup_done_cb =
      func_disabler_.Bind(&LookupExtentGroupIdMapCompleted,
                          _1, _2, &mvalue_vec_ret, &notification);
  shared_ptr<vector<int64>> extent_group_id_vec = make_shared<vector<int64>>();
  extent_group_id_vec->push_back(egroup_id);

  cluster_mgr_->medusa()->LookupExtentGroupIdMap(extent_group_id_vec,
                                                 false /* cached_ok */,
                                                 egroupidmap_lookup_done_cb);
  notification.Wait();
  CHECK_EQ(mvalue_vec_ret.size(), 1);
  Medusa::MedusaValue::PtrConst mvalue = move(mvalue_vec_ret[0]);
  CHECK(mvalue && mvalue->extent_groupid_map_entry);
  unique_ptr<MedusaExtentGroupIdMapEntryProto::ControlBlock>
    new_control_block =
    make_unique<MedusaExtentGroupIdMapEntryProto::ControlBlock>();
  new_control_block->CopyFrom(
    *mvalue->extent_groupid_map_entry->control_block());
  *egroup_idmap_entry =
    make_shared<MedusaExtentGroupIdMapEntry>(
      move(new_control_block),
      nullptr /* slice_map */,
      nullptr /* extent_map */);
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::AddTentativeUpdate(
  const int64 egroup_id,
  const bool managed_by_aes,
  Medusa::MedusaValue::PtrConst old_mvalue,
  ExtentIdProto *const eid_proto) {

  CHECK(eid_proto);
  const medusa::MedusaExtentGroupIdMapEntryProto::ControlBlock
    *old_control_block = nullptr;

  if (!old_mvalue) {
    Medusa::MedusaValue::PtrConst mvalue =
      medusa_connector_->LookupExtentGroupIdMapEntry(egroup_id);
    CHECK(mvalue && mvalue->extent_groupid_map_entry);
    old_mvalue = mvalue;
  }
  CHECK(old_mvalue);
  old_control_block = old_mvalue->extent_groupid_map_entry->control_block();
  CHECK(old_control_block);

  Medusa::MedusaValue::Ptr new_mvalue = make_shared<Medusa::MedusaValue>();
  unique_ptr<MedusaExtentGroupIdMapEntryProto::ControlBlock>
    new_control_block =
    make_unique<MedusaExtentGroupIdMapEntryProto::ControlBlock>();
  new_mvalue->epoch = old_mvalue->epoch;
  new_mvalue->timestamp = old_mvalue->timestamp + 1;

  new_control_block->CopyFrom(*old_control_block);

  LogicalOperationClock *new_loc = new_control_block->mutable_locs()->Add();
  // Dummy ids are good enough for the unit test.
  new_loc->set_component_id(1);
  new_loc->set_incarnation_id(2);
  new_loc->set_operation_id(3);

  if (!managed_by_aes) {
    new_control_block->set_tentative_update_component_id(1);
    new_control_block->set_tentative_update_incarnation_id(1);

    DCHECK_GE(old_control_block->replicas_size(), 1)
      << old_control_block->ShortDebugString();
    const int tu_size =
      old_control_block->replicas(0).tentative_updates_size();
    for (const auto& replica : old_control_block->replicas()) {
      DCHECK_EQ(replica.tentative_updates_size(), tu_size)
        << old_control_block->ShortDebugString();
      DCHECK_EQ(replica.diff_slices_size(), 0)
        << old_control_block->ShortDebugString();
      DCHECK_EQ(replica.diff_extents_size(), 0)
        << old_control_block->ShortDebugString();
    }
    const MedusaExtentGroupIdMapEntryProto::TentativeUpdate *const latest_tu =
      tu_size > 0 ?
      &old_control_block->replicas(0).tentative_updates(tu_size - 1) : nullptr;
    DCHECK(latest_tu == nullptr ||
           !latest_tu->has_replication_source_disk_id())
      << old_control_block->ShortDebugString();
    const int64 intent_sequence =
      latest_tu ? latest_tu->intent_sequence() + 1 :
      old_control_block->latest_intent_sequence() + 1;
    for (int xx = 0; xx < old_control_block->replicas_size(); ++xx) {
      MedusaExtentGroupIdMapEntryProto::Replica *const replica =
        new_control_block->mutable_replicas(xx);
      MedusaExtentGroupIdMapEntryProto::TentativeUpdate *const tu =
        replica->add_tentative_updates();
      tu->set_intent_sequence(intent_sequence);
      ExtentIdProto *const extent_id_proto = tu->add_extent_id();
      extent_id_proto->CopyFrom(*eid_proto);
      tu->add_refcount(1);
    }
  }

  // The extent map that provides the delta between the extents in old_mvalue
  // and new_mvalue. Since we're making only tentative changes right now, this
  // would be empty.
  shared_ptr<MedusaExtentGroupIdMapEntry::ExtentMap> diff_extent_map =
    make_shared<MedusaExtentGroupIdMapEntry::ExtentMap>();

  // The slice information remains unchanged. So create a NULL slice map.
  shared_ptr<MedusaExtentGroupIdMapEntry::SliceMap> diff_slice_map;

  // Create the new metadata.
  new_mvalue->extent_groupid_map_entry =
    make_shared<MedusaExtentGroupIdMapEntry>(
      move(new_control_block), diff_slice_map, diff_extent_map);
  Notification notify;
  Function<void(shared_ptr<vector<MedusaError::Type>> err_vec)> done_cb =
    [&notify](shared_ptr<vector<MedusaError::Type>> err_vec) {
      CHECK_EQ(err_vec->size(), 1);
      LOG(INFO) << "Update finished with " << err_vec->at(0);
      CHECK(err_vec->at(0) == MedusaError::kNoError);
      notify.Notify();
    };
  func_disabler_.Wrap(&done_cb);
  vector<int64> egid_vec(1, egroup_id);
  vector<Medusa::MedusaValue::Ptr> mvalue_vec(1, new_mvalue);
  notify.Reset();
  cluster_mgr_->medusa()->UpdateExtentGroupIdMap(
    make_shared<vector<int64>>(move(egid_vec)),
    true /* use_CAS */,
    make_shared<vector<Medusa::MedusaValue::Ptr>>(move(mvalue_vec)),
    done_cb,
    false /* cached_values */);
  notify.Wait();
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::FinalizeTentativeUpdate(
  const int64 egroup_id,
  const int64 intent_sequence,
  const shared_ptr<WriteExtentGroupRet>& ret) {

  Medusa::MedusaValue::PtrConst mvalue =
    medusa_connector_->LookupExtentGroupIdMapEntry(egroup_id);
  CHECK(mvalue && mvalue->extent_groupid_map_entry);

  const medusa::MedusaExtentGroupIdMapEntryProto::ControlBlock *const
    old_control_block = mvalue->extent_groupid_map_entry->control_block();

  const bool managed_by_aes =
    StargateUtil::GetEgroupProperty(
      *old_control_block,
      MedusaExtentGroupIdMapEntryProto::ControlBlock::kManagedByAes);

  DCHECK(!managed_by_aes) << "Finalizing AES egroup TU not supported "
                          << old_control_block->ShortDebugString();
  DCHECK(!StargateUtil::IsErasureCodedEgroup(*old_control_block))
    << "Finalizing erasure coded egroup TU not supported"
    << old_control_block->ShortDebugString();

  Medusa::MedusaValue::Ptr new_mvalue = make_shared<Medusa::MedusaValue>();
  new_mvalue->epoch = mvalue->epoch;
  new_mvalue->timestamp = mvalue->timestamp + 1;
  unique_ptr<MedusaExtentGroupIdMapEntryProto::ControlBlock>
    new_control_block =
    make_unique<MedusaExtentGroupIdMapEntryProto::ControlBlock>();
  new_control_block->CopyFrom(*old_control_block);
  DCHECK_GE(old_control_block->replicas_size(), 1)
    << old_control_block->ShortDebugString();
  DCHECK_GE(old_control_block->replicas(0).tentative_updates_size(), 1)
    << old_control_block->ShortDebugString();
  // Mapping from extents to their refcounts as per the tentative updates
  // that will be applied.
  unordered_map<ExtentId::PtrConst, int32,
    ExtentId::Hasher, ExtentId::Comparator> extent_2_refcount_map;
  for (int xx = 0; xx < new_control_block->replicas_size(); ++xx) {
    MedusaExtentGroupIdMapEntryProto::Replica *const replica =
      new_control_block->mutable_replicas(xx);
    CHECK_EQ(replica->diff_slices_size(), 0);
    CHECK_EQ(replica->diff_extents_size(), 0);
    bool found = false;
    replica->clear_tentative_updates();
    for (const auto& tu : old_control_block->replicas(0).tentative_updates()) {
      found |= (tu.intent_sequence() == intent_sequence);
      if (tu.intent_sequence() > intent_sequence) {
        replica->add_tentative_updates()->CopyFrom(tu);
      } else if (xx == 0) {
        for (int zz = 0; zz < tu.extent_id_size(); ++zz) {
          unique_ptr<ExtentIdProto> extent_id_proto =
            make_unique<ExtentIdProto>();
          extent_id_proto->CopyFrom(tu.extent_id(zz));
          ExtentId::PtrConst extent_id =
            make_shared<ExtentId>(move(extent_id_proto));
          CHECK(!extent_id->HasExtentIdMapping())
            << "Finalizing shared extents not supported";
          extent_2_refcount_map[extent_id] = tu.refcount(zz);
        }
      }
    }
    CHECK(found) << intent_sequence << " "
                 << old_control_block->ShortDebugString();
    replica->set_intent_sequence(intent_sequence);
  }
  CHECK_GT(extent_2_refcount_map.size(), 0)
    << old_control_block->ShortDebugString();

  shared_ptr<MedusaExtentGroupIdMapEntry::SliceMap> diff_smap =
    make_shared<MedusaExtentGroupIdMapEntry::SliceMap>();
  shared_ptr<MedusaExtentGroupIdMapEntry::ExtentMap> diff_emap =
    make_shared<MedusaExtentGroupIdMapEntry::ExtentMap>();
  CHECK_GT(ret->primary().slices_size(), 0) << ret->ShortDebugString();

  const int32 old_slice_allocation_offset =
    StargateUtil::ExtentGroupNextSliceAllocationOffset(
      egroup_id, *mvalue->extent_groupid_map_entry);
  int32 next_slice_allocation_offset = old_slice_allocation_offset;
  for (int xx = 0; xx < ret->primary().slices_size(); ++xx) {
    SliceState::UPtr slice_state_uptr =
      SliceState::Create(ret->primary().slices(xx));
    const SliceState *slice_state = slice_state_uptr.get();
    (*diff_smap)[slice_state->slice_id()] = move(slice_state_uptr);

    // Ignore empty slices in the extent group file size computation.
    if (!slice_state->has_extent_group_offset()) {
      continue;
    }
    CHECK_GT(slice_state->transformed_length(), 0);

    const int32 next_slice_start_offset =
      slice_state->extent_group_offset() +
      slice_state->transformed_length() + slice_state->cushion();
    if (next_slice_start_offset > next_slice_allocation_offset) {
      next_slice_allocation_offset = next_slice_start_offset;
    }
  }
  CHECK(!diff_smap->empty()) << ret->ShortDebugString();
  new_control_block->set_next_slice_allocation_offset_hint(
    next_slice_allocation_offset);
  for (int xx = 0; xx < ret->primary().extents_size(); ++xx) {
    unique_ptr<ExtentIdProto> extent_id_proto = make_unique<ExtentIdProto>();
    extent_id_proto->CopyFrom(ret->primary().extents(xx).extent_id());
    ExtentId::PtrConst extent_id =
      make_shared<ExtentId>(move(extent_id_proto));

    unordered_map<ExtentId::PtrConst, int32,
      ExtentId::Hasher, ExtentId::Comparator>::const_iterator eiter =
        extent_2_refcount_map.find(extent_id);
    if (eiter == extent_2_refcount_map.end()) {
      LOG(ERROR) << "Ignoring extent id " << extent_id->ToString()
                 << " returned by extent store upon writing to extent"
                 << " group " << egroup_id;
        continue;
    }
    // Find the extent state corresponding to extent_id in
    // old_egroup_metadata.
    ScopedArenaMark am;
    const ExtentState::ArenaUPtrConst& old_extent_state =
      mvalue->extent_groupid_map_entry->LookupExtent(extent_id, am.arena());

    ExtentState::ArenaUPtr extent_state = nullptr;
    if (old_extent_state) {
      extent_state = ExtentState::Create(*old_extent_state);
    } else {
      extent_state = ExtentState::Create(true /* create_proto */);
      extent_state->set_extent_id(*extent_id);
    }

    const WriteExtentGroupRet::Primary::Extent& extent =
      ret->primary().extents(xx);
    for (int yy = 0;
         yy < extent.diff_data_location().slice_ids_size();
         ++yy) {
      if (old_extent_state) {
        // We're updating an existing extent. The extent store might send us
        // diffs regarding replacements it made to the slice ids in the
        // extent. It shouldn't be adding any more slices though.
        CHECK_GT(extent_state->slice_ids_size(), yy);
        const int indx = extent.diff_data_location().slice_indices(yy);
        extent_state->set_slice_ids(
          indx, extent.diff_data_location().slice_ids(yy));
      } else {
        // We're adding a new extent - the extent store should be sending us
        // all the slice ids for the extent in order.
        CHECK_EQ(extent.diff_data_location().slice_indices(yy), yy);
        extent_state->add_slice_ids(
          extent.diff_data_location().slice_ids(yy));
      }
    }
    if (extent.diff_data_location().has_first_slice_offset()) {
      extent_state->set_first_slice_offset(
        extent.diff_data_location().first_slice_offset());
    }

    extent_state->set_refcount(eiter->second);
    (*diff_emap)[extent_id] = move(extent_state);
    // Remove extent_id from extent_2_refcount_map.
    extent_2_refcount_map.erase(extent_id);
  }
  // Create the new metadata.
  new_mvalue->extent_groupid_map_entry =
    make_shared<MedusaExtentGroupIdMapEntry>(
      move(new_control_block), diff_smap, diff_emap);
  Notification notify;
  Function<void(shared_ptr<vector<MedusaError::Type>> err_vec)> done_cb =
    [&notify](shared_ptr<vector<MedusaError::Type>> err_vec) {
      CHECK_EQ(err_vec->size(), 1);
      LOG(INFO) << "Update finished with " << err_vec->at(0);
      CHECK(err_vec->at(0) == MedusaError::kNoError);
      notify.Notify();
    };
  func_disabler_.Wrap(&done_cb);
  vector<int64> egid_vec(1, egroup_id);
  vector<Medusa::MedusaValue::Ptr> mvalue_vec(1, new_mvalue);
  notify.Reset();
  cluster_mgr_->medusa()->UpdateExtentGroupIdMap(
    make_shared<vector<int64>>(move(egid_vec)),
    true /* use_CAS */,
    make_shared<vector<Medusa::MedusaValue::Ptr>>(move(mvalue_vec)),
    done_cb,
    false /* cached_values */);
  notify.Wait();
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::LookupAndVerifyNextSliceAllocationOffsetHint(
  const int64 egroup_id, const int64 expected_value) {

  // Sleep for a bit to ensure that the medusa update finishes for the write
  // op.
  sleep(1);

  MedusaExtentGroupIdMapEntry::Ptr extent_groupid_map_entry;
  LookupExtentGroupIdMapHelper(egroup_id, &extent_groupid_map_entry);

  // Fetch the hint from the egroup metadata.
  int32 actual_value =
    StargateUtil::ExtentGroupNextSliceAllocationOffset(
      egroup_id, *(extent_groupid_map_entry.get()), true /* use_hint */);
  CHECK_EQ(expected_value, actual_value) << egroup_id;
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::TestBytesToWriteBeforeRetryingCompressionUpdateHelper(
  const int64 egroup_id, const bool managed_by_aes,
  MedusaExtentGroupIdMapEntry::Ptr&& extent_groupid_map_entry_secondary,
  MedusaExtentGroupIdMapEntry::Ptr&& extent_groupid_map_entry_primary,
  const int64 expected_value,
  const bool check_primary_copy) {

  // Test if the field bytes_to_write_before_retrying_compression is updated
  // for the extent store write request. This field is available in both map 3
  // in medusa (for non-AES egroups) and extent group physical state map (for
  // AES egroups). For egroups managed by AES, the caller already fetches the
  // state while verifying the slice/extent state. For non-AES cases, we need
  // to lookup the extent group id map entry from medusa. For non-AES, we just
  // need to fetch a single state from medusa.
  if (!managed_by_aes) {
    LookupExtentGroupIdMapHelper(
      egroup_id, &extent_groupid_map_entry_primary);
  }
  CHECK(extent_groupid_map_entry_primary);
  LOG(INFO) << "Control block for egroup: " << egroup_id << ": secondary=["
            << (extent_groupid_map_entry_secondary->control_block() ?
                extent_groupid_map_entry_secondary->control_block()->
                  ShortDebugString() : "none")
            << "], primary=["
            << (extent_groupid_map_entry_primary->control_block() ?
                extent_groupid_map_entry_primary->control_block()->
                  ShortDebugString() : "none")
            << "]";

  int64 actual_secondary_value = -1;
  int64 actual_primary_value = -1;
  bool field_set_in_secondary = false;
  bool field_set_in_primary = false;
  if (extent_groupid_map_entry_secondary->control_block()->
        has_bytes_to_write_before_retrying_compression()) {
    field_set_in_secondary = true;
    actual_secondary_value =
      extent_groupid_map_entry_secondary->control_block()->
      bytes_to_write_before_retrying_compression();
  }

  if (extent_groupid_map_entry_primary->control_block()->
        has_bytes_to_write_before_retrying_compression()) {
    field_set_in_primary = true;
    actual_primary_value =
      extent_groupid_map_entry_primary->control_block()->
      bytes_to_write_before_retrying_compression();
  }

  if (check_primary_copy) {
    CHECK_EQ(field_set_in_primary, field_set_in_secondary);
    CHECK_EQ(actual_primary_value, actual_secondary_value);
  }
  if (expected_value == -1) {
    CHECK(!field_set_in_primary && !field_set_in_secondary)
      << "p=" << field_set_in_primary << ", s=" << field_set_in_secondary;
    return;
  }
  CHECK_EQ(actual_secondary_value, expected_value);
}

//-----------------------------------------------------------------------------

shared_ptr<WriteExtentGroupArg> EgroupOpsTester::WriteEgArg(
  const bool managed_by_aes,
  const bool slices_stored_by_id,
  const int slice_group_size) {

  shared_ptr<WriteExtentGroupArg> arg = make_shared<WriteExtentGroupArg>();
  arg->set_qos_principal_name(string());
  arg->set_qos_priority(StargateQosPriority::kWrite);
  arg->set_owner_vdisk_id(vdisk_id_);
  arg->set_owner_container_id(container_id_);
  arg->set_managed_by_aes(managed_by_aes);
  if (managed_by_aes) {
    arg->set_slices_stored_by_id(slices_stored_by_id);
    arg->set_slice_group_size(slice_group_size);
    arg->set_vdisk_incarnation_id(9000);
  }
  return arg;
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::WriteExtentGroup(
  shared_ptr<WriteExtentGroupArg> arg,
  const IOBuffer::Ptr& data,
  Function<void(StargateError::Type, shared_ptr<string>,
                shared_ptr<WriteExtentGroupRet>)> done_cb) {

  DCHECK_GE(arg->disk_ids_size(), 1);
  DCHECK_GE(arg->primary().extents_size(), 1);

  vector<DataTransformation::Type> transformation_type_vec;
  for (int ii = 0; ii < arg->transformation_type_list_size(); ++ii) {
    transformation_type_vec.emplace_back(arg->transformation_type_list(ii));
  }

  // Make dummy tentative update for first write, else extent store usage
  // op might delete the egroup state if it doesn't find any entry for this
  // egroup in map 3.
  const int64 primary_disk_id = arg->disk_ids(0);
  const int64 intent_sequence =
    arg->managed_by_aes() ?
    arg->global_metadata_intent_sequence() : arg->intent_sequence();
  if (intent_sequence == 0) {
    MakeTentativeUpdate(
      arg->extent_group_id(),
      primary_disk_id,
      arg->disk_ids_size() == 1 ? -1 : arg->disk_ids(1)
        /* secondary_disk_id */,
      arg->transformation_type(),
      transformation_type_vec,
      arg->mutable_primary()->mutable_extents(0)->mutable_extent_id(),
      arg->managed_by_aes());
  }
  const StargateInterface::Ptr& ifc =
    cluster_mgr_->stargate_mgr()->GetStargateInterfaceForDisk(primary_disk_id);
  LOG(INFO) << "Issuing WriteExtentGroup RPC " << arg->ShortDebugString()
            << " to stargate "
            << cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(
                 primary_disk_id);

  ifc->WriteExtentGroup(arg, data, move(done_cb),
                        FLAGS_egroup_test_write_eg_rpc_timeout * 1000);
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::SetExtentGroupState(
  const int64 disk_id,
  const shared_ptr<SetEgroupStateArg>& arg) {

  Notification notification;
  const StargateInterface::Ptr& ifc =
    cluster_mgr_->stargate_mgr()->GetStargateInterfaceForDisk(disk_id);
  ifc->SetEgroupState(arg, bind(&Notification::Notify, &notification));
  LOG(INFO) << "Sending SetEgroupState RPC to " << disk_id
            << " with arg: " << arg->ShortDebugString();
  notification.Wait();
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::VerifyPhysicalStateControlBlock(
  const ControlBlockFB *const cb1,
  const ControlBlockFB *const cb2,
  const bool skip_fallocate_length) {

  CHECK_EQ(cb1->applied_intent_sequence(), cb2->applied_intent_sequence());
  CHECK_EQ(cb1->latest_intent_sequence(), cb2->latest_intent_sequence());
  CHECK_EQ(cb1->highest_committed_intent_sequence(),
           cb2->highest_committed_intent_sequence());
  CHECK_EQ(cb1->global_metadata_intent_sequence(),
           cb2->global_metadata_intent_sequence());
  CHECK_EQ(cb1->last_mutator_incarnation_id(),
           cb2->last_mutator_incarnation_id());
  CHECK_EQ(cb1->block_size(), cb2->block_size());
  CHECK_EQ(cb1->transformed_size_blocks(), cb2->transformed_size_blocks());
  if (!skip_fallocate_length) {
    CHECK_EQ(cb1->fallocate_size_blocks(), cb2->fallocate_size_blocks());
  }
  CHECK_EQ(cb1->next_slice_id(), cb2->next_slice_id());
  CHECK_EQ(cb1->untransformed_slice_length_blocks(),
           cb2->untransformed_slice_length_blocks());
  if (cb1->transformation_type_vec()) {
    CHECK_EQ(cb1->transformation_type_vec()->size(),
             cb2->transformation_type_vec()->size());
  } else {
    CHECK(!cb2->transformation_type_vec());
  }
  CHECK_EQ(cb1->slices_stored_by_id(), cb2->slices_stored_by_id());
  if (cb1->non_dedup_extent_ids_vec()) {
    CHECK(!cb1->dedup_extent_ids_vec());
    CHECK(!cb2->dedup_extent_ids_vec());
    CHECK_EQ(cb1->non_dedup_extent_ids_vec()->size(),
             cb2->non_dedup_extent_ids_vec()->size());

    unordered_set<ExtentId::PtrConst, ExtentId::Hasher, ExtentId::Comparator>
      extent_id_set;
    for (uint ii = 0; ii < cb1->non_dedup_extent_ids_vec()->size(); ++ii) {
      auto extent_ids = cb1->non_dedup_extent_ids_vec()->Get(ii);
      CHECK_GT(extent_ids->vblocks()->size(), 0);
      const int64 owner_id = extent_ids->owner_id();
      for (uint jj = 0; jj < extent_ids->vblocks()->size(); ++jj) {
        ExtentIdProto eid_proto;
        eid_proto.set_owner_id(owner_id);
        eid_proto.set_vdisk_block(extent_ids->vblocks()->Get(jj));
        extent_id_set.insert(make_shared<ExtentId>(&eid_proto, true));
      }
    }
    for (uint ii = 0; ii < cb2->non_dedup_extent_ids_vec()->size(); ++ii) {
      auto extent_ids = cb2->non_dedup_extent_ids_vec()->Get(ii);
      CHECK_GT(extent_ids->vblocks()->size(), 0);
      const int64 owner_id = extent_ids->owner_id();
      for (uint jj = 0; jj < extent_ids->vblocks()->size(); ++jj) {
        ExtentIdProto eid_proto;
        eid_proto.set_owner_id(owner_id);
        eid_proto.set_vdisk_block(extent_ids->vblocks()->Get(jj));
        ExtentId::PtrConst extent_id =
          make_shared<ExtentId>(&eid_proto, true);
        CHECK(extent_id_set.find(extent_id) != extent_id_set.end());
        extent_id_set.erase(extent_id);
      }
    }
  } else {
    CHECK(!cb2->non_dedup_extent_ids_vec());
    CHECK_EQ(cb1->dedup_extent_ids_vec()->size(),
             cb2->dedup_extent_ids_vec()->size());

    unordered_set<ExtentId::PtrConst, ExtentId::Hasher, ExtentId::Comparator>
      extent_id_set;
    for (uint ii = 0; ii < cb2->dedup_extent_ids_vec()->size(); ++ii) {
      auto extent_ids = cb1->dedup_extent_ids_vec()->Get(ii);
      CHECK_GT(extent_ids->sha1_hashes()->size(), 0);
      const int64 owner_id = extent_ids->owner_id();
      const int32 extent_size = extent_ids->size();
      for (uint jj = 0; jj < extent_ids->sha1_hashes()->size(); ++jj) {
        ExtentIdProto eid_proto;
        eid_proto.set_owner_id(owner_id);
        eid_proto.set_extent_size(extent_size);
        eid_proto.set_sha1_hash(extent_ids->sha1_hashes()->Get(jj)->str());
        extent_id_set.insert(make_shared<ExtentId>(&eid_proto, true));
      }
    }
    for (uint ii = 0; ii < cb2->dedup_extent_ids_vec()->size(); ++ii) {
      auto extent_ids = cb2->dedup_extent_ids_vec()->Get(ii);
      CHECK_GT(extent_ids->sha1_hashes()->size(), 0);
      const int64 owner_id = extent_ids->owner_id();
      const int32 extent_size = extent_ids->size();
      for (uint jj = 0; jj < extent_ids->sha1_hashes()->size(); ++jj) {
        ExtentIdProto eid_proto;
        eid_proto.set_owner_id(owner_id);
        eid_proto.set_extent_size(extent_size);
        eid_proto.set_sha1_hash(extent_ids->sha1_hashes()->Get(jj)->str());
        ExtentId::PtrConst extent_id =
          make_shared<ExtentId>(&eid_proto, true);
        CHECK(extent_id_set.find(extent_id) != extent_id_set.end());
        extent_id_set.erase(extent_id);
      }
    }
  }
  if (cb1->slice_group_id_vec()) {
    CHECK_EQ(cb1->slice_group_id_vec()->size(),
             cb2->slice_group_id_vec()->size());
  } else {
    CHECK(!cb2->slice_group_id_vec());
  }
}

//-----------------------------------------------------------------------------

static void ReadExtentGroupMultiRegionDone(
  nutanix::test::CdpCompositeClusterManager *const cluster_mgr,
  StargateError::Type err,
  shared_ptr<string> err_detail,
  shared_ptr<ReadExtentGroupRet> ret,
  IOBuffer::Ptr&& iobuf,
  Notification *notify,
  vector<shared_ptr<EgroupOpsTester::SliceState>> slice_state_vec,
  const IOBuffer::PtrConst& expected_data,
  const set<StargateError::Type>& expected_errors) {

  // Check for possible read errors.
  CHECK_GT(expected_errors.count(err), 0) << err;
  if (err != StargateError::kNoError) {
    if (notify) {
      notify->Notify();
    }
    return;
  }

  // Verify if we get the right amount of data.
  CHECK_GT(expected_data->size(), 0);
  CHECK_EQ(iobuf->size(), expected_data->size());

  // Verify the data read is the same as the data written.
  if (!IOBufferUtil::Memcmp(expected_data.get(), iobuf.get())) {
    LOG(INFO) << "Dumping data files in: "
              << cluster_mgr->cdp_options().test_out_dir;
    int fd = open(
      StringJoin(
        cluster_mgr->cdp_options().test_out_dir, "/expected.data").c_str(),
      O_RDWR | O_CREAT, 0755);
    SYSCALL_CHECK(fd >= 0);
    SYSCALL_CHECK(IOBufferUtil::WriteToFile(expected_data.get(), fd, 0) ==
                    expected_data->size());
    fsync(fd);
    close(fd);

    fd = open(
      StringJoin(cluster_mgr->cdp_options().test_out_dir,
                 "/read.data").c_str(),
      O_RDWR | O_CREAT, 0755);
    SYSCALL_CHECK(fd >= 0);
    SYSCALL_CHECK(IOBufferUtil::WriteToFile(iobuf.get(), fd, 0) ==
                  iobuf->size());
    fsync(fd);
    close(fd);

    LOG(FATAL) << "Data mismatch "
               << "data size: " << expected_data->size()
               << " iobuf size: " << iobuf->size();
  }

  notify->Notify();
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::ReadExtentGroupMultiRegion(
  const int64 egroup_id,
  const int64 primary_disk_id,
  const vector<pair<int64, int64>>& region_offset_size_vec,
  const vector<shared_ptr<EgroupOpsTester::SliceState>>& slice_state_vec,
  const shared_ptr<EgroupOpsTester::Extent>& extent_state,
  const IOBuffer::PtrConst& expected_data,
  const string& transformation_type,
  const vector<DataTransformation::Type>& transformation_type_vec,
  const set<StargateError::Type>& expected_errors,
  const bool managed_by_aes,
  const int32 untransformed_slice_length,
  const bool slices_stored_by_id,
  const int slice_group_size) {

  // Get stargate interface associated with the primary disk.
  const StargateInterface::Ptr& iface =
    cluster_mgr_->stargate_mgr()->GetStargateInterfaceForDisk(primary_disk_id);
  CHECK(iface);

  // Prepare the read request.
  shared_ptr<ReadExtentGroupArg> arg = make_shared<ReadExtentGroupArg>();
  arg->set_extent_group_id(egroup_id);
  arg->set_qos_principal_name(string());
  arg->set_qos_priority(StargateQosPriority::kRead);
  arg->set_owner_vdisk_id(vdisk_id_);
  arg->set_owner_container_id(container_id_);
  arg->set_disk_id(primary_disk_id);
  arg->set_managed_by_aes(managed_by_aes);
  if (managed_by_aes) {
    arg->set_untransformed_slice_length(untransformed_slice_length);
  }
  arg->set_slices_stored_by_id(slices_stored_by_id);
  arg->set_slice_group_size(slice_group_size);

  // Intent seqeunce should be the previous successful write intent sequence
  // number for this egroup.
  arg->set_min_expected_intent_sequence(
    egroup_id_intent_seq_map_[egroup_id]);

  // Set the slice state to arg. This is populated with the necessary
  // information by the caller.
  for (uint jj = 0; jj < slice_state_vec.size(); ++jj) {
    CHECK(slice_state_vec[jj]->has_extent_group_offset());
    slice_state_vec[jj]->CopyTo(arg->add_slices());
  }

  if (!transformation_type.empty()) {
    arg->set_transformation_type(transformation_type);
  } else {
    for (const DataTransformation::Type transform_type :
           transformation_type_vec) {
      arg->add_transformation_type_list(transform_type);
    }
  }

  ReadExtentGroupArg::Extent *ex = arg->add_extents();
  ex->mutable_data_location()->CopyFrom(extent_state->diff_data_location());
  ex->mutable_extent_id()->CopyFrom(extent_state->extent_id());

  for (uint ii = 0; ii < region_offset_size_vec.size(); ++ii) {
    ex->add_region_offset(get<0>(region_offset_size_vec[ii]));
    ex->add_region_length(get<1>(region_offset_size_vec[ii]));
  }

  Notification notification;
  Function<void(StargateError::Type,
                shared_ptr<string>,
                shared_ptr<ReadExtentGroupRet>,
                IOBuffer::Ptr&&)> done_cb =
    func_disabler_.Bind(&ReadExtentGroupMultiRegionDone, cluster_mgr_.get(),
                        _1, _2, _3, _4, &notification,
                        slice_state_vec, expected_data, expected_errors);

  // Invoke the stargate read extent group request.
  iface->ReadExtentGroup(move(arg), move(done_cb));
  notification.Wait();
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::GetDiskUsageStats(
  const shared_ptr<const vector<int64>>& disk_id_vec,
  vector<shared_ptr<DiskUsageStatProto>> *const disk_usage_vec) {

  StatsError error;
  StatsController<DiskUsageStatProto>::LookupStatsResultPtr stat_vec;
  CallbackWaiter<
    StatsError, StatsController<DiskUsageStatProto>::LookupStatsResultPtr> cb;

  LOG(INFO) << "Looking up disk usage stats";
  disk_usage_stats_->LookupIntegralStats(disk_id_vec, cb);
  tie(error, stat_vec) = cb.Wait();

  CHECK(error == StatsError::kNoError) << "Failed to lookup disk usage stats";
  CHECK(stat_vec) << "Invalid stats given by stats backend";
  // Expect usage stats for all disks requested.
  CHECK_EQ(stat_vec->size(), disk_id_vec->size())
    << "Disk usage stats not returned for all disks.";

  LOG(INFO) << "Populating disk usage stats";
  disk_usage_vec->clear();
  disk_usage_vec->reserve(disk_id_vec->size());
  for (unsigned ii = 0; ii < disk_id_vec->size(); ++ii) {
    const int64 disk_id = disk_id_vec->at(ii);
    shared_ptr<DiskUsageStatProto> stat = make_shared<DiskUsageStatProto>();
    if (stat_vec->at(ii).first) {
      stat->CopyFrom(*stat_vec->at(ii).first);
    } else {
      LOG(WARNING) << "Disk usage stats not found for disk " << disk_id;
    }

    LOG(INFO) << "Stats for disk_id " << disk_id << " : "
              << stat->ShortDebugString();
    disk_usage_vec->emplace_back(move(stat));
  }
}

//-----------------------------------------------------------------------------

int64 EgroupOpsTester::LookupOrUpdateOwnerVDiskIdInEgidMap(
  const int64 egroup_id,
  const bool update,
  const int64 new_owner_vdisk_id) {

  // Read the metadata of the extent group from Medusa's extent group id map.
  shared_ptr<vector<int64>> extent_group_id_vec = make_shared<vector<int64>>();
  extent_group_id_vec->push_back(egroup_id);
  vector<Medusa::MedusaValue::PtrConst> mvalue_vec_ret;

  Notification notification;
  const Function<void(MedusaError::Type,
                      shared_ptr<vector<Medusa::MedusaValue::PtrConst>>)>
    done_cb = func_disabler_.Bind(&LookupExtentGroupIdMapCompleted,
      _1, _2, &mvalue_vec_ret, &notification);
  cluster_mgr_->medusa()->LookupExtentGroupIdMap(extent_group_id_vec,
                                                 false /* cached_ok */,
                                                 done_cb);
  notification.Wait();
  CHECK_EQ(mvalue_vec_ret.size(), 1);

  Medusa::MedusaValue::PtrConst mvalue = move(mvalue_vec_ret[0]);
  CHECK(mvalue && mvalue->extent_groupid_map_entry) << egroup_id;

  const MedusaExtentGroupIdMapEntry * const egroup_metadata =
    mvalue->extent_groupid_map_entry.get();

  const int64 original_owner_vdisk_id =
    egroup_metadata->control_block()->owner_vdisk_id();

  if (!update) {
    return original_owner_vdisk_id;
  }
  CHECK_GT(new_owner_vdisk_id, 0);

  Medusa::MedusaValue::Ptr new_mvalue = make_shared<Medusa::MedusaValue>();
  new_mvalue->epoch = mvalue->epoch;
  new_mvalue->timestamp = mvalue->timestamp + 1;

  unique_ptr<MedusaExtentGroupIdMapEntryProto::ControlBlock>
    new_control_block =
    make_unique<MedusaExtentGroupIdMapEntryProto::ControlBlock>();
  new_control_block->CopyFrom(*egroup_metadata->control_block());
  new_control_block->set_owner_vdisk_id(new_owner_vdisk_id);
  new_mvalue->extent_groupid_map_entry =
    make_shared<MedusaExtentGroupIdMapEntry>(
      move(new_control_block),
      nullptr /* diff_slice_map */, nullptr /* diff_extent_map */);

  // Schedule new_mvalue to be written to Medusa.
  vector<int64> egid_vec(1, egroup_id);
  vector<Medusa::MedusaValue::Ptr> mvalue_vec(1, move(new_mvalue));

  notification.Reset();
  const Function<void(shared_ptr<vector<MedusaError::Type> >)>
    update_done_cb = func_disabler_.Bind(&TentativeUpdateCompleted, _1,
                                         &notification);
  cluster_mgr_->medusa()->UpdateExtentGroupIdMap(
    make_shared<vector<int64>>(move(egid_vec)),
    true /* use_CAS */,
    make_shared<vector<Medusa::MedusaValue::Ptr>>(move(mvalue_vec)),
    update_done_cb);
  notification.Wait();

  // TODO(sanket): Check return code for update in update_done_cb.
  return new_owner_vdisk_id;
}

//-----------------------------------------------------------------------------

vector<string> EgroupOpsTester::GetStargateLogs(
  const string& log_message, const int index) {

  vector<string> logs;
  FLAGS_log_dir = CHECK_NOTNULL(getenv("TESTOUTDIR"));
  const string log_file = StringJoin(FLAGS_log_dir,
    "/data/",
    index >= 0 ? to_string(index) : "*",
    ".stargate.test/logs/stargate.INFO");
  const string cmd = StringJoin("/bin/grep ", "\"", log_message, "\" ",
                                log_file);
  LOG(INFO) << "Executing: " << cmd;
  PopenExecutor::Ptr popen = make_shared<PopenExecutor>(cmd, "r");
  FILE *fp = popen->read_stream();
  size_t size = 4096;
  char *buf_str = static_cast<char *>(malloc(size));
  while (true) {
    if (getline(&buf_str, &size, fp) == -1) {
      break;
    }
    string log_str = buf_str;
    boost::trim_right(log_str);
    LOG(INFO) << "Found the message: " << log_str;
    logs.emplace_back(log_str);
  }
  free(buf_str);
  return logs;
}

//-----------------------------------------------------------------------------

bool EgroupOpsTester::HasStargateLog(
  const string& log_message, const int index) {

  const vector<string> logs = GetStargateLogs(log_message, index);
  return !logs.empty();
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::CorruptNormalReplica(
  const int64 egroup_id, const int replica_index) {

  const shared_ptr<ConfigurationProto_Disk> disk =
    GetEgroupReplicaDisk(egroup_id, replica_index);
  CHECK(disk);
  CHECK(!disk->mount_path().empty());
  const string egroup_file_path =
    GetEgroupFilePath(egroup_id, disk->mount_path());
  const int64 disk_id = disk->disk_id();
  LOG(INFO) << "Corrupting egroup " << egroup_id
            << " replica index " << replica_index
            << " disk " << disk->disk_id()
            << " on svm " << disk->service_vm_id()
            << " stargate index "
            << cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(disk_id)
            << " egroup file " << egroup_file_path;
  CorruptEgroupFile(egroup_file_path);
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::CorruptErasureReplica(const int64 egroup_id) {
  const shared_ptr<ConfigurationProto_Disk> disk =
    GetEgroupReplicaDisk(egroup_id, 0);
  CHECK(disk);
  CHECK(!disk->mount_path().empty());
  const string egroup_file_path =
    GetEgroupFilePath(egroup_id, disk->mount_path());
  const int64 disk_id = disk->disk_id();
  LOG(INFO) << "Corrupting erasure code egroup " << egroup_id
            << " replica " << disk_id
            << " on svm " << disk->service_vm_id()
            << " stargate index "
            << cluster_mgr_->stargate_mgr()->GetStargateIndexForDisk(disk_id)
            << " egroup file " << egroup_file_path;
  CorruptEgroupFile(egroup_file_path);
}

//-----------------------------------------------------------------------------

void EgroupOpsTester::CorruptEgroupFile(const string& egroup_file_path) {
  FILE *fp = fopen(egroup_file_path.c_str(), "r+");
  PCHECK(fp) << "Failed to open " << egroup_file_path;
  const int fd = fileno(fp);
  int32 data_size = Random::TL()->Uniform(768, 1024);
  const IOBuffer::Ptr random_iobuffer =
    Random::CreateRandomDataIOBuffer(data_size);
  LOG(INFO) << "Writing " << data_size << " random bytes to corrupt "
            << "egroup file " << egroup_file_path << " at offset 0";
  CHECK(IOBufferUtil::WriteToFile(random_iobuffer.get(), fd, 0));
  fclose(fp);
}

//-----------------------------------------------------------------------------

const shared_ptr<ConfigurationProto_Disk>
EgroupOpsTester::GetEgroupReplicaDisk(
  const int64 egroup_id, const int replica_index) {

  Medusa::MedusaValue::PtrConst mval =
    medusa_connector_->LookupExtentGroupIdMapEntry(egroup_id);
  CHECK(mval) << egroup_id;
  CHECK_GE(mval->timestamp, 0) << egroup_id;
  CHECK(mval->extent_groupid_map_entry) << egroup_id;
  const MedusaExtentGroupIdMapEntry& egroup_metadata =
    *mval->extent_groupid_map_entry;
  const MedusaExtentGroupIdMapEntryProto::ControlBlock *control_block =
    egroup_metadata.control_block();
  CHECK_GT(control_block->replicas_size(), replica_index);
  const MedusaExtentGroupIdMapEntryProto::Replica& replica =
    control_block->replicas(replica_index);
  CHECK_EQ(replica.tentative_updates_size(), 0)
    << control_block->ShortDebugString();
  const int64 disk_id = replica.disk_id();
  const Configuration::PtrConst config = cluster_mgr_->Config();
  return GetDiskFromConfig(config, disk_id);
}

//-----------------------------------------------------------------------------

string EgroupOpsTester::GetEgroupFilePath(
  const int64 egroup_id, const string& disk_mounth_path) {
  const string egroup_dir = GetEgroupDirectory(egroup_id, disk_mounth_path);
  return StringJoin(egroup_dir, "/", egroup_id, ".egroup");
}

//-----------------------------------------------------------------------------

string EgroupOpsTester::GetEgroupFilePath(
  const int64 egroup_id, const int replica_index) {

  const shared_ptr<ConfigurationProto_Disk> disk =
    GetEgroupReplicaDisk(egroup_id, replica_index);
  CHECK(disk);
  CHECK(!disk->mount_path().empty());
  return GetEgroupFilePath(egroup_id, disk->mount_path());
}

//-----------------------------------------------------------------------------

string EgroupOpsTester::GetSha1String(const IOBuffer *const iobuf) {
  string sha1;
  IOBufferUtil::Sha1(iobuf, &sha1);
  string scratch;
  scratch.reserve(2 * Sha1::kSha1Size);
  return Sha1::ToString(sha1.data(), &scratch);
}

//-----------------------------------------------------------------------------

string EgroupOpsTester::GetSha1String(
  const int64 egroup_id, const int replica_index) {

  const string egroup_file_path =
    GetEgroupFilePath(egroup_id, replica_index);
  CHECK(!egroup_file_path.empty()) << OUTVARS(egroup_id, replica_index);

  int64 file_size = -1;

  // Let's try to fetch the input size.
  struct stat statbuf;
  memset(&statbuf, 0, sizeof(statbuf));

  CHECK_EQ(stat(egroup_file_path.c_str(), &statbuf), 0)
    << "Unable to stat " << egroup_file_path;
  CHECK(S_ISREG(statbuf.st_mode)) << egroup_file_path;
  file_size = statbuf.st_size;
  FILE *fp = fopen(egroup_file_path.c_str(), "r+");
  PCHECK(fp) << "Failed to open " << egroup_file_path;
  const int fd = fileno(fp);
  CHECK_GE(fd, 0);
  const IOBuffer::PtrConst iobuf =
    IOBufferUtil::ReadFromFile(fd, 0, file_size);
  const string sha1 = GetSha1String(iobuf.get());
  LOG(INFO) << "egroup file " << egroup_file_path << " has sha1: " << sha1;
  return sha1;
}

//-----------------------------------------------------------------------------

string EgroupOpsTester::GetEgroupDirectory(
  const int64 egroup_id, const string& disk_mounth_path) {

  // The algorithm must be the same as
  // ExtentStore::DiskManager::ExtentGroupIdDirectory
  int64 id = egroup_id;

  string dir_name = StringJoin(disk_mounth_path, "/data");
  for (int xx = 0; xx < FLAGS_data_dir_sublevels; ++xx) {
    dir_name += StringJoin('/', id % FLAGS_data_dir_sublevel_dirs);
    id /= FLAGS_data_dir_sublevel_dirs;
  }
  return dir_name;
}

//-----------------------------------------------------------------------------

const shared_ptr<ConfigurationProto_Disk>
EgroupOpsTester::GetDiskFromConfig(
  const Configuration::PtrConst& config, const int64 disk_id) {

  const ConfigurationProto_Disk *disk = config->LookupDisk(disk_id);
  shared_ptr<ConfigurationProto_Disk> mutable_proto;
  if (disk) {
    mutable_proto = make_shared<ConfigurationProto_Disk>();
    mutable_proto->CopyFrom(*(disk));
  }
  return mutable_proto;
}

//-----------------------------------------------------------------------------

} } } // namespace
