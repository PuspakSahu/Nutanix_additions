/*
Copyright (c) 2023 Nutanix Inc. All rights reserved.

This tool has two use cases:
MODE 1. Recovers an egroup with partially
        unreadable replicas , it merges the payloads .

MODE 2. Reads from an offline disk in xmount mode.

Common flow followed after that:
        - saves it in a file as specified
        - replicates to a healthy disk with the payload
        - calls fixer op which internally deletes the
            unreadable replicas

Author: puspak.sahu@nutanix.com

usage:

merge_replicas -egroup_id=216 -mode=1
               [-target_file] [-target_disk]
               [-stargate_ip] [-num_replicas_desired]
               [-target_storage_tier][-num_replicas_to_migrate]



merge_replicas -egroup_id=216 -mode=2
             [-source_disk_offline] [-offline_disk_id]
             [-external_ip_for_offline] [-managed_by_aes]
             and all other flags for mode 1

*/

#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <ctype.h>
#include <fcntl.h>
#include <fstream>
#include <gflags/gflags.h>
#include <iostream>
#include <sstream>
#include <unistd.h>
#include <vector>

#include "cdp/client/stargate/stargate_base/stargate_error.h"
#include "cdp/client/stargate/stargate_base/stargate_util.h"
#include "cdp/client/stargate/stargate_interface/stargate_interface.h"
#include "cdp/server/stargate/xmount/xmount.h"
#include "content_cache/stargate_crypto_util.h"
#include "mantle/mantle.h"
#include "medusa/medusa.h"
#include "medusa/util/medusa_wrapper.h"
#include "tools/util/connectors/ntnxdb/zeus_connector.h"
#include "util/base/adler32.h"
#include "util/base/data_transformation_type.h"
#include "util/base/iobuffer.h"
#include "util/base/iobuffer_util.h"
#include "util/base/sha1.h"
#include "util/base/string_util.h"
#include "util/base/walltime.h"
#include "util/misc/init_nutanix.h"
#include "util/thread/epoll_driver.h"
#include "util/thread/notification.h"


using namespace nutanix::stargate::interface;
using namespace nutanix;
using namespace nutanix::content_cache;
using namespace nutanix::mantle;
using namespace nutanix::medusa;
using namespace nutanix::medusa::util;
using namespace nutanix::misc;
using namespace nutanix::thread;
using namespace nutanix::stargate;
using namespace std;
using namespace std::placeholders;
using namespace nutanix::zeus;
using namespace nutanix::tools::util::connectors;



DEFINE_int64(mode, 2,
            "mode 1 : reading both replicas"
            "mode 2 : reading from offline disk");

DEFINE_int64(egroup_id, -1,
             "Egroup to operate on. This field is mandatory");

DEFINE_string(target_file, "",
              "Full path of the file where the output must be saved. If not"
              "specified the tool will autogenerate a file name and print it "
              "as output.");

DEFINE_string(stargate_ip, "127.0.0.1",
              "IP address of stargate to which fixer op RPC will be"
              "sent if the vdisk is not hosted at any stargate.");

DEFINE_int64(target_disk, -1,
             "If set , replicates the egroup to that disk"
             "performs replicate op");

DEFINE_int64(num_replicas_desired, -1,
             "Fixer op will make sure these many haelthy replicas exist");

// /Not used currently (for the fixer op)

DEFINE_string(target_storage_tier, "",
             "target_storage_tier");


DEFINE_int64(num_replicas_to_migrate, -1,
             "num_replicas_to_migrate");


// MODE 2 specific : For reading from offline disk


DEFINE_int64(offline_disk_id, -1,
            "disk id of the offline disk, we are trying to read from");

DEFINE_bool(source_disk_offline, false,
             "Set to true , when the FLAGS_offline_disk_id is offline");

DEFINE_string(external_ip_for_offline, "127.0.0.1",
              "external ip for the offline disk, USUALLY NOT NEEDED"
              "used for creating stargate leader handle"
              "fetches from the 'last_service_vm_id' of disk");

DEFINE_bool(managed_by_aes_flag, false,
             "Set to True for AES type metadata");

// --------------------------------------------------------

DECLARE_int32(stargate_port);
DECLARE_int32(stargate_xmount_port);


namespace nutanix { namespace tools { namespace stargate {

class MergeReplicas {

 public:

    // List of possible errors
    enum ErrorType {
            kNoError,
            kMedusaError,
            kStargateError,
            kVDiskConfigError
        };

    typedef shared_ptr<MergeReplicas> Ptr;

    explicit MergeReplicas(EventDriver::Ptr ed);

    ~MergeReplicas();

    // Validates the input params
    // sets the class variables
    // looks up the metadata for egroup_id in MAP 3
    void Validate();


    // performs the main task
    void Execute(const Function<void(ErrorType)>& done_cb);

 private:
    // Set to True for AES type metadata
    bool managed_by_aes_;

    // used for creating a stargate interface in xmount mode
    shared_ptr<thread::EventDriver> delay_ed_;

    // used for creating a stargate interface in xmount mode
    shared_ptr<thread::EpollDriver> epd_;

    // contains slice states info of the egroup trying to read
    vector<SliceState::Ptr> slice_state_vec_;

    // contains extent states info of the egroup trying to read
    vector<ExtentIdProto> extent_id_list_;

    // vector with only one element
    // contains MedusaValue of the egroup_id
    vector<Medusa::MedusaValue::PtrConst> mvalue_vec_ret_;

    // function to save the iobuff payload to FLAGS_target_dir
    void SaveResult(const IOBuffer::Ptr& iobuf) const;

    // Egroup metadata fetched from map 3
    MedusaExtentGroupIdMapEntry::PtrConst egroup_metadata_;

    // for looking and updating map3
    MedusaWrapper::Ptr medusa_wrapper_;

    ZeusConnector::Ptr zeus_connector_;

    // the stargate interface (created before every rpc)
    StargateInterface::Ptr stargate_ifc_;

    // port in the leader handle
    // different for xmount mode and normal mode
    // both declared in the FLAGS section
    int32 port_;

    // for creating stargate interface
    EventDriver::Ptr rpc_ed_;

    // ed_ for fixer op execution
    EventDriver::Ptr ed_;

    // list of disk_id s of all replicas
    vector<int> disk_ids_;

    // remote ret recieved after replica_read_op
    // fed to the replicate op along with the iobuff to replicate
    shared_ptr<ReplicaReadEgroupRet> remote_ret_;

    thread::FunctionDisabler func_disabler_;

    // owner_vdisk_id of the egroup
    int64 vdisk_id_;

    // cluster fault tolerance of the cluster
    int64 cluster_ft_;


    // function for creatinging the stargate interface
    // fetch_for_vdisk :
    //     If set to true,
    //          -disk_id not required
    //          -Gets the interface associated with the vdisk_id_
    //      else,
    //          -gets the interface associated with the disk_id
    // use_http: TRUE: default setting
    //          FALSE: creation mode for xmount mode

    void GetStargateInterface(int64 disk_id,
                                bool fetch_for_vdisk = false,
                                bool use_http = true);


    // helper function for making read_replica_op
    void ReadReplicaEgroupHelper(
        int64 egroup_id,
        int64 disk_id,
        vector<SliceState::Ptr>* slice_state_vec,
        int replica_id,
        bool managed_by_aes = false,
        int64 expected_applied_intent_sequence = -1,
        StargateError::Type expected_error =
                            StargateError::kNoError,
        bool is_ec_egroup = false,
        vector<int> *unreadable_slices = nullptr,
        IOBuffer::Ptr *payload_rec = nullptr,
        bool save_read_replica_ret = false);

    // call back function for ReadReplicaEgroupHelper
    void ReadReplicaEgroupHelperDone(StargateError::Type err,
        shared_ptr<string> err_detail,
        shared_ptr<ReplicaReadEgroupRet> remote_ret,
        IOBuffer::Ptr&& payload,
        Notification *notify,
        vector<SliceState::Ptr> slice_state_vec,
        const StargateError::Type expected_error,
        const bool is_ec_egroup,
        vector<int> *unreadable_slices,
        IOBuffer::Ptr *payload_rec,
        bool save_read_replica_ret);

    // helper function for making fixer_op
    void FixerOpHelper(
        const Function<void(ErrorType)>& done_cb);

    // call back function for fixer_op
    void FixerOpHelperDone(
        const shared_ptr<FixExtentGroupsArg>& arg,
        const Function<void(ErrorType)>& done_cb,
        StargateError::Type error,
        shared_ptr<string> error_detail,
        const shared_ptr<FixExtentGroupsRet>& ret);

    // helper function for making replicate_op
    void ReplicateEgroupHelper(
        const int64 egroup_id,
        const int64 dest_disk_id,
        const vector<shared_ptr<SliceState>>& slice_state_vec,
        const vector<ExtentIdProto> *const extent_id_list,
        const bool managed_by_aes,
        shared_ptr<ReplicaReadEgroupRet> remote_ret,
        const IOBuffer::Ptr& merged_payload,
        const StargateError::Type expected_error =
                StargateError::kNoError);

    // call back function for replicate_op
    static void ReplicateExtentGroupDone(StargateError::Type err,
                        shared_ptr<string> err_detail,
                        shared_ptr<ReplicateExtentGroupRet> ret,
                        const StargateError::Type expected_error,
                        Notification *notify) {
        CHECK_EQ(err, expected_error);
        LOG(INFO) << "Error while replicating:" << err;
        LOG(INFO) << "replication done******* ";

        if (notify) {
            notify->Notify();
        }
    }

    // call back function for UpdateExtentGroupIdMap
    // USed to notify that map 3 has been updated
    static void TentativeUpdateCompleted(
        shared_ptr<vector<MedusaError::Type> > err_vec,
        Notification *notify) {
        if (notify) {
            notify->Notify();
        }
    }

    // Callback function for map 3 lookup for egid
    void LookupExtentGroupIdMapDone(
        MedusaError::Type error,
        shared_ptr<vector<MedusaValue::PtrConst>> value_vec,
        Notification *notification);

};

//----------------------------------------------------------------------------


MergeReplicas::MergeReplicas(EventDriver::Ptr ed) :
                                egroup_metadata_(nullptr),
                                medusa_wrapper_(make_shared<MedusaWrapper>()),
                                zeus_connector_(make_shared<ZeusConnector>()),
                                ed_(ed)
                            {}

MergeReplicas::~MergeReplicas() {
    LOG(INFO) << "destructor called";
}

// --------------------------------------------------------------------------


void MergeReplicas::Validate() {

    managed_by_aes_ = FLAGS_managed_by_aes_flag;
    Configuration::PtrConst zeus_config =
                                zeus_connector_->FetchZeusConfig();
    cluster_ft_ =
        StargateUtil::DefaultClusterMaxFaultTolerance(*zeus_config);

    LOG(INFO) << "Validate started";

    port_ = FLAGS_stargate_port;

    // Lookup in map 3 for egroup metadaata
    Notification notification;
    Function<void(MedusaError::Type,
                shared_ptr<vector<MedusaValue::PtrConst>>)> done_cb =
    bind(&MergeReplicas::LookupExtentGroupIdMapDone, this, _1, _2,
        &notification);

    medusa_wrapper_->medusa()->LookupExtentGroupIdMap(
                        make_shared<vector<int64>>(1, FLAGS_egroup_id),
                        false /* cached_ok */,
                        done_cb);
    notification.Wait();
    CHECK(egroup_metadata_) << "Could not lookup egroup metadata "
                            << FLAGS_egroup_id;


    LOG(INFO) << "Validate complete";

}

// --------------------------------------------------------------------------

void MergeReplicas::Execute(
    const Function<void(ErrorType)>& done_cb) {

    // LOGGING INFO about replicas
    LOG(INFO) << "Totalreplicas : "
              << egroup_metadata_->control_block()->replicas_size();
    LOG(INFO) << "Replica disk_id list: ";
    for (int ii = 0; ii <egroup_metadata_->control_block()
                                    ->replicas_size(); ++ii) {

            LOG(INFO) << egroup_metadata_->control_block()
                                        ->replicas().Get(ii).disk_id();
            disk_ids_.push_back(egroup_metadata_->control_block()
                                        ->replicas().Get(ii).disk_id());
    }

    ScopedArenaMark am;

    // populating slice_state_vec_ from map3 metadata for non-AES
    MedusaExtentGroupIdMapEntry::SliceIterator::Ptr slice_iter  =
    make_shared<MedusaExtentGroupIdMapEntry::SliceIterator>(
    egroup_metadata_.get());

    for (/*initialization above*/; !slice_iter->End(); slice_iter->Next()) {
        slice_state_vec_.push_back(
            move(SliceState::Create(*(slice_iter->Get()).second)));
    }


    vector<int> primary_unreadable_slices;
    vector<int> sec_unreadable_slices;
    IOBuffer::Ptr primary_payload  =  make_shared<IOBuffer>();
    IOBuffer::Ptr sec_payload  =  make_shared<IOBuffer>();
    IOBuffer::Ptr merged_payload  =  make_shared<IOBuffer>();

    // Reading from both replicas
    if (FLAGS_mode == 1) {

        LOG(INFO) << "Mode 1: reading from both rpelcias";
        ReadReplicaEgroupHelper(FLAGS_egroup_id,
                            disk_ids_[0],
                            &slice_state_vec_,
                            0, // replica_id
                            false, // managed_by_aes
                            -1,
                            StargateError::kNoError,
                            false, // is_ec_group
                            &primary_unreadable_slices,
                            &primary_payload);



        ReadReplicaEgroupHelper(FLAGS_egroup_id,
                            disk_ids_[1],
                            &slice_state_vec_,
                            1, // replica_id
                            false, // managed_by_aes
                            -1,
                            StargateError::kNoError,
                            false, // is_ec_group
                            &sec_unreadable_slices,
                            &sec_payload,
                            true); // save_replica_ret : If set,
                                   // saves this ret as remote_ret_


        // checking the final_payload
        const int slice_size = 32 * 1024;
        LOG(INFO) << "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@";
        LOG(INFO) << "recieved final primary payload size _: "
                  << primary_payload->size();
        LOG(INFO) << "num slices in final primary recieved payload _:"
                  << primary_payload->size()/slice_size;

        if (primary_unreadable_slices.size())
            LOG(INFO) << "num corrupted slices in final primary payload:"
                      << primary_unreadable_slices.size() << endl << endl;


        LOG(INFO) << "recieved final sec payload size _: "
                  << sec_payload->size();
        LOG(INFO) << "num slices in final sec recieved payload _: "
                  << sec_payload->size()/slice_size;
        if (sec_unreadable_slices.size())
            LOG(INFO) << "num corrupted slices in final sec payload _: "
                      << sec_unreadable_slices.size();

        LOG(INFO) << "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@";


        // Recieved both the payloads
        // Merging the final payload now
        vector<int> both_replica_unreadable;
        int p_ind = 0;
        int s_ind = 0;
        IOBuffer::Ptr iobuf =  make_shared<IOBuffer>();
        for (size_t ii = 0; ii < slice_state_vec_.size(); ++ii) {

            // is this slice unreadable in primary payload
            bool is_primary_unreadable =
                (find(primary_unreadable_slices.begin(),
                primary_unreadable_slices.end(),
                slice_state_vec_[ii]->slice_id())
                != primary_unreadable_slices.end());

            // is this slice unreadable in secondary payload
            bool is_sec_unreadable =
                (find(sec_unreadable_slices.begin(),
                sec_unreadable_slices.end(),
                slice_state_vec_[ii]->slice_id())
                != sec_unreadable_slices.end());

            if (!is_primary_unreadable) {
                iobuf = primary_payload->
                        Clone(p_ind++ * slice_size, slice_size);
                if (!is_sec_unreadable) s_ind++;
            } else {
                if (!is_sec_unreadable) {
                    iobuf = sec_payload->
                        Clone(s_ind++ * slice_size, slice_size);
                } else {
                LOG(INFO) << "Both replicas have corrupt slice id "
                          << slice_state_vec_[ii]->slice_id();
                both_replica_unreadable.push_back
                    (slice_state_vec_[ii]->slice_id());

                continue;
                }
            }
            merged_payload->AppendIOBuffer(iobuf.get());
        }

        LOG(INFO) << "final merge_payload size _: "
                  << merged_payload->size();
        LOG(INFO) << "num slices in final merge_payload _: "
                  << merged_payload->size()/slice_size;
        LOG(INFO) << "num corrupted slices in both payload _: "
                  << both_replica_unreadable.size();

    } else {

        // Reading from offline disks
        CHECK_GE(FLAGS_offline_disk_id, 0);
        LOG(INFO) << "Mode 2: reading from offline replcia";
        LOG(INFO) << "managed_by_aes_:" << managed_by_aes_;
        LOG(INFO) << "slice_state_vec_ aize:" << slice_state_vec_.size();

        // This roc call gives the whole slice state info and
        // partial data , so we have to make a read_replica_op
        // once more with the updated slice state to get full data
        ReadReplicaEgroupHelper(FLAGS_egroup_id,
                            FLAGS_offline_disk_id,
                            &slice_state_vec_,
                            0,
                            managed_by_aes_,
                            -1,
                            StargateError::kNoError,
                            false,
                            &primary_unreadable_slices,
                            &merged_payload,
                            true /* save read_replica_ret */);

        const int slice_size = 32 * 1024;
        LOG(INFO) << "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@";
        LOG(INFO) << "recieved final primary payload size: "
                  << merged_payload->size();
        LOG(INFO) << "num slices in final primary recieved payload: "
                  << merged_payload->size()/slice_size;


        LOG(INFO) << "slice_state_vec_:" << slice_state_vec_.size();
        LOG(INFO) << "remote_ret_ slice state size: "
                  << remote_ret_->extent_group_metadata().slices_size();


        // populate slice_state_vec_ for AES case for second call
        for (int ii = 0;
            ii < remote_ret_->extent_group_metadata().slices_size(); ii++) {

            slice_state_vec_.push_back(move(SliceState::Create(
                remote_ret_->extent_group_metadata().slices().Get(ii))));

            const int32 slice_id = slice_state_vec_.back()->slice_id();
            LOG(INFO) << "slice_id" <<  slice_id;
        }

        // reading 2nd time to actually get the full data
        // IN case of Non-AES we already have full data
        if (managed_by_aes_)
            ReadReplicaEgroupHelper(FLAGS_egroup_id,
                            FLAGS_offline_disk_id,
                            &slice_state_vec_,
                            0,
                            managed_by_aes_, // managed by aes
                            -1,
                            StargateError::kNoError,
                            false,
                            &primary_unreadable_slices,
                            &merged_payload);
        LOG(INFO) << "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@";
        LOG(INFO) << "recieved final primary payload size: "
                  << merged_payload->size();
        LOG(INFO) << "num slices in final primary recieved payload: "
                  << merged_payload->size()/slice_size;


        LOG(INFO) << "slice_state_vec_:" << slice_state_vec_.size();
        LOG(INFO) << "remote_ret_slice state size: "
                  << remote_ret_->extent_group_metadata().slices_size();
    }

    // reading done for either modes

    // save it to the file
    SaveResult(merged_payload);

    // populating the extent_id_list before replicating from the remote_ret_
    for (int ii = 0;
        ii < remote_ret_->extent_group_metadata().extents_size(); ii++) {

      extent_id_list_.push_back(
       remote_ret_->extent_group_metadata().extents().Get(ii).extent_id());

      const int32 vdisk_block = extent_id_list_.back().vdisk_block();
      LOG(INFO) << "vdisk_block" <<  vdisk_block;
    }




    // start replication to the target_disk, if set
    if (FLAGS_target_disk != -1) {
        cout << "Inside ReplicateEgroupHelper";
        LOG(INFO) << "Inside ReplicateEgroupHelper";

        ReplicateEgroupHelper(FLAGS_egroup_id,
                                FLAGS_target_disk,
                                slice_state_vec_,
                                &extent_id_list_,
                                managed_by_aes_,
                                remote_ret_, // ret from replica read
                                merged_payload);
        }


    // start fixer op
    // if it goes inside , it exits inside itself
    if (!FLAGS_stargate_ip.empty()) {
        cout << "Inside FixerOpHelper";
        LOG(INFO) << "Inside FixerOpHelper";
        FixerOpHelper(done_cb);
    } else {
        exit(0);
    }
    cout << "exiting from Execute";
    LOG(INFO) << "exiting from Execute";
}

// --------------------------------------------------------------------------

void MergeReplicas::FixerOpHelper(
            const Function<void(ErrorType)>& done_cb) {

    // wrap it in ed_
    if (!ed_->IsWorkerThread()) {
        ed_->Add(func_disabler_.Bind(&MergeReplicas::FixerOpHelper,
                                    this, done_cb));
        return;
    }

    CHECK_GE(FLAGS_egroup_id, 0);
    CHECK(egroup_metadata_) << FLAGS_egroup_id;
    CHECK(egroup_metadata_)
        << FLAGS_egroup_id;

    vdisk_id_ = egroup_metadata_->
                control_block()->owner_vdisk_id();

    // getting the interface in the vdisk setting
    GetStargateInterface(-1 , true /*fetch_for_vdisk*/);

    CHECK(stargate_ifc_) << "Stargate handle not set up correctly";

    shared_ptr<FixExtentGroupsArg> arg = make_shared<FixExtentGroupsArg>();
    arg->set_vdisk_id(vdisk_id_);

    FixExtentGroupsArg::ExtentGroup *egroup = arg->add_extent_groups();
    egroup->set_extent_group_id(FLAGS_egroup_id);



    // optional
    if (!FLAGS_target_storage_tier.empty()) {
        egroup->add_target_storage_tiers(FLAGS_target_storage_tier);
        if (FLAGS_num_replicas_to_migrate > 0) {
        egroup->set_num_replicas_to_migrate(FLAGS_num_replicas_to_migrate);
        }
    }


    if (FLAGS_num_replicas_desired > -1) {
        const int32 num_replicas_desired = FLAGS_num_replicas_desired;
        CHECK_LE(num_replicas_desired, cluster_ft_ + 1)
        << "Cannot set desired RF more than configured cluster RF "
        << OUTVARS(num_replicas_desired, cluster_ft_);
        egroup->set_desired_rf(num_replicas_desired);
    }


    // defining the callback func
    Function<void(StargateError::Type, shared_ptr<string>,
                shared_ptr<FixExtentGroupsRet>)> rpc_done_cb =
    func_disabler_.Bind(&MergeReplicas::FixerOpHelperDone, this,
                        arg, done_cb, _1, _2, _3);


    // call the rpc
    ed_->Wrap(&rpc_done_cb);
    stargate_ifc_->FixExtentGroups(arg, rpc_done_cb);

}

// --------------------------------------------------------------------------


void MergeReplicas::FixerOpHelperDone(
        const shared_ptr<FixExtentGroupsArg>& arg,
        const Function<void(ErrorType)>& done_cb,
        StargateError::Type error,
        shared_ptr<string> error_detail,
        const shared_ptr<FixExtentGroupsRet>& ret) {

    if (error != StargateError::kNoError) {
        LOG(WARNING) << "Stargate FixExtentGroups RPC for vdisk id "
                     << arg->vdisk_id()
                     << " failed with error " << error;

            LOG(WARNING) << "error_detail:- " << error_detail;
    } else {
        LOG(INFO) << "Fix Extent groups RPC for vdisk " << vdisk_id_
                  << " and extent group " << FLAGS_egroup_id
                  << " completed successfully";
        cout << "Fix Extent groups RPC for vdisk " << vdisk_id_
             << " and extent group " << FLAGS_egroup_id
             << " completed successfully" << endl;
    }

    if (done_cb) {
        done_cb(error == StargateError::kNoError?kNoError : kStargateError);
    }
}

// --------------------------------------------------------------------------

// function to invoke replicate rpc and update map 3
void MergeReplicas::ReplicateEgroupHelper(
        const int64 egroup_id,
        const int64 dest_disk_id,
        const vector<shared_ptr<SliceState>>& slice_state_vec,
        const vector<ExtentIdProto> *const extent_id_list,
        const bool managed_by_aes,
        shared_ptr<ReplicaReadEgroupRet> remote_ret,
        const IOBuffer::Ptr& merged_payload,
        const StargateError::Type expected_error) {

    // Get stargate interface associated with the destination disk.
    GetStargateInterface(dest_disk_id);
    CHECK(stargate_ifc_);

    shared_ptr<ReplicateExtentGroupArg> arg =
        make_shared<ReplicateExtentGroupArg>();
    arg->set_extent_group_id(egroup_id);
    arg->set_disk_id(dest_disk_id);
    arg->set_remote_disk_id(-1);
    arg->set_managed_by_aes(managed_by_aes);
    arg->set_qos_principal_name(string());
    arg->set_qos_priority(StargateQosPriority::kDefault);
    arg->set_migration_reason(MedusaExtentGroupIdMapEntryProto::kILM);

    arg->set_intent_sequence(egroup_metadata_->
                            control_block()->latest_intent_sequence() + 1);
    arg->set_owner_container_id(egroup_metadata_->
                            control_block()->owner_container_id());



    if (managed_by_aes) {
        CHECK(extent_id_list);
        // populate the extent_id_list of arg for AES
        for (uint ii = 0; ii < extent_id_list->size(); ++ii) {
        arg->add_extent_id_list()->CopyFrom((*extent_id_list)[ii]);
        }
    } else {
         // populate the slice_state_vec of arg for non-AES
        for (uint ii = 0; ii < slice_state_vec.size(); ++ii) {
            CHECK(slice_state_vec[ii]->has_extent_group_offset());
            slice_state_vec[ii]->CopyTo(arg->add_slices());
        }
    }

    LOG(INFO) << "Replicating egroup " << egroup_id
              << " from given IOBUFF to "
              << dest_disk_id
              << " arg: " << arg->ShortDebugString();



    // fetching mvalue from mvalue_vec_ret_
    CHECK_EQ(mvalue_vec_ret_.size(), 1);
    Medusa::MedusaValue::PtrConst mvalue = move(mvalue_vec_ret_[0]);
    CHECK(mvalue && mvalue->extent_groupid_map_entry);


    // fetching egroup_metadata from mvalue
    const MedusaExtentGroupIdMapEntry *egroup_metadata =
        mvalue->extent_groupid_map_entry.get();
    Configuration::PtrConst config = zeus_connector_->FetchZeusConfig();
    StargateUtil::FetchExtentGroupDiskUsage(
        arg->mutable_disk_usage(), egroup_id, egroup_metadata, config);



    // setting the arg's remote_ret field
    // withthe remote_ret_ that we already have after readop
    ReplicaReadEgroupRet *read_replica_arg = arg->mutable_read_replica_ret();

    read_replica_arg->mutable_extent_group_metadata()->CopyFrom(
        remote_ret->extent_group_metadata());

    read_replica_arg->set_largest_seen_intent_sequence(
        remote_ret->largest_seen_intent_sequence());

    read_replica_arg->set_last_mutator_incarnation_id(
        remote_ret->last_mutator_incarnation_id());

    read_replica_arg->set_checksum_type(
        remote_ret->checksum_type());



    // finding max intent_sequence among existing replicas
    int64 new_intent_sequence = -1;
    for (int ii = 0;
        ii < (egroup_metadata->control_block()->replicas_size()); ++ii) {
            new_intent_sequence = max(egroup_metadata->control_block()
              ->replicas().Get(ii).intent_sequence(), new_intent_sequence);
    }



    // Add dest_disk_id to the egroup metadata in medusa in new_mvalue.
    Medusa::MedusaValue::Ptr new_mvalue = make_shared<Medusa::MedusaValue>();
    new_mvalue->epoch = mvalue->epoch;
    new_mvalue->timestamp = mvalue->timestamp + 1;

    unique_ptr<MedusaExtentGroupIdMapEntryProto::ControlBlock>
        new_control_block =
        make_unique<MedusaExtentGroupIdMapEntryProto::ControlBlock>();
    new_control_block->CopyFrom(*egroup_metadata->control_block());


    arg->set_expected_intent_sequence(new_intent_sequence);
    new_control_block->set_latest_intent_sequence(
        new_control_block->latest_intent_sequence()+1);
    MedusaExtentGroupIdMapEntryProto::Replica *replica =
        new_control_block->add_replicas();
    replica->set_disk_id(dest_disk_id);
    if (new_intent_sequence != -1)
        replica->set_intent_sequence(new_intent_sequence);

    new_mvalue->extent_groupid_map_entry =
        make_shared<MedusaExtentGroupIdMapEntry>(
        move(new_control_block),
        nullptr /* diff_slice_map */, nullptr /* diff_extent_map */,
        egroup_metadata);


    // invoking thereplicate op
    Notification notification;
    const Function<void(StargateError::Type,
            shared_ptr<string>,
            shared_ptr<ReplicateExtentGroupRet>)> replicate_done_cb =
                func_disabler_.Bind(&ReplicateExtentGroupDone,
                    _1, _2, _3, expected_error, &notification);

    stargate_ifc_->ReplicateExtentGroup(arg,
                            replicate_done_cb, merged_payload);
    notification.Wait();

    // Schedule new_mvalue to be written to Medusa.
    vector<int64> egid_vec(1, egroup_id);
    vector<Medusa::MedusaValue::Ptr> mvalue_vec(1, move(new_mvalue));

    notification.Reset();
    const Function<void(shared_ptr<vector<MedusaError::Type> >)>
        update_done_cb = func_disabler_.Bind(&TentativeUpdateCompleted, _1,
                                            &notification);


    medusa_wrapper_->medusa()->UpdateExtentGroupIdMap(
        make_shared<vector<int64>>(move(egid_vec)),
        true /* use_CAS */,
        make_shared<vector<Medusa::MedusaValue::Ptr>>(move(mvalue_vec)),
        update_done_cb);
    notification.Wait();



    LOG(INFO) << "Added disk " << dest_disk_id
              << " in medusa metadata of egroup " << egroup_id
              << " arg: " << arg->ShortDebugString();

}


// --------------------------------------------------------------------------

void MergeReplicas::SaveResult(const IOBuffer::Ptr& iobuf) const {
    if (FLAGS_target_file.empty()) {
        // Output to console.
        string output_str;
        IOBuffer::Accessor ac(iobuf.get());
        ac.CopyToString(iobuf->size(), &output_str);
        cout << output_str << endl;
        return;
    }

    // Output to target_file.

    const int fd = open(FLAGS_target_file.c_str(), O_CREAT | O_RDWR, 0666);
    PCHECK(fd >= 0);

    IOBufferUtil::WriteToFile(iobuf.get(), fd, 0);
    PCHECK(close(fd) == 0);
}

// --------------------------------------------------------------------------

void MergeReplicas::ReadReplicaEgroupHelper(
            int64 egroup_id,
            int64 disk_id,
            vector<SliceState::Ptr>* slice_state_vec,
            int replica_id,
            bool managed_by_aes,
            int64 expected_applied_intent_sequence,
            StargateError::Type expected_error ,
            bool is_ec_egroup,
            vector<int> *unreadable_slices,
            IOBuffer::Ptr *payload_rec,
            bool save_read_replica_ret) {

    // Sort slice state vector in increasing order of extent group offset.
    sort(slice_state_vec->begin(), slice_state_vec->end(),
        SliceState::SliceStatePtrOrderIncreasingFileOffset);

    // Get stargate interface associated with the primary disk.
    GetStargateInterface(disk_id, false /* for vdisk */ ,
        !FLAGS_source_disk_offline /* usehttp:only false in xmountmode*/);

    shared_ptr<ReplicaReadEgroupArg> remote_arg =
        make_shared<ReplicaReadEgroupArg>();
    remote_arg->set_extent_group_id(egroup_id);
    remote_arg->set_disk_id(disk_id);
    remote_arg->set_qos_principal_name(string());
    remote_arg->set_qos_priority(StargateQosPriority::kDefault);
    remote_arg->set_managed_by_aes(managed_by_aes);

    remote_arg->set_expected_intent_sequence(
        managed_by_aes ?
        (expected_applied_intent_sequence >= 0 ?
            expected_applied_intent_sequence : -1) :
            egroup_metadata_->control_block()->replicas()
                        .Get(replica_id).intent_sequence());

    remote_arg->set_global_metadata_intent_sequence(
        egroup_metadata_->control_block()->latest_intent_sequence());

    // add slice_states in the arg
    for (uint ii = 0; ii < slice_state_vec->size(); ++ii) {
        (*slice_state_vec)[ii]->CopyTo(remote_arg->add_slices());
    }

    Notification notification;
    Function<void(StargateError::Type,
                    shared_ptr<string>,
                    shared_ptr<ReplicaReadEgroupRet>,
                    IOBuffer::Ptr&&)> done_cb =

    func_disabler_.Bind(&MergeReplicas::ReadReplicaEgroupHelperDone, this,
                        _1, _2, _3, _4, &notification,
                        *slice_state_vec, expected_error,
                        is_ec_egroup, unreadable_slices,
                        payload_rec, save_read_replica_ret);
    notification.Reset();
    // invoke rpc
    stargate_ifc_->ReplicaReadEgroup(move(remote_arg), move(done_cb),
        30 * 1000);
    notification.Wait();

    if (expected_error != StargateError::kNoError) {
        return;
    }
}

// --------------------------------------------------------------------------

void MergeReplicas::ReadReplicaEgroupHelperDone(StargateError::Type err,
    shared_ptr<string> err_detail,
    shared_ptr<ReplicaReadEgroupRet> remote_ret,
    IOBuffer::Ptr&& payload,
    Notification *notify,
    vector<SliceState::Ptr> slice_state_vec,
    const StargateError::Type expected_error,
    const bool is_ec_egroup,
    vector<int> *unreadable_slices,
    IOBuffer::Ptr *payload_rec,
    bool save_read_replica_ret) {

        CHECK_EQ(err, expected_error);
        if (err != StargateError::kNoError) {
            if (notify) {
            notify->Notify();
            }
            return;
        }

        DCHECK_EQ(is_ec_egroup, remote_ret->is_erasure_coded());
        const int slice_size = 32 * 1024;


        // reporting the unreadable slices
        if (remote_ret->corrupt_slices_size()) {
            LOG(INFO) << "Corrupt slices : ";
            for (int ii = 0; ii < remote_ret->corrupt_slices_size(); ++ii) {
                LOG(INFO) << remote_ret->corrupt_slices().Get(ii) << " ";
                unreadable_slices->push_back(
                        remote_ret->corrupt_slices().Get(ii));
            }

        }


        CHECK_GT(payload->size(), 0);
        CHECK_EQ(payload->size() % slice_size, 0) << payload->size();


        LOG(INFO) << "In ReplicaReadEgroupVerifyDone";
        if (unreadable_slices)
            LOG(INFO) << "unreadable_size:" << unreadable_slices->size();


        // payload returning
        *payload_rec = move(payload);

        // saves the remote_ret to be used while replicating
        if (save_read_replica_ret)
            remote_ret_ = remote_ret;

        LOG(INFO) << remote_ret->ShortDebugString();

        if (notify) {
            notify->Notify();
        }
}

// --------------------------------------------------------------------------


void MergeReplicas::LookupExtentGroupIdMapDone(
    MedusaError::Type error,
    shared_ptr<vector<MedusaValue::PtrConst>> value_vec,
    Notification *notification) {

    if (error == MedusaError::kNoError &&
        value_vec->size() == 1 &&
        value_vec->at(0)->timestamp >= 0) {
        egroup_metadata_ = value_vec->at(0)->extent_groupid_map_entry;
    }

    // put medusa values(only one would be there)
    // to the mvalue_vec_ret_
    for (uint ii = 0; ii < value_vec->size(); ++ii) {
        mvalue_vec_ret_.emplace_back((*value_vec)[ii]);
    }

    notification->Notify();
}

// --------------------------------------------------------------------------

void MergeReplicas::GetStargateInterface(int64 disk_id,
                                            bool fetch_for_vdisk,
                                            bool use_http) {
    string stargate_leader_handle;



    if (fetch_for_vdisk) {
        // fetches vdisk_associated with vdisk_id_
        DCHECK(ed_->IsWorkerThread());
        const string vdisk_leadership_name =
                        Zeus::VDiskLeadershipName(vdisk_id_);

        string leader_handle = zeus_connector_->FetchLeader(
                vdisk_leadership_name);

        if (leader_handle.empty()) {
            stargate_leader_handle = StringJoin(FLAGS_stargate_ip, ":",
                                    port_);
            LOG(INFO) << "VDisk " << vdisk_leadership_name
                      << " is not hosted. Using user provided stargate handle "
                      << stargate_leader_handle <<  " to send Stargate RPCs";
        } else {
        // 'leader_handle' is set by Stargate.Vdisk leaderhandle may contain
        // issci port info in it. So split the handle, extract first field
        // as Stargate IP and construct new handle (IP::PORT).
            vector<string> tokens;
            boost::split(tokens, leader_handle, boost::is_any_of(":"));
            CHECK_GT(tokens.size(), 0U);
            stargate_leader_handle = StringJoin(tokens[0], ":", port_);
            LOG(INFO) << "VDisk " << vdisk_leadership_name
                      << " is hosted by stargate "
                      << stargate_leader_handle;
            cout << "VDisk " << vdisk_leadership_name
                 << " is hosted by stargate "
                 << stargate_leader_handle << endl;
        }
    } else {
        // fetches vdisk_associated with disk_id
        Configuration::PtrConst config =
                    zeus_connector_->FetchZeusConfig();

        const ConfigurationProto::Disk *disk = config->LookupDisk(disk_id);
        LOG(INFO) << "disk->service_vm_id()" << disk->service_vm_id();
        CHECK(disk);

        const ConfigurationProto::Node *node =
                    config->LookupNode(disk->service_vm_id());

        if (node) {
            // Not offline disk
            LOG(INFO) << "node->service_vm_external_ip()"
                      << node->service_vm_external_ip();

            CHECK_NE(node->service_vm_external_ip(), "");
            stargate_leader_handle =
                StringJoin(node->service_vm_external_ip(), ":", port_);
        } else {
            // offline disk , so use xmount port
            // look at the last_service_vm_id instead of service_vm_id
            node = config->LookupNode(disk->last_service_vm_id());
            if (node) {
                // look at the last_service_vm_id instead of
                CHECK_NE(node->service_vm_external_ip(), "");
                stargate_leader_handle =
                    StringJoin(node->service_vm_external_ip(), ":",
                                FLAGS_stargate_xmount_port);
            } else {
                // use the flag at last
                stargate_leader_handle =
                    StringJoin(FLAGS_external_ip_for_offline, ":",
                            FLAGS_stargate_xmount_port);
            }

        }
    }

    LOG(INFO) << "stargate_leader_handle" << stargate_leader_handle;

    if (!rpc_ed_) {
        rpc_ed_ = make_shared<EventDriver>(1);
    }
    CHECK(rpc_ed_);

    if (use_http) {

        // standard mode
        stargate_ifc_ = StargateInterface::Create(
            rpc_ed_, stargate_leader_handle);
    } else {
        // xmount mode constructor of stargate interface
        epd_ = make_shared<EpollDriver>("xmount_epoll");
        CHECK(epd_);
        delay_ed_ = make_shared<EventDriver>(0, "xmount_ed");
        CHECK(delay_ed_);

        stargate_ifc_ = StargateInterface::Create(
            delay_ed_, epd_, false /* use_http */, stargate_leader_handle);

        LOG(INFO) << "interface created in non-http mode";
        cout << "interface created in non-http mode";
    }
    CHECK(stargate_ifc_);
    LOG(INFO) << "Created RPC interface for stargate at "
              << stargate_leader_handle;


}
// ---------------------------------------------------------------------------

} } } // namespace



using namespace nutanix::tools::stargate;

// function for fixer op to exit after end
void ExitWithResult(MergeReplicas::ErrorType error) {
    exit(error == MergeReplicas::kNoError ? 0 : 1);
}

// Main starts here
int main(int argc, char *argv[]) {
    LOG(INFO) << "main started";
    InitNutanix(&argc, &argv, InitOptions(true) /* default options */);

    Function<void(MergeReplicas::ErrorType)> exit_func =
    bind(&ExitWithResult, _1);

    shared_ptr<EventDriver> ed = make_shared<EventDriver>();

    MergeReplicas::Ptr obj = make_shared<MergeReplicas>(ed);
    obj->Validate();
    obj->Execute(exit_func);
    LOG(INFO) <<  "main complete";

    ed->WaitForEvents();
    return 0;

}
