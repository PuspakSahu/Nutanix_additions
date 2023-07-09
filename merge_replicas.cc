/* 
Copyright (c) 2023 Nutanix Inc. All rights reserved.

This tool helps to recover an egroup with partially unreadable replicas,
it merges the payloads and then 

    - saves it in a file as specified
    - replicates to a healthy disk with the merged payload
    - calls fixer op which internally deletes the unreadable replicas

Author: puspak.sahu@nutanix.com

usage: 

merge_replicas -egroup_id=216
               [-target_file]                  ----- if specified saves the payload to the destination flag
               [-target_disk]                  ----- if set , replicated into the target disk
               [-stargate_ip]                  ----- if set , IP address of stargate to which fixer
                                                     RPC will be sent if the vdisk is not hosted at any stargate
               [-num_replicas_desired]         ----- if set , fixer op will keep those many replicas finally 
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

#include "content_cache/stargate_crypto_util.h"
#include "mantle/mantle.h"
#include "medusa/medusa.h"
#include "medusa/util/medusa_wrapper.h"
#include "cdp/client/stargate/stargate_base/stargate_util.h"
#include "util/base/adler32.h"
#include "util/base/data_transformation_type.h"
#include "util/base/iobuffer.h"
#include "util/base/iobuffer_util.h"
#include "util/base/sha1.h"
#include "util/base/string_util.h"
#include "util/base/walltime.h"
#include "util/misc/init_nutanix.h"
#include "util/thread/notification.h"
#include "tools/util/connectors/ntnxdb/zeus_connector.h"
#include "cdp/client/stargate/stargate_base/stargate_error.h"
#include "cdp/client/stargate/stargate_interface/stargate_interface.h"
#include "cdp/server/stargate/xmount/xmount.h"

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

DEFINE_string(stargate_ip, "127.0.0.1",
              "IP address of stargate to which fixer op RPC will be "
              "sent if the vdisk is not hosted at any stargate.");

DEFINE_int64(egroup_id, -1,
             "Egroup to operate on. This field is required only for decrypt "
             "operation.");

DEFINE_string(target_file, "",
              "Full path of the file where the output must be saved. If not"
              "specified the tool will autogenerate a file name and print it "
              "as output.");

DEFINE_int64(target_disk, -1, 
             "target disk is set on if we want to operform replicate egroup");

             
DEFINE_bool(source_disk_offline, false, 
             "source_disk_offline");

DEFINE_int64(num_replicas_desired, -1, 
             "num_replicas_desired");

///Not used currently
             
DEFINE_string(target_storage_tier, "", 
             "target_storage_tier");


DEFINE_int64(num_replicas_to_migrate, -1, 
             "num_replicas_to_migrate");

//For reading from offline disk
DEFINE_int64(mode,2,
            "mode 1 : reading both replicas"
            "mode 2 : reading from offline disk");

DEFINE_int64(offline_disk_id,-1,
            "disk_id in mode 2");
//--------------------------------------------------------


DECLARE_int32(stargate_port);

DECLARE_int32(stargate_xmount_port);

// DEFINE_int64(simulate_unreadability, false,
//              "simulate_unreadability");

// DECLARE_bool(make_the_slices_corrupt);
// DECLARE_string(list_of_corrupt_slice_ids);

namespace nutanix { namespace tools { namespace stargate {

    class MergeReplicas {

        public:

            enum ErrorType {
                    kNoError,
                    kMedusaError,
                    kStargateError,
                    kVDiskConfigError
                };

            typedef shared_ptr<MergeReplicas> Ptr;

            MergeReplicas(EventDriver::Ptr ed);

            ~MergeReplicas();

            void Validate();

            void Execute(
                const Function<void(ErrorType)>& done_cb
                );

            vector<SliceState::Ptr> slice_state_vec;

            

            
        private:
            //void GetInfo();
            //void MergeReplicas::read_replica();
            vector<Medusa::MedusaValue::PtrConst> mvalue_vec_ret_;

            void SaveResult(const IOBuffer::Ptr& iobuf) const;

            MedusaExtentGroupIdMapEntry::PtrConst egroup_metadata_;

            MedusaWrapper::Ptr medusa_wrapper_;

            ZeusConnector::Ptr zeus_connector_;

            StargateInterface::Ptr stargate_ifc_;

            int32 port_;

            EventDriver::Ptr rpc_ed_;

            EventDriver::Ptr ed_;

            vector<int> disk_ids_;

            ReplicaReadEgroupRet *remote_ret_;

            thread::FunctionDisabler func_disabler_;

            int64 vdisk_id_;

            int64 cluster_ft_;
            
            void GetStargateInterface(int64 disk_id, bool fetch_for_vdisk=false);

            void ReadReplicaEgroupHelper(
                int64 egroup_id,
                int64 disk_id,
                vector<SliceState::Ptr>* slice_state_vec,
                int replica_id,
                bool managed_by_aes,
                int64 expected_applied_intent_sequence,
                StargateError::Type expected_error,
                bool is_ec_egroup ,
                vector<int> *unreadable_slices,
                IOBuffer::Ptr *payload_rec
                );
            void ReadReplicaEgroupHelperDone(StargateError::Type err,
                shared_ptr<string> err_detail,
                shared_ptr<ReplicaReadEgroupRet> remote_ret,
                IOBuffer::Ptr&& payload,
                Notification *notify,
                vector<SliceState::Ptr> slice_state_vec,
                const StargateError::Type expected_error,
                const bool is_ec_egroup,
                vector<int> *unreadable_slices,
                IOBuffer::Ptr *payload_rec);

            void FixerOpHelper(
                const Function<void(ErrorType)>& done_cb
                );

            void FixerOpHelperDone(
                const shared_ptr<FixExtentGroupsArg>& arg,
                const Function<void(ErrorType)>& done_cb,
                StargateError::Type error,
                shared_ptr<string> error_detail,
                const shared_ptr<FixExtentGroupsRet>& ret);


            void ReplicateEgroupHelper(
                const int64 egroup_id,
                const int64 dest_disk_id,
                const vector<shared_ptr<SliceState>>& slice_state_vec,
                ReplicaReadEgroupRet *remote_ret,
                IOBuffer::Ptr& merged_payload,
                const StargateError::Type expected_error=StargateError::kNoError,
                Notification *const user_notification=nullptr );

            static void TentativeUpdateCompleted(
                shared_ptr<vector<MedusaError::Type> > err_vec,
                Notification *notify) {
                if (notify) {
                    notify->Notify();
                }
            }

            static void ReplicateExtentGroupDone(StargateError::Type err,
                                        shared_ptr<string> err_detail,
                                        shared_ptr<ReplicateExtentGroupRet> ret,
                                        const StargateError::Type expected_error,
                                        Notification *notify) {
                CHECK_EQ(err, expected_error);
                LOG(INFO)<<"Error while replicating:"<<err;
                LOG(INFO)<<"replication done******* ";
                
                if (notify) {
                    notify->Notify();
                }
            }

            void LookupExtentGroupIdMapDone(
                MedusaError::Type error,
                shared_ptr<vector<MedusaValue::PtrConst>> value_vec,
                Notification *notification);

    };

    MergeReplicas::MergeReplicas(EventDriver::Ptr ed): 
                                    
                                    egroup_metadata_(nullptr),
                                    medusa_wrapper_(make_shared<MedusaWrapper>()),
                                    zeus_connector_(make_shared<ZeusConnector>()),
                                    ed_(ed)
                                     {}

    MergeReplicas::~MergeReplicas(){
        LOG(INFO)<<"destructor called";
    }



    void MergeReplicas::Validate(){
        Configuration::PtrConst zeus_config = zeus_connector_->FetchZeusConfig();
        cluster_ft_ = StargateUtil::DefaultClusterMaxFaultTolerance(*zeus_config);
  
        LOG(INFO)<<"Validate started";
        
        if(FLAGS_source_disk_offline)
            port_=FLAGS_stargate_xmount_port;
        else
            port_=FLAGS_stargate_port;


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


        LOG(INFO)<<"Validate complete";
        //GetInfo();
    }

    void MergeReplicas::Execute(
        const Function<void(ErrorType)>& done_cb
    ){
        LOG(INFO)<<"Totalreplicas : " <<egroup_metadata_->control_block()->replicas_size();
        LOG(INFO)<<"Replica disk_id list: "; 
        for(int ii=0;ii <egroup_metadata_->control_block()->replicas_size();++ii){
            LOG(INFO)<<egroup_metadata_->control_block()->replicas().Get(ii).disk_id();
            disk_ids_.push_back(egroup_metadata_->control_block()->replicas().Get(ii).disk_id());
        }
        
        ScopedArenaMark am;
        MedusaExtentGroupIdMapEntry::SliceIterator::Ptr slice_iter =
        make_shared<MedusaExtentGroupIdMapEntry::SliceIterator>(
        egroup_metadata_.get(), am.arena());

        for (/* initialization above */; !slice_iter->End(); slice_iter->Next()) {
            slice_state_vec.push_back(move(SliceState::Create(
            *(slice_iter->Get()).second, am.arena())));

            const int32 extent_group_offset = slice_state_vec.back()->extent_group_offset();
            LOG(INFO) << slice_state_vec.back()->slice_id() << "with offset"<< extent_group_offset;
        }

        vector<int> primary_unreadable_slices;
        vector<int> sec_unreadable_slices;
        IOBuffer::Ptr primary_payload = make_shared<IOBuffer>();
        IOBuffer::Ptr sec_payload = make_shared<IOBuffer>();
        IOBuffer::Ptr merged_payload = make_shared<IOBuffer>();

        if(FLAGS_mode==1){
            LOG(INFO)<<"Mode 1: reading from both rpelcias";
            ReadReplicaEgroupHelper(FLAGS_egroup_id,
                                disk_ids_[0],
                                &slice_state_vec,
                                0,
                                false,
                                -1,
                                StargateError::kNoError,
                                false,
                                &primary_unreadable_slices,
                                &primary_payload);
            ReadReplicaEgroupHelper(FLAGS_egroup_id,
                                disk_ids_[1],
                                &slice_state_vec,
                                1,
                                false,
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
            if(primary_unreadable_slices.size())
                LOG(INFO)<<"num corrupted slices in final primary recieved payload _: "
            <<primary_unreadable_slices.size()<<endl<<endl;
            

            LOG(INFO)<<"recieved final sec payload size _: "<<sec_payload->size();
            LOG(INFO)<<"num slices in final sec recieved payload _: "<<sec_payload->size()/slice_size;
            if(sec_unreadable_slices.size())
                LOG(INFO)<<"num corrupted slices in final sec recieved payload _: "<<sec_unreadable_slices.size();
            
            LOG(INFO)<<"@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@";


            //Merging the final payload


            
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

        }
        else{
            CHECK_GE(FLAGS_offline_disk_id,0);
            LOG(INFO)<<"Mode 2: reading from offline replcia";
            ReadReplicaEgroupHelper(FLAGS_egroup_id,
                                FLAGS_offline_disk_id,
                                &slice_state_vec,
                                0,
                                false,
                                -1,
                                StargateError::kNoError,
                                false,
                                &primary_unreadable_slices,
                                &merged_payload);

            const int slice_size = 32 * 1024;
            LOG(INFO)<<"@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@";
            LOG(INFO)<<"recieved final primary payload size _: "<<merged_payload->size();
            LOG(INFO)<<"num slices in final primary recieved payload _: "<<merged_payload->size()/slice_size;
            if(primary_unreadable_slices.size())
                LOG(INFO)<<"num corrupted slices in final primary recieved payload _: "
            <<primary_unreadable_slices.size()<<endl<<endl;

        }

        SaveResult(merged_payload);


        if (FLAGS_target_disk!=-1){
            cout<<"Inside ReplicateEgroupHelper";
            LOG(INFO)<<"Inside ReplicateEgroupHelper";
            
            ReplicateEgroupHelper(FLAGS_egroup_id,
                                 FLAGS_target_disk,
                                 slice_state_vec,
                                 remote_ret_,
                                 merged_payload
                                 );
            }
        if(!FLAGS_stargate_ip.empty())
        {   
            cout<<"Inside FixerOpHelper";
            LOG(INFO)<<"Inside FixerOpHelper";
            FixerOpHelper(done_cb);
        }
        else{
            cout<<"exiting from Execute";
            LOG(INFO)<<"exiting from Execute";
            exit(0);
        }
        cout<<"exiting from Execute";
        LOG(INFO)<<"exiting from Execute";
        
    }


    void MergeReplicas::FixerOpHelper(
        const Function<void(ErrorType)>& done_cb
        ){

            if (!ed_->IsWorkerThread()) {
                ed_->Add(func_disabler_.Bind(&MergeReplicas::FixerOpHelper,
                                            this,done_cb));
                return;
            }

            CHECK_GE(FLAGS_egroup_id, 0);
            CHECK(egroup_metadata_) << FLAGS_egroup_id;
            CHECK(egroup_metadata_)
                << FLAGS_egroup_id;

            vdisk_id_=egroup_metadata_->
                        control_block()->owner_vdisk_id();
            
            GetStargateInterface(-1 ,true /*fetch_for_vdisk*/ );
            CHECK(stargate_ifc_) << "Stargate handle not set up correctly";

            shared_ptr<FixExtentGroupsArg> arg = make_shared<FixExtentGroupsArg>();
            arg->set_vdisk_id(vdisk_id_);

            FixExtentGroupsArg::ExtentGroup *egroup = arg->add_extent_groups();
            egroup->set_extent_group_id(FLAGS_egroup_id);



            //optional
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
            


            Function<void(StargateError::Type, shared_ptr<string>,
                        shared_ptr<FixExtentGroupsRet>)> rpc_done_cb =
            func_disabler_.Bind(&MergeReplicas::FixerOpHelperDone, this,
                                arg,done_cb,_1,_2,_3);


            
            ed_->Wrap(&rpc_done_cb);
            stargate_ifc_->FixExtentGroups(arg, rpc_done_cb);



        }
        
    void MergeReplicas::FixerOpHelperDone(
        const shared_ptr<FixExtentGroupsArg>& arg,
        const Function<void(ErrorType)>& done_cb,
        StargateError::Type error,
        shared_ptr<string> error_detail,
        const shared_ptr<FixExtentGroupsRet>& ret){

            if (error != StargateError::kNoError) {
                LOG(WARNING) << "Stargate FixExtentGroups RPC for vdisk id "
                 << arg->vdisk_id() << " failed with error " << error;

                 LOG(WARNING)<<"error_detail:- "<<error_detail;
            }
            else {
                LOG(INFO) << "Fix Extent groups RPC for vdisk " << vdisk_id_
                        << " and extent group " << FLAGS_egroup_id
                        << " completed successfully";
                cout << "Fix Extent groups RPC for vdisk " << vdisk_id_
                    << " and extent group " << FLAGS_egroup_id
                    << " completed successfully" << endl;
            }

            if (done_cb) {
                done_cb(error == StargateError::kNoError ? kNoError : kStargateError);
            }
        }





    void MergeReplicas::ReplicateEgroupHelper(
    const int64 egroup_id,
    const int64 dest_disk_id,
    const vector<shared_ptr<SliceState>>& slice_state_vec,
    ReplicaReadEgroupRet *remote_ret,
    IOBuffer::Ptr& merged_payload,
    const StargateError::Type expected_error,
    Notification *const user_notification) {

        // Get stargate interface associated with the destination disk.
        GetStargateInterface(dest_disk_id);
        CHECK(stargate_ifc_);

        shared_ptr<ReplicateExtentGroupArg> arg =
            make_shared<ReplicateExtentGroupArg>();
        arg->set_extent_group_id(egroup_id);
        arg->set_disk_id(dest_disk_id);
        arg->set_remote_disk_id(-1);
        arg->set_qos_principal_name(string());
        arg->set_qos_priority(StargateQosPriority::kDefault);
        arg->set_migration_reason(MedusaExtentGroupIdMapEntryProto::kILM);

        arg->set_intent_sequence(egroup_metadata_->
                                control_block()->latest_intent_sequence() + 1);
        arg->set_owner_container_id(egroup_metadata_->
                                control_block()->owner_container_id());

        for (uint ii = 0; ii < slice_state_vec.size(); ++ii) {
        CHECK(slice_state_vec[ii]->has_extent_group_offset());
        slice_state_vec[ii]->CopyTo(arg->add_slices());
        }

        LOG(INFO) << "Replicating egroup " << egroup_id
                    << " from given IOBUFF to "
                    << dest_disk_id
                    << " arg: " << arg->ShortDebugString();


        ReplicaReadEgroupRet *read_replica_arg = arg->mutable_read_replica_ret();
        read_replica_arg->mutable_extent_group_metadata()->CopyFrom(
            remote_ret->extent_group_metadata());


        // Save a copy to the configuration as recommended in stargate.h.
        Configuration::PtrConst config = zeus_connector_->FetchZeusConfig();
       
       
        // Read the metadata of the extent group from Medusa's extent group id map.
        //shared_ptr<vector<int64>> extent_group_id_vec = make_shared<vector<int64>>();
        // extent_group_id_vec->push_back(egroup_id);
        Notification notification;
        // const Function<void(MedusaError::Type,
        //                     shared_ptr<vector<Medusa::MedusaValue::PtrConst>>)>
        //     done_cb =
        //     func_disabler_.Bind(&LookupExtentGroupIdMapCompleted,
        //                         _1, _2, &notification);
        // cluster_mgr_->medusa()->LookupExtentGroupIdMap(extent_group_id_vec,
        //                                                 false /* cached_ok */,
        //                                                 done_cb);
        // notification.Wait();
        
        
        CHECK_EQ(mvalue_vec_ret_.size(), 1);
        Medusa::MedusaValue::PtrConst mvalue = move(mvalue_vec_ret_[0]);
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


        //finding max intent_sequence among existing replicas
        int64 new_intent_sequence=-1;
        for(int ii=0;ii< (egroup_metadata->control_block()->replicas_size());++ii){
        new_intent_sequence=
            max(egroup_metadata->control_block()->replicas().Get(ii).intent_sequence()
                ,new_intent_sequence);
        }

        arg->set_expected_intent_sequence(new_intent_sequence);
        new_control_block->set_latest_intent_sequence(new_control_block->latest_intent_sequence()+1);
        MedusaExtentGroupIdMapEntryProto::Replica *replica =
            new_control_block->add_replicas();
        replica->set_disk_id(dest_disk_id);
        replica->set_intent_sequence(new_intent_sequence);

        new_mvalue->extent_groupid_map_entry =
            make_shared<MedusaExtentGroupIdMapEntry>(
            move(new_control_block),
            nullptr /* diff_slice_map */, nullptr /* diff_extent_map */,
            egroup_metadata);


        //replicate op

        const Function<void(StargateError::Type,
                            shared_ptr<string>,
                            shared_ptr<ReplicateExtentGroupRet>)> replicate_done_cb =
            func_disabler_.Bind(&ReplicateExtentGroupDone, _1, _2, _3, expected_error,
                                (user_notification ?
                                user_notification : &notification));
        stargate_ifc_->ReplicateExtentGroup(arg, replicate_done_cb,merged_payload);


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

        notification.Reset();

        
        if (!user_notification) {
            notification.Wait();
        }
    }


    





    void MergeReplicas::SaveResult(const IOBuffer::Ptr& iobuf) const {
        if (FLAGS_target_file.empty()) {
            // Output to console.
            string output_str;
            IOBuffer::Accessor ac(iobuf.get());
            ac.CopyToString(iobuf->size(), &output_str);
            cout << output_str << endl;
            return;
        }

        // Output to file.
        
        const int fd = open(FLAGS_target_file.c_str(), O_CREAT | O_RDWR, 0666);
        PCHECK(fd >= 0);

        IOBufferUtil::WriteToFile(iobuf.get(), fd, 0);
        PCHECK(close(fd) == 0);
        }






    void MergeReplicas::ReadReplicaEgroupHelper(
                int64 egroup_id,
                int64 disk_id,
                vector<SliceState::Ptr>* slice_state_vec,
                int replica_id,
                bool managed_by_aes = false,
                int64 expected_applied_intent_sequence = -1,
                StargateError::Type expected_error = StargateError::kNoError,
                bool is_ec_egroup = false,
                vector<int> *unreadable_slices=nullptr,
                IOBuffer::Ptr *payload_rec=nullptr
                ){


                     // Sort slice state vector in increasing order of extent group offset.
                    sort(slice_state_vec->begin(), slice_state_vec->end(),
                        SliceState::SliceStatePtrOrderIncreasingFileOffset);

                    // Get stargate interface associated with the primary disk.
                    GetStargateInterface(disk_id);

                    //map<int64, vector<uint>> slice_id_cksum_map;
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
                            expected_applied_intent_sequence : -1) : egroup_metadata_->control_block()->replicas().Get(replica_id).intent_sequence());
                    remote_arg->set_global_metadata_intent_sequence(egroup_metadata_->control_block()->latest_intent_sequence());
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
                                            payload_rec
                                            );
                    notification.Reset();
                    stargate_ifc_->ReplicaReadEgroup(move(remote_arg), move(done_cb),
                        30 * 1000);
                    notification.Wait();

                    if (expected_error != StargateError::kNoError) {
                        return;
                    }

                    //checking the recieved payload
                    const int slice_size = 32 * 1024;

                    LOG(INFO)<<"recieved payload size _: "<<(*payload_rec)->size();
                    LOG(INFO)<<"num slices in recieved payload _: "<<(*payload_rec)->size()/slice_size;
                    if(unreadable_slices)
                        LOG(INFO)<<"unreadable_size:"<<unreadable_slices->size();
                    
                }
    void MergeReplicas::ReadReplicaEgroupHelperDone(StargateError::Type err,
        shared_ptr<string> err_detail,
        shared_ptr<ReplicaReadEgroupRet> remote_ret,
        IOBuffer::Ptr&& payload,
        Notification *notify,
        vector<SliceState::Ptr> slice_state_vec,
        const StargateError::Type expected_error,
        const bool is_ec_egroup,
        vector<int> *unreadable_slices,
        IOBuffer::Ptr *payload_rec){

            CHECK_EQ(err, expected_error);
            if (err != StargateError::kNoError) {
                if (notify) {
                notify->Notify();
                }
                return;
            }

            DCHECK_EQ(is_ec_egroup, remote_ret->is_erasure_coded());
            const int slice_size = 32 * 1024;


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

            CHECK_GT(payload->size(), 0);
            CHECK_EQ(payload->size() % slice_size, 0) << payload->size();
            //const int num_slices = payload->size() / slice_size;


            LOG(INFO)<<"In ReplicaReadEgroupVerifyDone";
            if(unreadable_slices)
                LOG(INFO)<<"unreadable_size:"<<unreadable_slices->size();

            
            *payload_rec=move(payload);
            remote_ret_=remote_ret.get();

            if (notify) {
                notify->Notify();
            }
    }


    void MergeReplicas::LookupExtentGroupIdMapDone(
        MedusaError::Type error,
        shared_ptr<vector<MedusaValue::PtrConst>> value_vec,
        Notification *notification) {

        if (error == MedusaError::kNoError &&
            value_vec->size() == 1 &&
            value_vec->at(0)->timestamp >= 0) {
            egroup_metadata_ = value_vec->at(0)->extent_groupid_map_entry;
        }

        for (uint ii = 0; ii < value_vec->size(); ++ii) {
            mvalue_vec_ret_.emplace_back((*value_vec)[ii]);
        }

        notification->Notify();
    }

    void MergeReplicas::GetStargateInterface(int64 disk_id, bool fetch_for_vdisk){
        //if(stargate_ifc_) return;
        string stargate_leader_handle;
        if(fetch_for_vdisk){
            DCHECK(ed_->IsWorkerThread());
            const string vdisk_leadership_name = Zeus::VDiskLeadershipName(vdisk_id_);
            
            string leader_handle = zeus_connector_->FetchLeader(
                    vdisk_leadership_name);
            
            if (leader_handle.empty()) {

                stargate_leader_handle = StringJoin(FLAGS_stargate_ip, ":",
                                        port_);
                LOG(INFO) << "VDisk " << vdisk_leadership_name << " is not hosted. Using "
              << "user provided stargate handle " << stargate_leader_handle
              << " to send Stargate RPCs";
            } else {
                // 'leader_handle' is set by Stargate. Vdisk leader handle may contain
                // issci port information in it. So split the handle, extract first field
                // as Stargate IP and construct new handle (IP::PORT).
                vector<string> tokens;
                boost::split(tokens, leader_handle, boost::is_any_of(":"));
                CHECK_GT(tokens.size(), 0U);
                stargate_leader_handle = StringJoin(tokens[0], ":", port_);
                LOG(INFO) << "VDisk " << vdisk_leadership_name << " is hosted by stargate "
                        << stargate_leader_handle;
                cout << "VDisk " << vdisk_leadership_name << " is hosted by stargate "
                    << stargate_leader_handle << endl;
            }
        } 
        else{
            Configuration::PtrConst config = zeus_connector_->FetchZeusConfig();
        
            const ConfigurationProto::Disk *disk=config->LookupDisk(disk_id);
            LOG(INFO)<<"disk->service_vm_id()"<<disk->service_vm_id();
            
            const ConfigurationProto::Node *node=config->LookupNode(disk->service_vm_id());

            LOG(INFO)<<"node->service_vm_external_ip()"<<node->service_vm_external_ip();

            stargate_leader_handle = 
                    StringJoin(node->service_vm_external_ip(), ":", port_);


        }
        
        LOG(INFO)<<"stargate_leader_handle"<<stargate_leader_handle;

        if (!rpc_ed_) {
            rpc_ed_ = make_shared<EventDriver>(1);
        }
        CHECK(rpc_ed_);

        stargate_ifc_ = StargateInterface::Create(
            rpc_ed_, stargate_leader_handle);
        CHECK(stargate_ifc_);
        LOG(INFO) << "Created RPC interface for stargate at "
                    << stargate_leader_handle;


    }

    

    


}}}



using namespace nutanix::tools::stargate;

void ExitWithResult(MergeReplicas::ErrorType error) {
        exit(error == MergeReplicas::kNoError ? 0 : 1);
    }

int main(int argc, char *argv[]) {
  LOG(INFO)<<"main started";
  InitNutanix(&argc, &argv, InitOptions(true) /* default options */);

  Function<void(MergeReplicas::ErrorType)> exit_func =
    bind(&ExitWithResult, _1);

  shared_ptr<EventDriver> ed = make_shared<EventDriver>();

  MergeReplicas::Ptr obj = make_shared<MergeReplicas>(ed);
  obj->Validate();
  obj->Execute(exit_func);
  LOG(INFO)<<"main complete";

  ed->WaitForEvents();
  return 0;

}
