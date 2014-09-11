/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/platform/basic.h"

#include "mongo/db/repl/heartbeat_response_action.h"
#include "mongo/db/repl/member_heartbeat_data.h"
#include "mongo/db/repl/repl_set_heartbeat_args.h"
#include "mongo/db/repl/repl_set_heartbeat_response.h"
#include "mongo/db/repl/topology_coordinator_impl.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/net/hostandport.h"
#include "mongo/util/time_support.h"

#define ASSERT_NO_ACTION(EXPRESSION) \
    ASSERT_EQUALS(mongo::repl::HeartbeatResponseAction::NoAction, (EXPRESSION))

namespace mongo {
namespace repl {
namespace {

    bool stringContains(const std::string &haystack, const std::string& needle) {
        return haystack.find(needle) != std::string::npos;
    }

    class TopoCoordTest : public mongo::unittest::Test {
    public:
        virtual void setUp() {
            _topo.reset(new TopologyCoordinatorImpl(Seconds(100)));
            _now = 0;
            _selfIndex = -1;
            _cbData.reset(new ReplicationExecutor::CallbackData(
                        NULL, ReplicationExecutor::CallbackHandle(), Status::OK()));
        }

        virtual void tearDown() {
            _topo.reset(NULL);
            _cbData.reset(NULL);
        }

    protected:
        TopologyCoordinatorImpl& getTopoCoord() {return *_topo;}
        ReplicationExecutor::CallbackData cbData() {return *_cbData;}
        Date_t& now() {return _now;}

        int64_t countLogLinesContaining(const std::string& needle) {
            return std::count_if(getCapturedLogMessages().begin(),
                                 getCapturedLogMessages().end(),
                                 stdx::bind(stringContains,
                                            stdx::placeholders::_1,
                                            needle));
        }

        void makeSelfPrimary(const OpTime& electionOpTime = OpTime(0,0)) {
            setSelfMemberState(MemberState::RS_PRIMARY);
            getTopoCoord()._setCurrentPrimaryForTest(_selfIndex);
            getTopoCoord()._setElectionTime(electionOpTime);
        }

        void setSelfMemberState(const MemberState& newState) {
            getTopoCoord().changeMemberState_forTest(newState);
        }

        // Update config and set selfIndex
        // If "now" is passed in, set _now to now+1
        void updateConfig(BSONObj cfg, int selfIndex, Date_t now = Date_t(-1)) {
            ReplicaSetConfig config;
            ASSERT_OK(config.initialize(cfg));
            ASSERT_OK(config.validate());

            _selfIndex = selfIndex;

            if (now == Date_t(-1)) {
                getTopoCoord().updateConfig(config, selfIndex, _now++, OpTime(0,0));
            }
            else {
                invariant(now > _now);
                getTopoCoord().updateConfig(config, selfIndex, now, OpTime(0,0));
                _now = now + 1;
            }
        }

        HeartbeatResponseAction downMember(const HostAndPort& member, const std::string& setName) {
            StatusWith<ReplSetHeartbeatResponse> hbResponse =
                    StatusWith<ReplSetHeartbeatResponse>(Status(ErrorCodes::HostUnreachable, ""));
            getTopoCoord().prepareHeartbeatRequest(_now++, setName, member);
            return getTopoCoord().processHeartbeatResponse(_now++,
                                                           Milliseconds(0),
                                                           member,
                                                           hbResponse,
                                                           OpTime(0,0));
        }

        void heartbeatFromMember(const HostAndPort& member,
                                 const std::string& setName,
                                 MemberState memberState,
                                 OpTime opTime,
                                 Milliseconds roundTripTime = Milliseconds(0)) {
            ReplSetHeartbeatResponse hb;
            hb.initialize(BSON("ok" << 1 <<
                               "v" << 1 <<
                               "state" << memberState.s));
            hb.setOpTime(opTime);
            StatusWith<ReplSetHeartbeatResponse> hbResponse =
                    StatusWith<ReplSetHeartbeatResponse>(hb);
            getTopoCoord().prepareHeartbeatRequest(_now++,
                                                   setName,
                                                   member);
            getTopoCoord().processHeartbeatResponse(_now++,
                                                    roundTripTime,
                                                    member,
                                                    hbResponse,
                                                    OpTime(0,0));
        }

    private:
        scoped_ptr<TopologyCoordinatorImpl> _topo;
        scoped_ptr<ReplicationExecutor::CallbackData> _cbData;
        Date_t _now;
        int _selfIndex;
    };

    TEST_F(TopoCoordTest, ChooseSyncSourceBasic) {
        updateConfig(BSON("_id" << "rs0" <<
                          "version" << 1 <<
                          "members" << BSON_ARRAY(
                              BSON("_id" << 10 << "host" << "hself") <<
                              BSON("_id" << 20 << "host" << "h2") <<
                              BSON("_id" << 30 << "host" << "h3"))),
                     0);
        setSelfMemberState(MemberState::RS_SECONDARY);

        // member h2 is the furthest ahead
        heartbeatFromMember(HostAndPort("h2"), "rs0", MemberState::RS_SECONDARY, OpTime(1,0));
        heartbeatFromMember(HostAndPort("h3"), "rs0", MemberState::RS_SECONDARY, OpTime(0,0));

        // We start with no sync source
        ASSERT(getTopoCoord().getSyncSourceAddress().empty());

        // Fail due to insufficient number of pings
        getTopoCoord().chooseNewSyncSource(now()++, OpTime(0,0));
        ASSERT(getTopoCoord().getSyncSourceAddress().empty());

        // Record 2nd round of pings to allow choosing a new sync source; all members equidistant
        heartbeatFromMember(HostAndPort("h2"), "rs0", MemberState::RS_SECONDARY, OpTime(1,0));
        heartbeatFromMember(HostAndPort("h3"), "rs0", MemberState::RS_SECONDARY, OpTime(0,0));

        // Should choose h2, since it is furthest ahead
        getTopoCoord().chooseNewSyncSource(now()++, OpTime(0,0));
        ASSERT_EQUALS(HostAndPort("h2"), getTopoCoord().getSyncSourceAddress());
        
        // h3 becomes further ahead, so it should be chosen
        heartbeatFromMember(HostAndPort("h3"), "rs0", MemberState::RS_SECONDARY, OpTime(2,0));
        getTopoCoord().chooseNewSyncSource(now()++, OpTime(0,0));
        ASSERT_EQUALS(HostAndPort("h3"), getTopoCoord().getSyncSourceAddress());

        // h3 becomes an invalid candidate for sync source; should choose h2 again
        heartbeatFromMember(HostAndPort("h3"), "rs0", MemberState::RS_RECOVERING, OpTime(2,0));
        getTopoCoord().chooseNewSyncSource(now()++, OpTime(0,0));
        ASSERT_EQUALS(HostAndPort("h2"), getTopoCoord().getSyncSourceAddress());

        // h3 goes down
        downMember(HostAndPort("h3"), "rs0");
        getTopoCoord().chooseNewSyncSource(now()++, OpTime(0,0));
        ASSERT_EQUALS(HostAndPort("h2"), getTopoCoord().getSyncSourceAddress());

        // h3 back up and ahead
        heartbeatFromMember(HostAndPort("h3"), "rs0", MemberState::RS_SECONDARY, OpTime(2,0));
        getTopoCoord().chooseNewSyncSource(now()++, OpTime(0,0));
        ASSERT_EQUALS(HostAndPort("h3"), getTopoCoord().getSyncSourceAddress());

    }

    TEST_F(TopoCoordTest, ChooseSyncSourceCandidates) {
        updateConfig(BSON("_id" << "rs0" <<
                          "version" << 1 <<
                          "members" << BSON_ARRAY(
                              BSON("_id" << 1 << "host" << "hself") <<
                              BSON("_id" << 10 << "host" << "h1") <<
                              BSON("_id" << 20 << "host" << "h2" <<
                                   "buildIndexes" << false << "priority" << 0) <<
                              BSON("_id" << 30 << "host" << "h3" <<
                                   "hidden" << true << "priority" << 0 << "votes" << 0) <<
                              BSON("_id" << 40 << "host" << "h4" <<"arbiterOnly" << true) <<
                              BSON("_id" << 50 << "host" << "h5" <<
                                   "slaveDelay" << 1 << "priority" << 0) <<
                              BSON("_id" << 60 << "host" << "h6") <<
                              BSON("_id" << 70 << "host" << "hprimary"))),
                     0);

        setSelfMemberState(MemberState::RS_SECONDARY);
        OpTime lastOpTimeWeApplied = OpTime(100,0);

        heartbeatFromMember(HostAndPort("h1"), "rs0", MemberState::RS_SECONDARY,
                            OpTime(501, 0), Milliseconds(700));
        heartbeatFromMember(HostAndPort("h2"), "rs0", MemberState::RS_SECONDARY,
                            OpTime(501, 0), Milliseconds(600));
        heartbeatFromMember(HostAndPort("h3"), "rs0", MemberState::RS_SECONDARY,
                            OpTime(501, 0), Milliseconds(500));
        heartbeatFromMember(HostAndPort("h4"), "rs0", MemberState::RS_SECONDARY,
                            OpTime(501, 0), Milliseconds(400));
        heartbeatFromMember(HostAndPort("h5"), "rs0", MemberState::RS_SECONDARY,
                            OpTime(501, 0), Milliseconds(300));

        // This node is lagged further than maxSyncSourceLagSeconds.
        heartbeatFromMember(HostAndPort("h6"), "rs0", MemberState::RS_SECONDARY,
                            OpTime(499, 0), Milliseconds(200));

        heartbeatFromMember(HostAndPort("hprimary"), "rs0", MemberState::RS_PRIMARY,
                            OpTime(600, 0), Milliseconds(100));

        // Record 2nd round of pings to allow choosing a new sync source
        heartbeatFromMember(HostAndPort("h1"), "rs0", MemberState::RS_SECONDARY,
                            OpTime(501, 0), Milliseconds(700));
        heartbeatFromMember(HostAndPort("h2"), "rs0", MemberState::RS_SECONDARY,
                            OpTime(501, 0), Milliseconds(600));
        heartbeatFromMember(HostAndPort("h3"), "rs0", MemberState::RS_SECONDARY,
                            OpTime(501, 0), Milliseconds(500));
        heartbeatFromMember(HostAndPort("h4"), "rs0", MemberState::RS_SECONDARY,
                            OpTime(501, 0), Milliseconds(400));
        heartbeatFromMember(HostAndPort("h5"), "rs0", MemberState::RS_SECONDARY,
                            OpTime(501, 0), Milliseconds(300));
        heartbeatFromMember(HostAndPort("h6"), "rs0", MemberState::RS_SECONDARY,
                            OpTime(499, 0), Milliseconds(200));
        heartbeatFromMember(HostAndPort("hprimary"), "rs0", MemberState::RS_PRIMARY,
                            OpTime(600, 0), Milliseconds(100));

        // Should choose primary first; it's closest
        getTopoCoord().chooseNewSyncSource(now()++, lastOpTimeWeApplied);
        ASSERT_EQUALS(HostAndPort("hprimary"), getTopoCoord().getSyncSourceAddress());

        // Primary goes far far away
        heartbeatFromMember(HostAndPort("hprimary"), "rs0", MemberState::RS_PRIMARY,
                            OpTime(600, 0), Milliseconds(100000000));

        // Should choose h4.  (if an arbiter has an oplog, it's a valid sync source)
        // h6 is not considered because it is outside the maxSyncLagSeconds window,
        getTopoCoord().chooseNewSyncSource(now()++, lastOpTimeWeApplied);
        ASSERT_EQUALS(HostAndPort("h4"), getTopoCoord().getSyncSourceAddress());
        
        // h4 goes down; should choose h1
        downMember(HostAndPort("h4"), "rs0");
        getTopoCoord().chooseNewSyncSource(now()++, lastOpTimeWeApplied);
        ASSERT_EQUALS(HostAndPort("h1"), getTopoCoord().getSyncSourceAddress());

        // Primary and h1 go down; should choose h6 
        downMember(HostAndPort("h1"), "rs0");
        downMember(HostAndPort("hprimary"), "rs0");
        getTopoCoord().chooseNewSyncSource(now()++, lastOpTimeWeApplied);
        ASSERT_EQUALS(HostAndPort("h6"), getTopoCoord().getSyncSourceAddress());

        // h6 goes down; should choose h5
        downMember(HostAndPort("h6"), "rs0");
        getTopoCoord().chooseNewSyncSource(now()++, lastOpTimeWeApplied);
        ASSERT_EQUALS(HostAndPort("h5"), getTopoCoord().getSyncSourceAddress());

        // h5 goes down; should choose h3
        downMember(HostAndPort("h5"), "rs0");
        getTopoCoord().chooseNewSyncSource(now()++, lastOpTimeWeApplied);
        ASSERT_EQUALS(HostAndPort("h3"), getTopoCoord().getSyncSourceAddress());

        // h3 goes down; no sync source candidates remain
        downMember(HostAndPort("h3"), "rs0");
        getTopoCoord().chooseNewSyncSource(now()++, lastOpTimeWeApplied);
        ASSERT(getTopoCoord().getSyncSourceAddress().empty());
    }


    TEST_F(TopoCoordTest, ChooseSyncSourceChainingNotAllowed) {
        updateConfig(BSON("_id" << "rs0" <<
                          "version" << 1 <<
                          "settings" << BSON("chainingAllowed" << false) <<
                          "members" << BSON_ARRAY(
                              BSON("_id" << 10 << "host" << "hself") <<
                              BSON("_id" << 20 << "host" << "h2") <<
                              BSON("_id" << 30 << "host" << "h3"))),
                     0);

        setSelfMemberState(MemberState::RS_SECONDARY);

        heartbeatFromMember(HostAndPort("h2"), "rs0", MemberState::RS_SECONDARY,
                            OpTime(1, 0), Milliseconds(100));
        heartbeatFromMember(HostAndPort("h2"), "rs0", MemberState::RS_SECONDARY,
                            OpTime(1, 0), Milliseconds(100));
        heartbeatFromMember(HostAndPort("h3"), "rs0", MemberState::RS_SECONDARY,
                            OpTime(0, 0), Milliseconds(300));
        heartbeatFromMember(HostAndPort("h3"), "rs0", MemberState::RS_SECONDARY,
                            OpTime(0, 0), Milliseconds(300));

        // No primary situation: should choose no sync source.
        getTopoCoord().chooseNewSyncSource(now()++, OpTime(0,0));
        ASSERT(getTopoCoord().getSyncSourceAddress().empty());
        
        // Add primary
        heartbeatFromMember(HostAndPort("h3"), "rs0", MemberState::RS_PRIMARY,
                            OpTime(0, 0), Milliseconds(300));

        // h3 is primary and should be chosen as sync source, despite being further away than h2.
        getTopoCoord().chooseNewSyncSource(now()++, OpTime(0,0));
        ASSERT_EQUALS(HostAndPort("h3"), getTopoCoord().getSyncSourceAddress());

    }

    TEST_F(TopoCoordTest, ForceSyncSource) {
        updateConfig(BSON("_id" << "rs0" <<
                          "version" << 1 <<
                          "members" << BSON_ARRAY(
                              BSON("_id" << 10 << "host" << "hself") <<
                              BSON("_id" << 20 << "host" << "h2") <<
                              BSON("_id" << 30 << "host" << "h3"))),
                     0);

        setSelfMemberState(MemberState::RS_SECONDARY);

        // two rounds of heartbeat pings from each member
        heartbeatFromMember(HostAndPort("h2"), "rs0", MemberState::RS_SECONDARY,
                            OpTime(1, 0), Milliseconds(300));
        heartbeatFromMember(HostAndPort("h2"), "rs0", MemberState::RS_SECONDARY,
                            OpTime(1, 0), Milliseconds(300));
        heartbeatFromMember(HostAndPort("h3"), "rs0", MemberState::RS_SECONDARY,
                            OpTime(2, 0), Milliseconds(100));
        heartbeatFromMember(HostAndPort("h3"), "rs0", MemberState::RS_SECONDARY,
                            OpTime(2, 0), Milliseconds(100));

        getTopoCoord().chooseNewSyncSource(now()++, OpTime(0,0));
        ASSERT_EQUALS(HostAndPort("h3"), getTopoCoord().getSyncSourceAddress());
        getTopoCoord().setForceSyncSourceIndex(1);
        getTopoCoord().chooseNewSyncSource(now()++, OpTime(0,0));
        ASSERT_EQUALS(HostAndPort("h2"), getTopoCoord().getSyncSourceAddress());
    }

    TEST_F(TopoCoordTest, BlacklistSyncSource) {
        updateConfig(BSON("_id" << "rs0" <<
                          "version" << 1 <<
                          "members" << BSON_ARRAY(
                              BSON("_id" << 10 << "host" << "hself") <<
                              BSON("_id" << 20 << "host" << "h2") <<
                              BSON("_id" << 30 << "host" << "h3"))),
                     0);

        setSelfMemberState(MemberState::RS_SECONDARY);

        heartbeatFromMember(HostAndPort("h2"), "rs0", MemberState::RS_SECONDARY,
                            OpTime(1, 0), Milliseconds(300));
        heartbeatFromMember(HostAndPort("h2"), "rs0", MemberState::RS_SECONDARY,
                            OpTime(1, 0), Milliseconds(300));
        heartbeatFromMember(HostAndPort("h3"), "rs0", MemberState::RS_SECONDARY,
                            OpTime(2, 0), Milliseconds(100));
        heartbeatFromMember(HostAndPort("h3"), "rs0", MemberState::RS_SECONDARY,
                            OpTime(2, 0), Milliseconds(100));

        getTopoCoord().chooseNewSyncSource(now()++, OpTime(0,0));
        ASSERT_EQUALS(HostAndPort("h3"), getTopoCoord().getSyncSourceAddress());
        
        Date_t expireTime = 100;
        getTopoCoord().blacklistSyncSource(HostAndPort("h3"), expireTime);
        getTopoCoord().chooseNewSyncSource(now()++, OpTime(0,0));
        // Should choose second best choice now that h3 is blacklisted.
        ASSERT_EQUALS(HostAndPort("h2"), getTopoCoord().getSyncSourceAddress());

        // After time has passed, should go back to original sync source
        getTopoCoord().chooseNewSyncSource(expireTime, OpTime(0,0));
        ASSERT_EQUALS(HostAndPort("h3"), getTopoCoord().getSyncSourceAddress());
    }

    TEST_F(TopoCoordTest, PrepareSyncFromResponse) {
        // Test trying to sync from another node when we are an arbiter
        updateConfig(BSON("_id" << "rs0" <<
                          "version" << 1 <<
                          "members" << BSON_ARRAY(BSON("_id" << 0 <<
                                                       "host" << "hself" <<
                                                       "arbiterOnly" << true) <<
                                                  BSON("_id" << 1 <<
                                                       "host" << "h1"))),
                     0);

        OpTime staleOpTime(1, 1);
        OpTime ourOpTime(staleOpTime.getSecs() + 11, 1);
         
        Status result = Status::OK();
        BSONObjBuilder response;
        getTopoCoord().prepareSyncFromResponse(cbData(), HostAndPort("h1"),
                                               ourOpTime, &response, &result);
        ASSERT_EQUALS(ErrorCodes::NotSecondary, result);
        ASSERT_EQUALS("arbiters don't sync", result.reason());

        // Set up config for the rest of the tests
        updateConfig(BSON("_id" << "rs0" <<
                          "version" << 1 <<
                          "members" << BSON_ARRAY(
                                  BSON("_id" << 0 << "host" << "hself") <<
                                  BSON("_id" << 1 << "host" << "h1" << "arbiterOnly" << true) <<
                                  BSON("_id" << 2 << "host" << "h2" <<
                                       "priority" << 0 << "buildIndexes" << false) <<
                                  BSON("_id" << 3 << "host" << "h3") <<
                                  BSON("_id" << 4 << "host" << "h4") <<
                                  BSON("_id" << 5 << "host" << "h5") <<
                                  BSON("_id" << 6 << "host" << "h6"))),
                     0);

        // Try to sync while PRIMARY
        makeSelfPrimary();
        getTopoCoord()._setCurrentPrimaryForTest(0);
        BSONObjBuilder response1;
        getTopoCoord().prepareSyncFromResponse(
                cbData(), HostAndPort("h3"), ourOpTime, &response1, &result);
        ASSERT_EQUALS(ErrorCodes::NotSecondary, result);
        ASSERT_EQUALS("primaries don't sync", result.reason());
        ASSERT_EQUALS("h3:27017", response1.obj()["syncFromRequested"].String());

        // Try to sync from non-existent member
        setSelfMemberState(MemberState::RS_SECONDARY);
        getTopoCoord()._setCurrentPrimaryForTest(-1);
        BSONObjBuilder response2;
        getTopoCoord().prepareSyncFromResponse(
                cbData(), HostAndPort("fakemember"), ourOpTime, &response2, &result);
        ASSERT_EQUALS(ErrorCodes::NodeNotFound, result);
        ASSERT_EQUALS("Could not find member \"fakemember:27017\" in replica set", result.reason());

        // Try to sync from self
        BSONObjBuilder response3;
        getTopoCoord().prepareSyncFromResponse(
                cbData(), HostAndPort("hself"), ourOpTime, &response3, &result);
        ASSERT_EQUALS(ErrorCodes::InvalidOptions, result);
        ASSERT_EQUALS("I cannot sync from myself", result.reason());

        // Try to sync from an arbiter
        BSONObjBuilder response4;
        getTopoCoord().prepareSyncFromResponse(
                cbData(), HostAndPort("h1"), ourOpTime, &response4, &result);
        ASSERT_EQUALS(ErrorCodes::InvalidOptions, result);
        ASSERT_EQUALS("Cannot sync from \"h1:27017\" because it is an arbiter", result.reason());

        // Try to sync from a node that doesn't build indexes
        BSONObjBuilder response5;
        getTopoCoord().prepareSyncFromResponse(
                cbData(), HostAndPort("h2"), ourOpTime, &response5, &result);
        ASSERT_EQUALS(ErrorCodes::InvalidOptions, result);
        ASSERT_EQUALS("Cannot sync from \"h2:27017\" because it does not build indexes",
                      result.reason());

        // Try to sync from a member that is down
        downMember(HostAndPort("h4"), "rs0");

        BSONObjBuilder response7;
        getTopoCoord().prepareSyncFromResponse(
                cbData(), HostAndPort("h4"), ourOpTime, &response7, &result);
        ASSERT_EQUALS(ErrorCodes::HostUnreachable, result);
        ASSERT_EQUALS("I cannot reach the requested member: h4:27017", result.reason());

        // Sync successfully from a member that is stale
        heartbeatFromMember(HostAndPort("h5"), "rs0", MemberState::RS_SECONDARY,
                            staleOpTime, Milliseconds(100));

        BSONObjBuilder response8;
        getTopoCoord().prepareSyncFromResponse(
                cbData(), HostAndPort("h5"), ourOpTime, &response8, &result);
        ASSERT_OK(result);
        ASSERT_EQUALS("requested member \"h5:27017\" is more than 10 seconds behind us",
                      response8.obj()["warning"].String());
        getTopoCoord().chooseNewSyncSource(now()++, ourOpTime);
        ASSERT_EQUALS(HostAndPort("h5"), getTopoCoord().getSyncSourceAddress());

        // Sync successfully from an up-to-date member
        heartbeatFromMember(HostAndPort("h6"), "rs0", MemberState::RS_SECONDARY,
                            ourOpTime, Milliseconds(100));

        BSONObjBuilder response9;
        getTopoCoord().prepareSyncFromResponse(
                cbData(), HostAndPort("h6"), ourOpTime, &response9, &result);
        ASSERT_OK(result);
        BSONObj response9Obj = response9.obj();
        ASSERT_FALSE(response9Obj.hasField("warning"));
        ASSERT_EQUALS(HostAndPort("h5").toString(), response9Obj["prevSyncTarget"].String());
        getTopoCoord().chooseNewSyncSource(now()++, ourOpTime);
        ASSERT_EQUALS(HostAndPort("h6"), getTopoCoord().getSyncSourceAddress());
    }

    TEST_F(TopoCoordTest, ReplSetGetStatus) {
        // This test starts by configuring a TopologyCoordinator as a member of a 4 node replica
        // set, with each node in a different state.
        // The first node is DOWN, as if we tried heartbeating them and it failed in some way.
        // The second node is in state SECONDARY, as if we've received a valid heartbeat from them.
        // The third node is in state UNKNOWN, as if we've not yet had any heartbeating activity
        // with them yet.  The fourth node is PRIMARY and corresponds to ourself, which gets its
        // information for replSetGetStatus from a different source than the nodes that aren't
        // ourself.  After this setup, we call prepareStatusResponse and make sure that the fields
        // returned for each member match our expectations.
        Date_t startupTime(100);
        Date_t heartbeatTime = 5000;
        Seconds uptimeSecs(10);
        Date_t curTime = heartbeatTime + uptimeSecs.total_milliseconds();
        OpTime electionTime(1, 2);
        OpTime oplogProgress(3, 4);
        std::string setName = "mySet";

        updateConfig(BSON("_id" << setName <<
                          "version" << 1 <<
                          "members" << BSON_ARRAY(BSON("_id" << 0 << "host" << "test0:1234") <<
                                                  BSON("_id" << 1 << "host" << "test1:1234") <<
                                                  BSON("_id" << 2 << "host" << "test2:1234") <<
                                                  BSON("_id" << 3 << "host" << "test3:1234"))),
                     3,
                     startupTime + 1);

        // Now that the replica set is setup, put the members into the states we want them in.
        HostAndPort member = HostAndPort("test0:1234");
        StatusWith<ReplSetHeartbeatResponse> hbResponse =
                StatusWith<ReplSetHeartbeatResponse>(Status(ErrorCodes::HostUnreachable, ""));
        getTopoCoord().prepareHeartbeatRequest(startupTime + 2, setName, member);
        getTopoCoord().processHeartbeatResponse(heartbeatTime,
                                                Milliseconds(0),
                                                member,
                                                hbResponse,
                                                OpTime(0,0));

        member = HostAndPort("test1:1234");
        ReplSetHeartbeatResponse hb;
        hb.initialize(BSON("ok" << 1 <<
                           "v" << 1 <<
                           "state" << MemberState::RS_SECONDARY <<
                           "electionTime" << electionTime <<
                           "hbmsg" << "READY"));
        hb.setOpTime(oplogProgress);
        hbResponse = StatusWith<ReplSetHeartbeatResponse>(hb);
        getTopoCoord().prepareHeartbeatRequest(startupTime + 2,
                                               setName,
                                               member);
        getTopoCoord().processHeartbeatResponse(heartbeatTime,
                                                Milliseconds(4000),
                                                member,
                                                hbResponse,
                                                OpTime(0,0));
        makeSelfPrimary();

        // Now node 0 is down, node 1 is up, and for node 2 we have no heartbeat data yet.
        BSONObjBuilder statusBuilder;
        Status resultStatus(ErrorCodes::InternalError, "prepareStatusResponse didn't set result");
        getTopoCoord().prepareStatusResponse(cbData(),
                                             curTime,
                                             uptimeSecs.total_seconds(),
                                             oplogProgress,
                                             &statusBuilder,
                                             &resultStatus);
        ASSERT_OK(resultStatus);
        BSONObj rsStatus = statusBuilder.obj();

        // Test results for all non-self members
        ASSERT_EQUALS(setName, rsStatus["set"].String());
        ASSERT_EQUALS(curTime.asInt64(), rsStatus["date"].Date().asInt64());
        std::vector<BSONElement> memberArray = rsStatus["members"].Array();
        ASSERT_EQUALS(4U, memberArray.size());
        BSONObj member0Status = memberArray[0].Obj();
        BSONObj member1Status = memberArray[1].Obj();
        BSONObj member2Status = memberArray[2].Obj();

        // Test member 0, the node that's DOWN
        ASSERT_EQUALS(0, member0Status["_id"].Int());
        ASSERT_EQUALS("test0:1234", member0Status["name"].String());
        ASSERT_EQUALS(0, member0Status["health"].Double());
        ASSERT_EQUALS(MemberState::RS_DOWN, member0Status["state"].Int());
        ASSERT_EQUALS("(not reachable/healthy)", member0Status["stateStr"].String());
        ASSERT_EQUALS(0, member0Status["uptime"].Int());
        ASSERT_EQUALS(OpTime(), OpTime(member0Status["optime"].timestampValue()));
        ASSERT_EQUALS(OpTime().asDate(), member0Status["optimeDate"].Date().millis);
        ASSERT_EQUALS(heartbeatTime, member0Status["lastHeartbeat"].Date());
        ASSERT_EQUALS(Date_t(), member0Status["lastHeartbeatRecv"].Date());

        // Test member 1, the node that's SECONDARY
        ASSERT_EQUALS(1, member1Status["_id"].Int());
        ASSERT_EQUALS("test1:1234", member1Status["name"].String());
        ASSERT_EQUALS(1, member1Status["health"].Double());
        ASSERT_EQUALS(MemberState::RS_SECONDARY, member1Status["state"].Int());
        ASSERT_EQUALS(MemberState(MemberState::RS_SECONDARY).toString(),
                      member1Status["stateStr"].String());
        ASSERT_EQUALS(uptimeSecs.total_seconds(), member1Status["uptime"].Int());
        ASSERT_EQUALS(oplogProgress, OpTime(member1Status["optime"].timestampValue()));
        ASSERT_EQUALS(oplogProgress.asDate(), member1Status["optimeDate"].Date().millis);
        ASSERT_EQUALS(heartbeatTime, member1Status["lastHeartbeat"].Date());
        ASSERT_EQUALS(Date_t(), member1Status["lastHeartbeatRecv"].Date());
        ASSERT_EQUALS("READY", member1Status["lastHeartbeatMessage"].String());

        // Test member 2, the node that's UNKNOWN
        ASSERT_EQUALS(2, member2Status["_id"].Int());
        ASSERT_EQUALS("test2:1234", member2Status["name"].String());
        ASSERT_EQUALS(-1, member2Status["health"].Double());
        ASSERT_EQUALS(MemberState::RS_UNKNOWN, member2Status["state"].Int());
        ASSERT_EQUALS(MemberState(MemberState::RS_UNKNOWN).toString(),
                      member2Status["stateStr"].String());
        ASSERT_FALSE(member2Status.hasField("uptime"));
        ASSERT_FALSE(member2Status.hasField("optime"));
        ASSERT_FALSE(member2Status.hasField("optimeDate"));
        ASSERT_FALSE(member2Status.hasField("lastHearbeat"));
        ASSERT_FALSE(member2Status.hasField("lastHearbeatRecv"));

        // Now test results for ourself, the PRIMARY
        ASSERT_EQUALS(MemberState::RS_PRIMARY, rsStatus["myState"].Int());
        BSONObj selfStatus = memberArray[3].Obj();
        ASSERT_TRUE(selfStatus["self"].Bool());
        ASSERT_EQUALS(3, selfStatus["_id"].Int());
        ASSERT_EQUALS("test3:1234", selfStatus["name"].String());
        ASSERT_EQUALS(1, selfStatus["health"].Double());
        ASSERT_EQUALS(MemberState::RS_PRIMARY, selfStatus["state"].Int());
        ASSERT_EQUALS(MemberState(MemberState::RS_PRIMARY).toString(),
                      selfStatus["stateStr"].String());
        ASSERT_EQUALS(uptimeSecs.total_seconds(), selfStatus["uptime"].Int());
        ASSERT_EQUALS(oplogProgress, OpTime(selfStatus["optime"].timestampValue()));
        ASSERT_EQUALS(oplogProgress.asDate(), selfStatus["optimeDate"].Date().millis);

        // TODO(spencer): Test electionTime and pingMs are set properly
    }

    TEST_F(TopoCoordTest, PrepareFreshResponse) {
        updateConfig(BSON("_id" << "rs0" <<
                          "version" << 10 <<
                          "members" << BSON_ARRAY(
                              BSON("_id" << 10 <<
                                   "host" << "hself" <<
                                   "priority" << 10) <<
                              BSON("_id" << 20 << "host" << "h1") <<
                              BSON("_id" << 30 << "host" << "h2") <<
                              BSON("_id" << 40 <<
                                   "host" << "h3" <<
                                   "priority" << 10))),
                     0);

        OpTime ourOpTime(10, 10);
        OpTime staleOpTime(1, 1);

        Status internalErrorStatus(ErrorCodes::InternalError, "didn't set status");

        // Test with incorrect replset name
        ReplicationCoordinator::ReplSetFreshArgs args;
        args.setName = "fakeset";

        BSONObjBuilder responseBuilder0;
        Status status0 = internalErrorStatus;
        getTopoCoord().prepareFreshResponse(cbData(), args, ourOpTime, &responseBuilder0, &status0);
        ASSERT_EQUALS(ErrorCodes::ReplicaSetNotFound, status0);
        ASSERT_TRUE(responseBuilder0.obj().isEmpty());


        // Test with non-existent node.
        args.setName = "rs0";
        args.cfgver = 5; // stale config
        args.id = 0;
        args.who = HostAndPort("fakenode");
        args.opTime = staleOpTime;

        BSONObjBuilder responseBuilder1;
        Status status1 = internalErrorStatus;
        getTopoCoord().prepareFreshResponse(cbData(), args, ourOpTime, &responseBuilder1, &status1);
        ASSERT_OK(status1);
        BSONObj response1 = responseBuilder1.obj();
        ASSERT_EQUALS("config version stale", response1["info"].String());
        ASSERT_EQUALS(ourOpTime, OpTime(response1["opTime"].timestampValue()));
        ASSERT_TRUE(response1["fresher"].Bool());
        ASSERT_TRUE(response1["veto"].Bool());
        ASSERT_EQUALS("replSet couldn't find member with id 0", response1["errmsg"].String());


        // Test when we are primary and target node is stale.
        args.id = 20;
        args.cfgver = 10;
        args.who = HostAndPort("h1");
        args.opTime = ourOpTime;

        heartbeatFromMember(HostAndPort("h1"), "rs0", MemberState::RS_SECONDARY, staleOpTime);
        makeSelfPrimary();

        BSONObjBuilder responseBuilder2;
        Status status2 = internalErrorStatus;
        getTopoCoord().prepareFreshResponse(cbData(), args, ourOpTime, &responseBuilder2, &status2);
        ASSERT_OK(status2);
        BSONObj response2 = responseBuilder2.obj();
        ASSERT_FALSE(response2.hasField("info"));
        ASSERT_EQUALS(ourOpTime, OpTime(response2["opTime"].timestampValue()));
        ASSERT_FALSE(response2["fresher"].Bool());
        ASSERT_TRUE(response2["veto"].Bool());
        ASSERT_EQUALS("I am already primary, h1:27017 can try again once I've stepped down",
                      response2["errmsg"].String());


        // Test when someone else is primary and target node is stale.
        heartbeatFromMember(HostAndPort("h2"), "rs0", MemberState::RS_SECONDARY, ourOpTime);
        setSelfMemberState(MemberState::RS_SECONDARY);
        getTopoCoord()._setCurrentPrimaryForTest(2);

        BSONObjBuilder responseBuilder3;
        Status status3 = internalErrorStatus;
        getTopoCoord().prepareFreshResponse(cbData(), args, ourOpTime, &responseBuilder3, &status3);
        ASSERT_OK(status3);
        BSONObj response3 = responseBuilder3.obj();
        ASSERT_FALSE(response3.hasField("info"));
        ASSERT_EQUALS(ourOpTime, OpTime(response3["opTime"].timestampValue()));
        ASSERT_FALSE(response3["fresher"].Bool());
        ASSERT_TRUE(response3["veto"].Bool());
        ASSERT_EQUALS(
                "h1:27017 is trying to elect itself but h2:27017 is already primary and more "
                        "up-to-date",
                response3["errmsg"].String());


        // Test trying to elect a node that is caught up but isn't the highest priority node.
        heartbeatFromMember(HostAndPort("h1"), "rs0", MemberState::RS_SECONDARY, ourOpTime);
        heartbeatFromMember(HostAndPort("h2"), "rs0", MemberState::RS_SECONDARY, staleOpTime);
        heartbeatFromMember(HostAndPort("h3"), "rs0", MemberState::RS_SECONDARY, ourOpTime);

        BSONObjBuilder responseBuilder4;
        Status status4 = internalErrorStatus;
        getTopoCoord().prepareFreshResponse(cbData(), args, ourOpTime, &responseBuilder4, &status4);
        ASSERT_OK(status4);
        BSONObj response4 = responseBuilder4.obj();
        ASSERT_FALSE(response4.hasField("info"));
        ASSERT_EQUALS(ourOpTime, OpTime(response4["opTime"].timestampValue()));
        ASSERT_FALSE(response4["fresher"].Bool());
        ASSERT_TRUE(response4["veto"].Bool());
        ASSERT_EQUALS("h1:27017 has lower priority of 1 than h3:27017 which has a priority of 10",
                      response4["errmsg"].String());


        // Test trying to elect a node that isn't electable
        args.id = 40;
        args.who = HostAndPort("h3");

        downMember(HostAndPort("h3"), "rs0");

        BSONObjBuilder responseBuilder5;
        Status status5 = internalErrorStatus;
        getTopoCoord().prepareFreshResponse(cbData(), args, ourOpTime, &responseBuilder5, &status5);
        ASSERT_OK(status5);
        BSONObj response5 = responseBuilder5.obj();
        ASSERT_FALSE(response5.hasField("info"));
        ASSERT_EQUALS(ourOpTime, OpTime(response5["opTime"].timestampValue()));
        ASSERT_FALSE(response5["fresher"].Bool());
        ASSERT_TRUE(response5["veto"].Bool());
        ASSERT_EQUALS(
            "I don't think h3:27017 is electable because the member is not currently a secondary",
            response5["errmsg"].String());


        // Finally, test trying to elect a valid node
        args.id = 40;
        args.who = HostAndPort("h3");

        heartbeatFromMember(HostAndPort("h3"), "rs0", MemberState::RS_SECONDARY, ourOpTime);

        BSONObjBuilder responseBuilder6;
        Status status6 = internalErrorStatus;
        getTopoCoord().prepareFreshResponse(cbData(), args, ourOpTime, &responseBuilder6, &status6);
        ASSERT_OK(status6);
        BSONObj response6 = responseBuilder6.obj();
        cout << response6.jsonString(TenGen, 1);
        ASSERT_FALSE(response6.hasField("info")) << response6.toString();
        ASSERT_EQUALS(ourOpTime, OpTime(response6["opTime"].timestampValue()));
        ASSERT_FALSE(response6["fresher"].Bool()) << response6.toString();
        ASSERT_FALSE(response6["veto"].Bool()) << response6.toString();
        ASSERT_FALSE(response6.hasField("errmsg")) << response6.toString();
    }

    class HeartbeatResponseTest : public TopoCoordTest {
    public:

        virtual void setUp() {
            TopoCoordTest::setUp();
            updateConfig(BSON("_id" << "rs0" <<
                              "version" << 5 <<
                              "members" << BSON_ARRAY(
                                  BSON("_id" << 0 << "host" << "host1:27017") <<
                                  BSON("_id" << 1 << "host" << "host2:27017") <<
                                  BSON("_id" << 2 << "host" << "host3:27017")) <<
                              "settings" << BSON("heartbeatTimeoutSecs" << 5)),
                         0);
        }

        HeartbeatResponseAction receiveUpHeartbeat(
                const HostAndPort& member,
                const std::string& setName,
                const MemberState& memberState,
                const OpTime& electionTime,
                const OpTime& lastOpTimeSender,
                const OpTime& lastOpTimeReceiver) {
            ReplSetHeartbeatResponse hb;
            hb.initialize(BSON("ok" << 1 <<
                               "v" << 1 <<
                               "state" << memberState.s));
            hb.setOpTime(lastOpTimeSender);
            hb.setElectionTime(electionTime);
            StatusWith<ReplSetHeartbeatResponse> hbResponse =
                    StatusWith<ReplSetHeartbeatResponse>(hb);
            getTopoCoord().prepareHeartbeatRequest(now()++,
                                                   setName,
                                                   member);
            return getTopoCoord().processHeartbeatResponse(now()++,
                                                           Milliseconds(0),
                                                           member,
                                                           hbResponse,
                                                           lastOpTimeReceiver);
        }

        HeartbeatResponseAction receiveDownHeartbeat(const HostAndPort& member,
                                                     const std::string& setName) {
            return downMember(member, setName);
        }
    };

    TEST_F(HeartbeatResponseTest, HeartbeatRetriesAtMostTwice) {
        // Confirm that the topology coordinator attempts to retry a failed heartbeat two times
        // after initial failure, assuming that the heartbeat timeout (set to 5 seconds in the
        // fixture) has not expired.
        //
        // Failed heartbeats propose taking no action, other than scheduling the next heartbeat.  We
        // can detect a retry vs the next regularly scheduled heartbeat because retries are
        // scheduled immediately, while subsequent heartbeats are scheduled after the hard-coded
        // heartbeat interval of 2 seconds.
        HostAndPort target("host2", 27017);
        Date_t firstRequestDate = unittest::assertGet(dateFromISOString("2014-08-29T13:00Z"));

        // Initial heartbeat request prepared, at t + 0.
        std::pair<ReplSetHeartbeatArgs, Milliseconds> request =
            getTopoCoord().prepareHeartbeatRequest(firstRequestDate,
                                                   "rs0",
                                                   target);
        // 5 seconds to successfully complete the heartbeat before the timeout expires.
        ASSERT_EQUALS(5000, request.second.total_milliseconds());

        // Initial heartbeat request fails at t + 4000ms
        HeartbeatResponseAction action =
            getTopoCoord().processHeartbeatResponse(
                    firstRequestDate + 4000, // 4 of the 5 seconds elapsed; could still retry.
                    Milliseconds(3990), // Spent 3.99 of the 4 seconds in the network.
                    target,
                    StatusWith<ReplSetHeartbeatResponse>(ErrorCodes::NodeNotFound, "Bad DNS?"),
                    OpTime(0, 0));  // We've never applied anything.

        ASSERT_EQUALS(HeartbeatResponseAction::NoAction, action.getAction());
        // Because the heartbeat failed without timing out, we expect to retry immediately.
        ASSERT_EQUALS(Date_t(firstRequestDate + 4000), action.getNextHeartbeatStartDate());

        // First heartbeat retry prepared, at t + 4000ms.
        request =
            getTopoCoord().prepareHeartbeatRequest(
                    firstRequestDate + 4000,
                    "rs0",
                    target);
        // One second left to complete the heartbeat.
        ASSERT_EQUALS(1000, request.second.total_milliseconds());

        // First retry fails at t + 4500ms
        action =
            getTopoCoord().processHeartbeatResponse(
                    firstRequestDate + 4500, // 4.5 of the 5 seconds elapsed; could still retry.
                    Milliseconds(400), // Spent 0.4 of the 0.5 seconds in the network.
                    target,
                    StatusWith<ReplSetHeartbeatResponse>(ErrorCodes::NodeNotFound, "Bad DNS?"),
                    OpTime(0, 0));  // We've never applied anything.
        ASSERT_EQUALS(HeartbeatResponseAction::NoAction, action.getAction());
        // Because the first retry failed without timing out, we expect to retry immediately.
        ASSERT_EQUALS(Date_t(firstRequestDate + 4500), action.getNextHeartbeatStartDate());

        // Second retry prepared at t + 4500ms.
        request =
            getTopoCoord().prepareHeartbeatRequest(
                    firstRequestDate + 4500,
                    "rs0",
                    target);
        // 500ms left to complete the heartbeat.
        ASSERT_EQUALS(500, request.second.total_milliseconds());

        // Second retry fails at t + 4800ms
        action =
            getTopoCoord().processHeartbeatResponse(
                    firstRequestDate + 4800, // 4.8 of the 5 seconds elapsed; could still retry.
                    Milliseconds(100), // Spent 0.1 of the 0.3 seconds in the network.
                    target,
                    StatusWith<ReplSetHeartbeatResponse>(ErrorCodes::NodeNotFound, "Bad DNS?"),
                    OpTime(0, 0));  // We've never applied anything.
        ASSERT_EQUALS(HeartbeatResponseAction::NoAction, action.getAction());
        // Because this is the second retry, rather than retry again, we expect to wait for the
        // heartbeat interval of 2 seconds to elapse.
        ASSERT_EQUALS(Date_t(firstRequestDate + 6800), action.getNextHeartbeatStartDate());
    }

    TEST_F(HeartbeatResponseTest, HeartbeatTimeoutSuppressesFirstRetry) {
        // Confirm that the topology coordinator does not schedule an immediate heartbeat retry if
        // the heartbeat timeout period expired before the initial request completed.

        HostAndPort target("host2", 27017);
        Date_t firstRequestDate = unittest::assertGet(dateFromISOString("2014-08-29T13:00Z"));

        // Initial heartbeat request prepared, at t + 0.
        std::pair<ReplSetHeartbeatArgs, Milliseconds> request =
            getTopoCoord().prepareHeartbeatRequest(firstRequestDate,
                                                   "rs0",
                                                   target);
        // 5 seconds to successfully complete the heartbeat before the timeout expires.
        ASSERT_EQUALS(5000, request.second.total_milliseconds());

        // Initial heartbeat request fails at t + 5000ms
        HeartbeatResponseAction action =
            getTopoCoord().processHeartbeatResponse(
                    firstRequestDate + 5000, // Entire heartbeat period elapsed; no retry allowed.
                    Milliseconds(4990), // Spent 4.99 of the 4 seconds in the network.
                    target,
                    StatusWith<ReplSetHeartbeatResponse>(ErrorCodes::ExceededTimeLimit,
                                                         "Took too long"),
                    OpTime(0, 0));  // We've never applied anything.

        ASSERT_EQUALS(HeartbeatResponseAction::NoAction, action.getAction());
        // Because the heartbeat timed out, we'll retry in 2 seconds.
        ASSERT_EQUALS(Date_t(firstRequestDate + 7000), action.getNextHeartbeatStartDate());
    }

    TEST_F(HeartbeatResponseTest, HeartbeatTimeoutSuppressesSecondRetry) {
        // Confirm that the topology coordinator does not schedule an second heartbeat retry if
        // the heartbeat timeout period expired before the first retry completed.

        HostAndPort target("host2", 27017);
        Date_t firstRequestDate = unittest::assertGet(dateFromISOString("2014-08-29T13:00Z"));

        // Initial heartbeat request prepared, at t + 0.
        std::pair<ReplSetHeartbeatArgs, Milliseconds> request =
            getTopoCoord().prepareHeartbeatRequest(firstRequestDate,
                                                   "rs0",
                                                   target);
        // 5 seconds to successfully complete the heartbeat before the timeout expires.
        ASSERT_EQUALS(5000, request.second.total_milliseconds());

        // Initial heartbeat request fails at t + 5000ms
        HeartbeatResponseAction action =
            getTopoCoord().processHeartbeatResponse(
                    firstRequestDate + 4000, // 4 seconds elapsed, retry allowed.
                    Milliseconds(3990), // Spent 3.99 of the 4 seconds in the network.
                    target,
                    StatusWith<ReplSetHeartbeatResponse>(ErrorCodes::ExceededTimeLimit,
                                                         "Took too long"),
                    OpTime(0, 0));  // We've never applied anything.

        ASSERT_EQUALS(HeartbeatResponseAction::NoAction, action.getAction());
        // Because the heartbeat failed without timing out, we expect to retry immediately.
        ASSERT_EQUALS(Date_t(firstRequestDate + 4000), action.getNextHeartbeatStartDate());

        // First heartbeat retry prepared, at t + 4000ms.
        request =
            getTopoCoord().prepareHeartbeatRequest(
                    firstRequestDate + 4000,
                    "rs0",
                    target);
        // One second left to complete the heartbeat.
        ASSERT_EQUALS(1000, request.second.total_milliseconds());

        action =
            getTopoCoord().processHeartbeatResponse(
                    firstRequestDate + 5010, // Entire heartbeat period elapsed; no retry allowed.
                    Milliseconds(1000), // Spent 1 of the 1.01 seconds in the network.
                    target,
                    StatusWith<ReplSetHeartbeatResponse>(ErrorCodes::ExceededTimeLimit,
                                                         "Took too long"),
                    OpTime(0, 0));  // We've never applied anything.

        ASSERT_EQUALS(HeartbeatResponseAction::NoAction, action.getAction());
        // Because the heartbeat timed out, we'll retry in 2 seconds.
        ASSERT_EQUALS(Date_t(firstRequestDate + 7010), action.getNextHeartbeatStartDate());
    }

    TEST_F(HeartbeatResponseTest, DecideToReconfigAfterFirstRetry) {
        // Confirm that action responses can come back from retries; in this, expect a Reconfig
        // action.

        HostAndPort target("host2", 27017);
        Date_t firstRequestDate = unittest::assertGet(dateFromISOString("2014-08-29T13:00Z"));

        // Initial heartbeat request prepared, at t + 0.
        std::pair<ReplSetHeartbeatArgs, Milliseconds> request =
            getTopoCoord().prepareHeartbeatRequest(firstRequestDate,
                                                   "rs0",
                                                   target);
        // 5 seconds to successfully complete the heartbeat before the timeout expires.
        ASSERT_EQUALS(5000, request.second.total_milliseconds());

        // Initial heartbeat request fails at t + 5000ms
        HeartbeatResponseAction action =
            getTopoCoord().processHeartbeatResponse(
                    firstRequestDate + 4000, // 4 seconds elapsed, retry allowed.
                    Milliseconds(3990), // Spent 3.99 of the 4 seconds in the network.
                    target,
                    StatusWith<ReplSetHeartbeatResponse>(ErrorCodes::ExceededTimeLimit,
                                                         "Took too long"),
                    OpTime(0, 0));  // We've never applied anything.

        ASSERT_EQUALS(HeartbeatResponseAction::NoAction, action.getAction());
        // Because the heartbeat failed without timing out, we expect to retry immediately.
        ASSERT_EQUALS(Date_t(firstRequestDate + 4000), action.getNextHeartbeatStartDate());

        // First heartbeat retry prepared, at t + 4000ms.
        request =
            getTopoCoord().prepareHeartbeatRequest(
                    firstRequestDate + 4000,
                    "rs0",
                    target);
        // One second left to complete the heartbeat.
        ASSERT_EQUALS(1000, request.second.total_milliseconds());

        ReplicaSetConfig newConfig;
        ASSERT_OK(newConfig.initialize(
                          BSON("_id" << "rs0" <<
                               "version" << 7 <<
                               "members" << BSON_ARRAY(
                                       BSON("_id" << 0 << "host" << "host1:27017") <<
                                       BSON("_id" << 1 << "host" << "host2:27017") <<
                                       BSON("_id" << 2 << "host" << "host3:27017") <<
                                       BSON("_id" << 3 << "host" << "host4:27017")) <<
                               "settings" << BSON("heartbeatTimeoutSecs" << 5))));
        ASSERT_OK(newConfig.validate());

        ReplSetHeartbeatResponse reconfigResponse;
        reconfigResponse.noteReplSet();
        reconfigResponse.setSetName("rs0");
        reconfigResponse.setState(MemberState::RS_SECONDARY);
        reconfigResponse.setElectable(true);
        reconfigResponse.setVersion(7);
        reconfigResponse.setConfig(newConfig);
        action =
            getTopoCoord().processHeartbeatResponse(
                    firstRequestDate + 4500, // Time is left.
                    Milliseconds(400), // Spent 0.4 of the 0.5 second in the network.
                    target,
                    StatusWith<ReplSetHeartbeatResponse>(reconfigResponse),
                    OpTime(0, 0));  // We've never applied anything.
        ASSERT_EQUALS(HeartbeatResponseAction::Reconfig, action.getAction());
        ASSERT_EQUALS(Date_t(firstRequestDate + 6500), action.getNextHeartbeatStartDate());
    }

    TEST_F(HeartbeatResponseTest, UpdateHeartbeatDataNewPrimary) {
        OpTime election = OpTime(5,0);
        OpTime lastOpTimeApplied = OpTime(3,0);

        HeartbeatResponseAction nextAction = receiveUpHeartbeat(HostAndPort("host2"),
                                                                "rs0",
                                                                MemberState::RS_PRIMARY,
                                                                election,
                                                                election,
                                                                lastOpTimeApplied);
        ASSERT_NO_ACTION(nextAction.getAction());
    }

    TEST_F(HeartbeatResponseTest, UpdateHeartbeatDataTwoPrimariesNewOneOlder) {
        OpTime election = OpTime(5,0);
        OpTime election2 = OpTime(4,0);
        OpTime lastOpTimeApplied = OpTime(3,0);

        HeartbeatResponseAction nextAction = receiveUpHeartbeat(HostAndPort("host2"),
                                                                "rs0",
                                                                MemberState::RS_PRIMARY,
                                                                election,
                                                                election,
                                                                lastOpTimeApplied);
        ASSERT_NO_ACTION(nextAction.getAction());

        nextAction = receiveUpHeartbeat(HostAndPort("host3"),
                                        "rs0",
                                        MemberState::RS_PRIMARY,
                                        election2,
                                        election,
                                        lastOpTimeApplied);
        ASSERT_NO_ACTION(nextAction.getAction());
    }

    TEST_F(HeartbeatResponseTest, UpdateHeartbeatDataTwoPrimariesNewOneNewer) {
        OpTime election = OpTime(4,0);
        OpTime election2 = OpTime(5,0);
        OpTime lastOpTimeApplied = OpTime(3,0);

        HeartbeatResponseAction nextAction = receiveUpHeartbeat(HostAndPort("host2"),
                                                                "rs0",
                                                                MemberState::RS_PRIMARY,
                                                                election,
                                                                election,
                                                                lastOpTimeApplied);
        ASSERT_NO_ACTION(nextAction.getAction());

        nextAction = receiveUpHeartbeat(HostAndPort("host3"),
                                        "rs0",
                                        MemberState::RS_PRIMARY,
                                        election2,
                                        election,
                                        lastOpTimeApplied);
        ASSERT_NO_ACTION(nextAction.getAction());
    }

    TEST_F(HeartbeatResponseTest, UpdateHeartbeatDataTwoPrimariesIncludingMeNewOneOlder) {
        makeSelfPrimary(OpTime(5,0));

        OpTime election = OpTime(4,0);
        OpTime lastOpTimeApplied = OpTime(3,0);

        HeartbeatResponseAction nextAction = receiveUpHeartbeat(HostAndPort("host2"),
                                                                "rs0",
                                                                MemberState::RS_PRIMARY,
                                                                election,
                                                                election,
                                                                lastOpTimeApplied);
        ASSERT_EQUALS(HeartbeatResponseAction::StepDownRemotePrimary, nextAction.getAction());
        ASSERT_EQUALS(1, nextAction.getPrimaryConfigIndex());
    }

    TEST_F(HeartbeatResponseTest, UpdateHeartbeatDataTwoPrimariesIncludingMeNewOneNewer) {
        makeSelfPrimary(OpTime(2,0));

        OpTime election = OpTime(4,0);
        OpTime lastOpTimeApplied = OpTime(3,0);

        HeartbeatResponseAction nextAction = receiveUpHeartbeat(HostAndPort("host2"),
                                                                "rs0",
                                                                MemberState::RS_PRIMARY,
                                                                election,
                                                                election,
                                                                lastOpTimeApplied);
        ASSERT_EQUALS(HeartbeatResponseAction::StepDownSelf, nextAction.getAction());
        ASSERT_EQUALS(0, nextAction.getPrimaryConfigIndex());
    }

    TEST_F(HeartbeatResponseTest, UpdateHeartbeatDataPrimaryDownNoMajority) {
        setSelfMemberState(MemberState::RS_SECONDARY);

        OpTime election = OpTime(4,0);
        OpTime lastOpTimeApplied = OpTime(3,0);

        HeartbeatResponseAction nextAction = receiveUpHeartbeat(HostAndPort("host2"),
                                                                "rs0",
                                                                MemberState::RS_PRIMARY,
                                                                election,
                                                                election,
                                                                lastOpTimeApplied);
        ASSERT_NO_ACTION(nextAction.getAction());

        nextAction = receiveDownHeartbeat(HostAndPort("host2"), "rs0");
        ASSERT_NO_ACTION(nextAction.getAction());
    }

    TEST_F(HeartbeatResponseTest, UpdateHeartbeatDataPrimaryDownMajorityButNoPriority) {
        setSelfMemberState(MemberState::RS_SECONDARY);

        updateConfig(BSON("_id" << "rs0" <<
                          "version" << 5 <<
                          "members" << BSON_ARRAY(
                              BSON("_id" << 0 << "host" << "host1:27017" << "priority" << 0) <<
                              BSON("_id" << 1 << "host" << "host2:27017") <<
                              BSON("_id" << 2 << "host" << "host3:27017"))),
                     0);

        OpTime election = OpTime(4,0);
        OpTime lastOpTimeApplied = OpTime(3,0);

        HeartbeatResponseAction nextAction = receiveUpHeartbeat(HostAndPort("host2"),
                                                                "rs0",
                                                                MemberState::RS_PRIMARY,
                                                                election,
                                                                election,
                                                                lastOpTimeApplied);
        ASSERT_NO_ACTION(nextAction.getAction());

        nextAction = receiveUpHeartbeat(HostAndPort("host3"),
                                        "rs0",
                                        MemberState::RS_SECONDARY,
                                        election,
                                        election,
                                        lastOpTimeApplied);
        ASSERT_NO_ACTION(nextAction.getAction());

        nextAction = receiveDownHeartbeat(HostAndPort("host2"), "rs0");
        ASSERT_NO_ACTION(nextAction.getAction());
    }

    TEST_F(HeartbeatResponseTest, UpdateHeartbeatDataPrimaryDownMajorityButIAmStarting) {
        setSelfMemberState(MemberState::RS_STARTUP);

        OpTime election = OpTime(4,0);
        OpTime lastOpTimeApplied = OpTime(3,0);

        HeartbeatResponseAction nextAction = receiveUpHeartbeat(HostAndPort("host2"),
                                                                "rs0",
                                                                MemberState::RS_PRIMARY,
                                                                election,
                                                                election,
                                                                lastOpTimeApplied);
        ASSERT_NO_ACTION(nextAction.getAction());

        nextAction = receiveUpHeartbeat(HostAndPort("host3"),
                                        "rs0",
                                        MemberState::RS_SECONDARY,
                                        election,
                                        election,
                                        lastOpTimeApplied);
        ASSERT_NO_ACTION(nextAction.getAction());

        nextAction = receiveDownHeartbeat(HostAndPort("host2"), "rs0");
        ASSERT_NO_ACTION(nextAction.getAction());
    }

    TEST_F(HeartbeatResponseTest, UpdateHeartbeatDataPrimaryDownMajorityButIAmRecovering) {
        setSelfMemberState(MemberState::RS_RECOVERING);

        OpTime election = OpTime(4,0);
        OpTime lastOpTimeApplied = OpTime(3,0);

        HeartbeatResponseAction nextAction = receiveUpHeartbeat(HostAndPort("host2"),
                                                                "rs0",
                                                                MemberState::RS_PRIMARY,
                                                                election,
                                                                election,
                                                                lastOpTimeApplied);
        ASSERT_NO_ACTION(nextAction.getAction());

        nextAction = receiveDownHeartbeat(HostAndPort("host2"), "rs0");
        ASSERT_NO_ACTION(nextAction.getAction());
    }

    TEST_F(HeartbeatResponseTest, UpdateHeartbeatDataPrimaryDownMajorityButIHaveStepdownWait) {
        setSelfMemberState(MemberState::RS_SECONDARY);

        OpTime election = OpTime(4,0);
        OpTime lastOpTimeApplied = OpTime(3,0);

        HeartbeatResponseAction nextAction = receiveUpHeartbeat(HostAndPort("host2"),
                                                                "rs0",
                                                                MemberState::RS_PRIMARY,
                                                                election,
                                                                election,
                                                                lastOpTimeApplied);
        ASSERT_NO_ACTION(nextAction.getAction());

        nextAction = receiveUpHeartbeat(HostAndPort("host3"),
                                        "rs0",
                                        MemberState::RS_SECONDARY,
                                        election,
                                        election,
                                        lastOpTimeApplied);
        ASSERT_NO_ACTION(nextAction.getAction());

        // freeze node to set stepdown wait
        Status result = Status::OK();
        BSONObjBuilder response;
        getTopoCoord().prepareFreezeResponse(cbData(), now()++, 20, &response, &result);

        nextAction = receiveDownHeartbeat(HostAndPort("host2"), "rs0");
        ASSERT_NO_ACTION(nextAction.getAction());
    }

    TEST_F(HeartbeatResponseTest, UpdateHeartbeatDataPrimaryDownMajorityButIArbiter) {
        updateConfig(BSON("_id" << "rs0" <<
                          "version" << 5 <<
                          "members" << BSON_ARRAY(
                              BSON("_id" << 0 << "host" << "host1:27017" <<
                                   "arbiterOnly" << true) <<
                              BSON("_id" << 1 << "host" << "host2:27017"))),
                     0);

        OpTime election = OpTime(4,0);
        OpTime lastOpTimeApplied = OpTime(3,0);

        HeartbeatResponseAction nextAction = receiveUpHeartbeat(HostAndPort("host2"),
                                                                "rs0",
                                                                MemberState::RS_PRIMARY,
                                                                election,
                                                                election,
                                                                lastOpTimeApplied);
        ASSERT_NO_ACTION(nextAction.getAction());

        nextAction = receiveDownHeartbeat(HostAndPort("host2"), "rs0");
        ASSERT_NO_ACTION(nextAction.getAction());
    }

    TEST_F(HeartbeatResponseTest, UpdateHeartbeatDataPrimaryDownMajority) {
        setSelfMemberState(MemberState::RS_SECONDARY);

        OpTime election = OpTime(4,0);
        OpTime lastOpTimeApplied = OpTime(3,0);

        HeartbeatResponseAction nextAction = receiveUpHeartbeat(HostAndPort("host2"),
                                                                "rs0",
                                                                MemberState::RS_PRIMARY,
                                                                election,
                                                                election,
                                                                lastOpTimeApplied);
        ASSERT_NO_ACTION(nextAction.getAction());

        nextAction = receiveUpHeartbeat(HostAndPort("host3"),
                                        "rs0",
                                        MemberState::RS_SECONDARY,
                                        election,
                                        election,
                                        lastOpTimeApplied);
        ASSERT_NO_ACTION(nextAction.getAction());

        nextAction = receiveDownHeartbeat(HostAndPort("host2"), "rs0");
        ASSERT_EQUALS(HeartbeatResponseAction::StartElection, nextAction.getAction());
    }

    class PrepareElectResponseTest : public TopoCoordTest {
    public:

        PrepareElectResponseTest() :
            now(0),
            round(OID::gen()),
            cbData(NULL, ReplicationExecutor::CallbackHandle(), Status::OK()) {}

        virtual void setUp() {
            TopoCoordTest::setUp();
            updateConfig(BSON("_id" << "rs0" <<
                              "version" << 10 <<
                              "members" << BSON_ARRAY(
                                  BSON("_id" << 0 << "host" << "hself") <<
                                  BSON("_id" << 1 << "host" << "h1") <<
                                  BSON("_id" << 2 <<
                                       "host" << "h2" <<
                                       "priority" << 10) <<
                                  BSON("_id" << 3 <<
                                       "host" << "h3" <<
                                       "priority" << 10))),
                         0);
        }

    protected:
        Date_t now;
        OID round;
        ReplicationExecutor::CallbackData cbData;
    };

    TEST_F(PrepareElectResponseTest, IncorrectReplSetName) {
        // Test with incorrect replset name
        ReplicationCoordinator::ReplSetElectArgs args;
        args.set = "fakeset";
        args.round = round;

        BSONObjBuilder responseBuilder;
        startCapturingLogMessages();
        getTopoCoord().prepareElectResponse(cbData, args, now++, &responseBuilder);
        stopCapturingLogMessages();
        BSONObj response = responseBuilder.obj();
        ASSERT_EQUALS(0, response["vote"].Int());
        ASSERT_EQUALS(round, response["round"].OID());
        ASSERT_EQUALS(1,
                      countLogLinesContaining("received an elect request for 'fakeset' but our "
                              "set name is 'rs0'"));
    }

    TEST_F(PrepareElectResponseTest, OurConfigStale) {
        // Test with us having a stale config version
        ReplicationCoordinator::ReplSetElectArgs args;
        args.set = "rs0";
        args.round = round;
        args.cfgver = 20;

        BSONObjBuilder responseBuilder;
        startCapturingLogMessages();
        getTopoCoord().prepareElectResponse(cbData, args, now++, &responseBuilder);
        stopCapturingLogMessages();
        BSONObj response = responseBuilder.obj();
        ASSERT_EQUALS(0, response["vote"].Int());
        ASSERT_EQUALS(round, response["round"].OID());
        ASSERT_EQUALS(1,
                      countLogLinesContaining("not voting because our config version is stale"));
    }

    TEST_F(PrepareElectResponseTest, TheirConfigStale) {
        // Test with them having a stale config version
        ReplicationCoordinator::ReplSetElectArgs args;
        args.set = "rs0";
        args.round = round;
        args.cfgver = 5;

        BSONObjBuilder responseBuilder;
        startCapturingLogMessages();
        getTopoCoord().prepareElectResponse(cbData, args, now++, &responseBuilder);
        stopCapturingLogMessages();
        BSONObj response = responseBuilder.obj();
        ASSERT_EQUALS(-10000, response["vote"].Int());
        ASSERT_EQUALS(round, response["round"].OID());
        ASSERT_EQUALS(1,
                      countLogLinesContaining("received stale config version # during election"));
    }

    TEST_F(PrepareElectResponseTest, NonExistentNode) {
        // Test with a non-existent node
        ReplicationCoordinator::ReplSetElectArgs args;
        args.set = "rs0";
        args.round = round;
        args.cfgver = 10;
        args.whoid = 99;

        BSONObjBuilder responseBuilder;
        startCapturingLogMessages();
        getTopoCoord().prepareElectResponse(cbData, args, now++, &responseBuilder);
        stopCapturingLogMessages();
        BSONObj response = responseBuilder.obj();
        ASSERT_EQUALS(-10000, response["vote"].Int());
        ASSERT_EQUALS(round, response["round"].OID());
        ASSERT_EQUALS(1, countLogLinesContaining("couldn't find member with id 99"));
    }

    TEST_F(PrepareElectResponseTest, WeArePrimary) {
        // Test when we are already primary
        ReplicationCoordinator::ReplSetElectArgs args;
        args.set = "rs0";
        args.round = round;
        args.cfgver = 10;
        args.whoid = 1;

        getTopoCoord()._setCurrentPrimaryForTest(0);

        BSONObjBuilder responseBuilder;
        startCapturingLogMessages();
        getTopoCoord().prepareElectResponse(cbData, args, now++, &responseBuilder);
        stopCapturingLogMessages();
        BSONObj response = responseBuilder.obj();
        ASSERT_EQUALS(-10000, response["vote"].Int());
        ASSERT_EQUALS(round, response["round"].OID());
        ASSERT_EQUALS(1, countLogLinesContaining("I am already primary"));
    }

    TEST_F(PrepareElectResponseTest, SomeoneElseIsPrimary) {
        // Test when someone else is already primary
        ReplicationCoordinator::ReplSetElectArgs args;
        args.set = "rs0";
        args.round = round;
        args.cfgver = 10;
        args.whoid = 1;
        getTopoCoord()._setCurrentPrimaryForTest(2);

        BSONObjBuilder responseBuilder;
        startCapturingLogMessages();
        getTopoCoord().prepareElectResponse(cbData, args, now++, &responseBuilder);
        stopCapturingLogMessages();
        BSONObj response = responseBuilder.obj();
        ASSERT_EQUALS(-10000, response["vote"].Int());
        ASSERT_EQUALS(round, response["round"].OID());
        ASSERT_EQUALS(1, countLogLinesContaining("h2:27017 is already primary"));
    }

    TEST_F(PrepareElectResponseTest, NotHighestPriority) {
        // Test trying to elect someone who isn't the highest priority node
        ReplicationCoordinator::ReplSetElectArgs args;
        args.set = "rs0";
        args.round = round;
        args.cfgver = 10;
        args.whoid = 1;

        heartbeatFromMember(HostAndPort("h3"), "rs0", MemberState::RS_SECONDARY, jsTime());

        BSONObjBuilder responseBuilder;
        startCapturingLogMessages();
        getTopoCoord().prepareElectResponse(cbData, args, now++, &responseBuilder);
        stopCapturingLogMessages();
        BSONObj response = responseBuilder.obj();
        ASSERT_EQUALS(-10000, response["vote"].Int());
        ASSERT_EQUALS(round, response["round"].OID());
        ASSERT_EQUALS(1, countLogLinesContaining("h1:27017 has lower priority than h3:27017"));
    }

    TEST_F(PrepareElectResponseTest, ValidVotes) {
        // Test a valid vote
        ReplicationCoordinator::ReplSetElectArgs args;
        args.set = "rs0";
        args.round = round;
        args.cfgver = 10;
        args.whoid = 2;
        now = 100;

        BSONObjBuilder responseBuilder1;
        startCapturingLogMessages();
        getTopoCoord().prepareElectResponse(cbData, args, now++, &responseBuilder1);
        stopCapturingLogMessages();
        BSONObj response1 = responseBuilder1.obj();
        ASSERT_EQUALS(1, response1["vote"].Int());
        ASSERT_EQUALS(round, response1["round"].OID());
        ASSERT_EQUALS(1, countLogLinesContaining("voting yea for h2:27017 (2)"));

        // Test what would be a valid vote except that we already voted too recently
        args.whoid = 3;

        BSONObjBuilder responseBuilder2;
        startCapturingLogMessages();
        getTopoCoord().prepareElectResponse(cbData, args, now++, &responseBuilder2);
        stopCapturingLogMessages();
        BSONObj response2 = responseBuilder2.obj();
        ASSERT_EQUALS(0, response2["vote"].Int());
        ASSERT_EQUALS(round, response2["round"].OID());
        ASSERT_EQUALS(1, countLogLinesContaining("voting no for h3:27017; "
                "voted for h2:27017 0 secs ago"));

        // Test that after enough time passes the same vote can proceed
        now = Date_t(now.millis + 3 * 1000); // 3 seconds later

        BSONObjBuilder responseBuilder3;
        startCapturingLogMessages();
        getTopoCoord().prepareElectResponse(cbData, args, now++, &responseBuilder3);
        stopCapturingLogMessages();
        BSONObj response3 = responseBuilder3.obj();
        ASSERT_EQUALS(1, response3["vote"].Int());
        ASSERT_EQUALS(round, response3["round"].OID());
        ASSERT_EQUALS(1, countLogLinesContaining("voting yea for h3:27017 (3)"));
    }

    class PrepareFreezeResponseTest : public TopoCoordTest {
    public:

        virtual void setUp() {
            TopoCoordTest::setUp();
            updateConfig(BSON("_id" << "rs0" <<
                              "version" << 5 <<
                              "members" << BSON_ARRAY(
                                  BSON("_id" << 0 << "host" << "host1:27017") <<
                                  BSON("_id" << 1 << "host" << "host2:27017"))),
                         0);
        }

        BSONObj prepareFreezeResponse(int duration,
                                      Status& result) {
            BSONObjBuilder response;
            startCapturingLogMessages();
            getTopoCoord().prepareFreezeResponse(cbData(), now()++, duration, &response, &result);
            stopCapturingLogMessages();
            return response.obj();
        }
    };

    TEST_F(PrepareFreezeResponseTest, UnfreezeEvenWhenNotFrozen) {
        Status result = Status(ErrorCodes::InternalError, "");
        BSONObj response = prepareFreezeResponse(0, result);
        ASSERT_EQUALS(Status::OK(), result);
        ASSERT_EQUALS("unfreezing", response["info"].String());
        ASSERT_EQUALS(1, countLogLinesContaining("replSet info 'unfreezing'"));
    }

    TEST_F(PrepareFreezeResponseTest, FreezeForOneSecond) {
        Status result = Status(ErrorCodes::InternalError, "");
        BSONObj response = prepareFreezeResponse(1, result);
        ASSERT_EQUALS(Status::OK(), result);
        ASSERT_EQUALS("you really want to freeze for only 1 second?",
                      response["warning"].String());
        ASSERT_EQUALS(1, countLogLinesContaining("replSet info 'freezing' for 1 seconds"));
    }

    TEST_F(PrepareFreezeResponseTest, FreezeForManySeconds) {
        Status result = Status(ErrorCodes::InternalError, "");
        BSONObj response = prepareFreezeResponse(20, result);
        ASSERT_EQUALS(Status::OK(), result);
        ASSERT_TRUE(response.isEmpty());
        ASSERT_EQUALS(1, countLogLinesContaining("replSet info 'freezing' for 20 seconds"));
    }

    TEST_F(PrepareFreezeResponseTest, UnfreezeEvenWhenNotFrozenWhilePrimary) {
        makeSelfPrimary();
        Status result = Status(ErrorCodes::InternalError, "");
        BSONObj response = prepareFreezeResponse(0, result);
        ASSERT_EQUALS(Status::OK(), result);
        ASSERT_EQUALS("unfreezing", response["info"].String());
        // doesn't mention being primary in this case for some reason
        ASSERT_EQUALS(0, countLogLinesContaining(
                "replSet info received freeze command but we are primary"));
    }

    TEST_F(PrepareFreezeResponseTest, FreezeForOneSecondWhilePrimary) {
        makeSelfPrimary();
        Status result = Status(ErrorCodes::InternalError, "");
        BSONObj response = prepareFreezeResponse(1, result);
        ASSERT_EQUALS(Status::OK(), result);
        ASSERT_EQUALS("you really want to freeze for only 1 second?",
                      response["warning"].String());
        ASSERT_EQUALS(1, countLogLinesContaining(
                "replSet info received freeze command but we are primary"));
    }

    TEST_F(PrepareFreezeResponseTest, FreezeForManySecondsWhilePrimary) {
        makeSelfPrimary();
        Status result = Status(ErrorCodes::InternalError, "");
        BSONObj response = prepareFreezeResponse(20, result);
        ASSERT_EQUALS(Status::OK(), result);
        ASSERT_TRUE(response.isEmpty());
        ASSERT_EQUALS(1, countLogLinesContaining(
                "replSet info received freeze command but we are primary"));
    }

    class ShutdownInProgressTest : public TopoCoordTest {
    public:

        ShutdownInProgressTest() :
            ourCbData(NULL,
                      ReplicationExecutor::CallbackHandle(),
                      Status(ErrorCodes::CallbackCanceled, "")) {}

        virtual ReplicationExecutor::CallbackData cbData() { return ourCbData; }

    private:
        ReplicationExecutor::CallbackData ourCbData;
    };

    TEST_F(ShutdownInProgressTest, ShutdownInProgressWhenCallbackCanceledSyncFrom) {
        Status result = Status::OK();
        BSONObjBuilder response;
        getTopoCoord().prepareSyncFromResponse(cbData(),
                                               HostAndPort("host2:27017"),
                                               OpTime(0,0),
                                               &response,
                                               &result);
        ASSERT_EQUALS(ErrorCodes::ShutdownInProgress, result);
        ASSERT_TRUE(response.obj().isEmpty());

    }

    TEST_F(ShutdownInProgressTest, ShutDownInProgressWhenCallbackCanceledFresh) {
        Status result = Status::OK();
        BSONObjBuilder response;
        getTopoCoord().prepareFreshResponse(cbData(),
                                            ReplicationCoordinator::ReplSetFreshArgs(),
                                            OpTime(0,0),
                                            &response,
                                            &result);
        ASSERT_EQUALS(ErrorCodes::ShutdownInProgress, result);
        ASSERT_TRUE(response.obj().isEmpty());
    }

    TEST_F(ShutdownInProgressTest, ShutDownInProgressWhenCallbackCanceledElectCmd) {
        Status result = Status::OK();
        BSONObjBuilder response;
        getTopoCoord().prepareFreshResponse(cbData(),
                                            ReplicationCoordinator::ReplSetFreshArgs(),
                                            OpTime(0,0),
                                            &response,
                                            &result);
        ASSERT_EQUALS(ErrorCodes::ShutdownInProgress, result);
        ASSERT_TRUE(response.obj().isEmpty());
    }

    TEST_F(ShutdownInProgressTest, ShutDownInProgressWhenCallbackCanceledHeartbeat) {
        Status result = Status::OK();
        BSONObjBuilder response;
        getTopoCoord().prepareFreshResponse(cbData(),
                                            ReplicationCoordinator::ReplSetFreshArgs(),
                                            OpTime(0,0),
                                            &response,
                                            &result);
        ASSERT_EQUALS(ErrorCodes::ShutdownInProgress, result);
        ASSERT_TRUE(response.obj().isEmpty());
    }

    TEST_F(ShutdownInProgressTest, ShutDownInProgressWhenCallbackCanceledStatus) {
        Status result = Status::OK();
        BSONObjBuilder response;
        getTopoCoord().prepareFreshResponse(cbData(),
                                            ReplicationCoordinator::ReplSetFreshArgs(),
                                            OpTime(0,0),
                                            &response,
                                            &result);
        ASSERT_EQUALS(ErrorCodes::ShutdownInProgress, result);
        ASSERT_TRUE(response.obj().isEmpty());
    }

    TEST_F(ShutdownInProgressTest, ShutDownInProgressWhenCallbackCanceledFreeze) {
        Status result = Status::OK();
        BSONObjBuilder response;
        getTopoCoord().prepareFreshResponse(cbData(),
                                            ReplicationCoordinator::ReplSetFreshArgs(),
                                            OpTime(0,0),
                                            &response,
                                            &result);
        ASSERT_EQUALS(ErrorCodes::ShutdownInProgress, result);
        ASSERT_TRUE(response.obj().isEmpty());

    }

}  // namespace
}  // namespace repl
}  // namespace mongo
