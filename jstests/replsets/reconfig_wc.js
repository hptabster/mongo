// Tests different configuration of a replSet with
// replSetReconfig and verifies writeConcern

function addTagset(replSet, tags, tagKey) {
    var primary = replSet.getPrimary();
    var conf = getReplSetConf(primary);
    // Add tag set
    for (var i=0; i<conf.members.length; i++) {
        // Set each member's tag
        conf.members[i].tags = tags[i%tags.length];
    }
    conf.settings = {getLastErrorModes: tagKey};
    conf.version++;
    print("Adding tagset:", tojson(conf));
    primary.adminCommand({replSetReconfig: conf});
}

function waitForAllMembers(replSet, version, timeout, interval) {
    print("Waiting for all members to have this config version", version);

    for (var i=0; i<replSet.nodes.length; i++) {
        var conn = replSet.nodes[i];
        assert.soon(function() {
            curConf = getReplSetConf(conn);
            if (curConf.version == version) {
                return true;
            }
            return false;
        }, "not all members ready", timeout || 5000, interval || 500);
    }

    print( "All members have updated configuration" );
}

function setDelayAndPriority(member, delay) {
    if (delay > 0) {
        member.slaveDelay = delay;
        member.priority = 0;
    } else {
        delete member.slaveDelay;
        delete member.priority;
    }
   return member;
}

function getReplSetConf(conn) {
    return conn.getDB("local").system.replset.findOne();
}

function setTagsetConf(replSet, conf, writeConcern, delay) {
    var tagSet = writeConcern.w;
    // Initialize all members to be slave delayed, except primary
    for (var i = 0; i < conf.members.length; i++) {
        // Primary is never slave delayed
        if (replSet.nodes[i].host == replSet.getPrimary().host) {
            conf.members[i] = setDelayAndPriority(conf.members[i], 0);
        } else {
        conf.members[i] = setDelayAndPriority(conf.members[i], delay);
        }
    }
    for (var key in tagKey[tagSet]) {
        // Determine the number of nodes in the writeConcern for the tagKey
        // i.e, if tagKey = {maindc: {main: 1}}
        //      set only one node to be in the writeConcern with
        //      a tag of main, the others will be slave delayed
        nodesInWrite = Math.max(1, tagKey[tagSet][key]);
        var tagIdx = 0;
        // Unset slaveDelay for all members matching the tag inside the writeConcern
        for (var i = 0; i < conf.members.length; i++) {
            var member = conf.members[i];
            if ("tags" in member &&
              member.tags[key] &&
              tagIdx < nodesInWrite) {
                conf.members[i] = setDelayAndPriority(member, 0);
                tagIdx++;
            }
        }
    }
    return conf;
}

function setConf(replSet, conf, nodesInWrite, delay) {
    // Set each member's slaveDelay & priority
    for (var i = 0; i < conf.members.length; i++) {
        if (i < nodesInWrite ||
          replSet.nodes[i].host == replSet.getPrimary().host) {
            // Primary is never slaved delayed
            conf.members[i] = setDelayAndPriority(conf.members[i], 0);
        } else {
            conf.members[i] = setDelayAndPriority(conf.members[i], delay);
        }
    }
    return conf;
}

function setSlaveDelay(replSet, writeConcern, delay) {
    var primary = replSet.getPrimary();
    var conf = getReplSetConf(primary);
    var nodesInWrite = 0;
    var isTagSet = false;

    if (typeof writeConcern.w === "number") {
        nodesInWrite = Math.min(writeConcern.w, conf.members.length);
    } else {
        if (writeConcern.w == "majority") {
            nodesInWrite = Math.round(conf.members.length/2);
        } else {
            // write concern is a tag set
            isTagSet = true;
            conf = setTagsetConf(replSet, conf, writeConcern, delay);
        }
    }
    // At least one member of set has nodelay
    nodesInWrite = Math.max(1, nodesInWrite);
    // Set slaveDelay for non-tag write concerns
    if (!isTagSet) {
        conf = setConf(replSet, conf, nodesInWrite, delay);
    }
    conf.version++;
    print("Reconfiguring replSet nodesInWrite: ", nodesInWrite, tojson(conf));
    try {
        var result = primary.adminCommand({replSetReconfig: conf, force: false});
        assert.commandWorked(result);
    }
    catch(err) {
        print("Error reconfiguring:", err);
    }

    // Wait until all memberd of replSet are ready
    waitForAllMembers(replSet, conf.version);

    // Wait for replSet to come back
    print("Wait completed!");
}

function checkHost(conn, collName, numDocs) {
    // Read from host, to check num of docs
    conn.setSlaveOk();
    var numOnHost = conn.getDB("test")[collName].find().itcount();
    //print("****Num of docs, on host",conn.host,numOnHost,"****");
    assert.eq(numDocs, numOnHost, "Num of docs, on host "+conn.host);
}

function checkReplSet(replSet, collName, docsInWrite, docsOutsideWrite) {
    var conf = getReplSetConf(replSet.getPrimary());
    // Check all nodes for number of docs
    for (i=0; i<replSet.nodes.length; i++) {
        //print("Configuration of replSet member",tojson(conf.members[i]));
        if ("slaveDelay" in conf.members[i] && conf.members[i].slaveDelay > 0) {
            checkHost(replSet.nodes[i], collName, docsOutsideWrite);
        } else {
            checkHost(replSet.nodes[i], collName, docsInWrite);
        }
    }
}

function runTest(test) {

    jsTest.log("Test:"+tojson(test));
    var primary = replSet.getPrimary();
    // Write data to primary node
    var collName = "replSet_WriteConcern";
    var coll = primary.getDB("test")[collName];
    coll.ensureIndex({replKey: 1});
    var numDocs = 1000;
    var delay = 60*60*10; // 10 hour delay
    var wc = test.writeConcern;
    var writeAll = {writeConcern: {w: numNodes}};

    // Bulk write unordered
    setSlaveDelay(replSet, test.writeConcern, delay);
    primary = replSet.getPrimary();
    coll = primary.getDB("test")[collName];
    var bulk = coll.initializeUnorderedBulkOp();
    for (var i=0; i<numDocs; i++) {
        bulk.insert({replKey: i, data: 'repl info '+i});
    }
    assert.writeOK(bulk.execute({w: wc.w}));
    checkReplSet(replSet, collName, numDocs, 0);

    // Cleanup docs
    coll.remove({}, {writeConcern: wc});
    checkReplSet(replSet, collName, 0, 0);

    // Bulk write ordered
    bulk = coll.initializeOrderedBulkOp();
    for (i=0; i<numDocs; i++) {
        bulk.insert({replKey: i, data: 'repl info '+i});
    }
    assert.writeOK(bulk.execute({w: wc.w}));
    checkReplSet(replSet, collName, numDocs, 0);

    // Cleanup collection
    setSlaveDelay(replSet, writeAll.writeConcern, 0);
    primary = replSet.getPrimary();
    coll = primary.getDB("test")[collName];
    coll.remove({}, writeAll);
    checkReplSet(replSet, collName, 0, 0);
    coll.drop();
}

// Main
var numNodes = 5;
// Tags used to represent 2 datacenters: main & backup
var tags = [{"main": "NY"}, {"backup": "SF"}];
// Tag keys used to specify the writeConcern for different tests
//   1. maindc - writeConcern of 1 node in main datacenter
//   2. alldc -  writeConcern of 1 node in man & backup datacenters
var tagKey = {"maindc": {"main": 1}, "alldc": {"main": 1, "backup": 1}};

var tests = [
    {name: "Write all",
     writeConcern: {w: numNodes, wtimeout: 0}
    },
    {name: "Write 1",
     writeConcern: {w: 1, wtimeout: 0}
    },
    {name: "Write 2",
     writeConcern: {w: 2, wtimeout: 0}
    },
    {name: "Write majority",
     writeConcern: {w: "majority", wtimeout: 0}
    },
    {name: "Tag set - maindc",
     writeConcern: {w: "maindc", wtimeout: 0}
    },
    {name: "Tag set - alldc",
     writeConcern: {w: "alldc", wtimeout: 0}
    },
];

// Start up replica set
var replSet = new ReplSetTest({ name: 'reconfig', nodes: numNodes });
replSet.startSet({nojournal: "" });
replSet.initiate();

// Update Replication set configuration with tag sets
addTagset(replSet, tags, tagKey);

// Execute all tests
tests.forEach(function(test) {
    runTest(test);
});

replSet.stopSet();
