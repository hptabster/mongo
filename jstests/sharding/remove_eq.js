// Test remove including findAndModify, in sharding context,
// using single value clause queries (SERVER-14973)

function buildQueries(id, values) {
    // Build out different single value clause queries
    // The query key must use dot notation, i.e.,
    //    {"x.y" : {$eq : 2}}

    var queries = [];
    var i = 0;

    // { ID : 1 },
    var q = {};
    setKey(q, id, values[i]);
    queries[i++] = q;

    // { ID : { $eq : 1 } },
    q = {};
    setKey(q, id, { $eq : values[i] });
    queries[i++] = q;

    // { ID : { $all : [1] } },
    q = {};
    setKey(q, id, { $all : [values[i]] });
    queries[i++] = q;

    // { $or : [{ ID : { $eq : 1 } }] },
    q = {};
    var op = {};
    setKey(op, id, { $eq : values[i] });
    q = { $or : [ op ] };
    queries[i++] = q;

    // { $or : [{ ID : 1 }] },
    q = {};
    op = {};
    setKey(op, id, values[i]);
    q = { $or : [ op ] };
    queries[i++] = q;

    // { $and : [{ ID : { $all : [1] } }] }
    op = {};
    setKey(op, id, { $all : [values[i]] });
    q = { $and : [ op ] };
    queries[i++] =q;

    // { $and : [{ ID : 1 }] }
    op = {};
    setKey(op, id, values[i]);
    q = { $and : [ op ] };
    queries[i++] =q;

    return queries;
}

function setByPath(obj, path, value) {
    // path may have dot notation, i.e., 
    // "x.y.z" is expanded to {x: {y: {z: 1}}}
    var parts = path.split('.');
    var o = obj;
    if (parts.length > 1) {
      for (var i = 0; i < parts.length - 1; i++) {
          if (!o[parts[i]])
              o[parts[i]] = {};
          o = o[parts[i]];
      }
    }
    o[parts[parts.length - 1]] = value;
}

function setPaths(obj, paths, value) {
    // set path for each value for insert, i.e.,
    //   {x : {y : 1}}
    //   {x : 1, y : 1}
    var pathArray = paths;

    // compound id is represented as array of paths
    if (!(paths instanceof Array)) {
        pathArray = [paths];
    }
    pathArray.forEach(function(p) {
        setByPath(obj, p, value);
    });
}

function setKey(obj, ids, value) {
    // set the key used for shardKey or a query, i.e.,
    //   {"x.y" : 1}
    //   {"x" :  1, "y" : 1}
    var o = obj;
    // compound id is represented as array of ids
    if (ids instanceof Array) {
        ids.forEach(function(i) {
            o[i] = value;
        });
    } else {
        o[ids] = value;
    }
}

function createArray(min ,max) {
    var a = [];
    for (var i = min; i <= max; i++) {
        a.push(i);
    }
    return a;
}

function runTest(test) {
    jsTest.log(tojson(test));

    // Create collection, define shardkey, pre-split collection, and balance chunks
    var coll = mongos.getCollection("test."+test.name);
    var shardKey = {};
    setKey(shardKey, test.id, 1);
    assert.commandWorked(admin.runCommand({ shardCollection : coll + "", key : shardKey }),
                         "shardCollection " + tojson(coll));
    setKey(shardKey, test.id, test.numDocs / 2);
    assert.commandWorked(admin.runCommand({ split : coll + "", middle : shardKey }),
                         "split on middle key "+tojson(shardKey));
    setKey(shardKey, test.id, test.numDocs);
    assert.commandWorked(admin.runCommand({ moveChunk : coll + "",
                                            find : shardKey,
                                            to : shards[1]._id,
                                            _waitForDelete : true }),
                         "moveChunk");

    st.printShardingStatus();

    var notId = "notAnId";
    // populate collection across 2 chunks (1 in each shard)
    // document will contain {ID : i, notAnId :  i}
    for (var i = 0; i < test.numDocs; i++) {
        var insert = {};
        setPaths(insert, test.id, i);
        insert[notId] = i;
        coll.insert(insert);
    }

    // Remove with shard key
    buildQueries(test.id, createArray(1, 10)).forEach(function(q) {
        assert.eq(1, coll.remove(q).nRemoved, test.name + " remove " + tojson(q));
    });

    buildQueries(test.id, createArray(11, 20)).forEach(function(q) {
        assert.neq(null,
                   coll.findAndModify({query: q, remove: true}),
                   test.name + " findAndModify " + tojson(q));
    });

    // Remove with not existing field
    buildQueries("not_a_field", createArray(41, 50)).forEach(function(q) {
        assert.eq(0, coll.remove(q).nRemoved, test.name + " remove " + tojson(q));
    });

    buildQueries("not_a_field", createArray(51, 60)).forEach(function(q) {
        assert.throws(function() {coll.findAndModify({query: q, remove: true});},
                      [],
                      test.name + " findAndModify " + tojson(q));
    });

    var ids = [];
    if (test.id instanceof Array) {
        test.id.forEach(function (i) {
            ids.push(i + "." + notId);
        });
    } else {
        ids.push(test.id + "." + notId);
    }

    buildQueries(ids, createArray(71, 80)).forEach(function(q) {
        assert.eq(0, coll.remove(q).nRemoved, test.name + " remove " + tojson(q));
    });

    buildQueries(ids, createArray(81, 90)).forEach(function(q) {
        assert.throws(function() {coll.findAndModify({query: q, remove: true});},
                      [],
                      test.name + " findAndModify " + tojson(q));
    });

    // drop the collection
    coll.drop();
}


// Main
var multiVersion = MongoRunner.versionIterator(["2.6","2.8"]);
var options = {separateConfig : true,
               mongosOptions : { binVersion : "2.8" },
               //shardOptions : { binVersion : multiVersion }};
               shardOptions : { binVersion : "2.8" }};

var st = new ShardingTest({shards : 2, mongos : 1, other : options});
st.stopBalancer();

// Enable sharding & set primary shard
var mongos = st.s0;
var admin = mongos.getDB("admin");
var shards = mongos.getCollection("config.shards" ).find().toArray();
assert.commandWorked(admin.runCommand({ enableSharding : "test" }), "enableSharding");
printjson(admin.runCommand({ movePrimary : "test", to : shards[0]._id }));

tests = [
    { name : "_id",
      id : "_id",
      numDocs : 100
    },
    { name : "non_id",
      id : "non_id",
      numDocs : 100
    },
    { name : "nested x.y",
      id : "x.y",
      numDocs : 100
    },
    { name : "compound x,y",
      id : ["x", "y"],
      numDocs : 100
    },
    { name : "compound nested x.a,y.b",
      id : ["x.a", "y.b"],
      numDocs : 100
    }
];

tests.forEach(function(t) {
    runTest(t);
});

st.stop();
