// Test simple findAndModify updates issued through mongos.
// The query must include the shard key.

s = new ShardingTest( "auto1" , 2 , 1 , 1 );

s.adminCommand( { enablesharding: "test" } );
// repeat same tests with hashed shard key, to ensure identical behavior
s.adminCommand( { shardcollection: "test.findAndModify0" , key: { key: 1 } } );
s.adminCommand( { shardcollection: "test.findAndModify1" , key: { key: "hashed" } } );

db = s.getDB( "test" );
for(i=0; i < 2; i++){
    // regular or hashed shard key
    coll = db.getCollection("findAndModify" + i);

    coll.insert({_id:1, key:1});

    // these are both upserts
    coll.save({_id:2, key:2});
    coll.findAndModify({query: {_id:3, key:3},
                        update: {$set: {foo: 'bar'}},
                        upsert: true});

    assert.eq(coll.count(), 3, "count A");
    assert.eq(coll.findOne({_id:3}).key, 3 , "findOne 3 key A");
    assert.eq(coll.findOne({_id:3}).foo, 'bar' , "findOne 3 foo A");

    // update existing using save()
    coll.save({_id:1, key:1, other:1});

    // update existing using update(), requires shard key
    assert.throws(function() {
        coll.findAndModify({query: {_id:2}, update: {key:2, other:2}}); } );

    // do a replacement-style update which queries the shard key and keeps it constant
    coll.save( {_id:4, key:4} );
    coll.findAndModify({query: {key:4}, update: {key:4, other:4}});
    assert.eq( coll.find({key:4, other:4}).count() , 1 , 'replacement update error');
    coll.remove( {_id:4} );

    assert.eq(coll.count(), 3, "count B");

    assert.throws(function() {
        coll.findAndModify({query: { _id: 1, key: 1 }, update: { $set: { key: 2 }}}); });
    assert.eq(coll.findOne({_id:1}).key, 1, 'key unchanged');

    assert.eq(2, coll.findAndModify(
                    { query: { _id: 1, key: 1 }, update: { $set: { foo: 2 }}, new: true}).foo);

    coll.findAndModify( { query: { key: 17 } , update: { $inc: { x: 5 } } , upsert: true } );
    assert.eq( 5 , coll.findOne( { key: 17 } ).x , "up1" );

    // Invalid extraction of exact _id from query
    assert.throws(function() {
        coll.findAndModify({ query: {}, update: {$set: {x: 1}}}); });
    assert.throws(function() {
        coll.findAndModify({ query: {_id: {$gt: ObjectId()}}, update: {$set: {x: 1}}}); });
    assert.throws(function() {
        coll.findAndModify({ query: {_id: {$in: [ObjectId()]}}, update: {$set: {x: 1}}}); });
    assert.throws(function() {
        coll.findAndModify({ query: {$or: [{_id: ObjectId()}, {_id: ObjectId()}]},
                             update: {$set: {x: 1}}}); });
    assert.throws(function() {
        coll.findAndModify({ query: {$and: [{_id: ObjectId()}, {_id: ObjectId()}]},
                             update: {$set: {x: 1}}}); });
    assert.throws(function() {
        coll.findAndModify({ query: {'_id.x': ObjectId()}, update: {$set: {x: 1}}}); });

    // Make sure we can extract exact shard key from certain queries
    assert.eq(1, coll.findAndModify(
                    { query: {key: ObjectId()},
                      update: {$set: {x: 1}},
                      upsert: true,
                      new: true}).x);
    assert.eq(1, coll.findAndModify(
                    { query: {key: {$eq: ObjectId()}},
                      update: {$set: {x: 1}},
                      upsert: true,
                      new: true}).x);
    assert.eq(1, coll.findAndModify(
                    { query: {key: {$all: [ObjectId()]}},
                      update: {$set: {x: 1}},
                      upsert: true,
                      new: true}).x);
    assert.eq(1, coll.findAndModify(
                    { query: {$or: [{key: ObjectId()}]},
                      update: {$set: {x: 1}},
                      upsert: true,
                      new: true}).x);
    assert.eq(1, coll.findAndModify(
                    { query: {$and: [{key: ObjectId()}]},
                      update: {$set: {x: 1}},
                      upsert: true,
                      new: true}).x);

    // Invalid extraction of exact key from query
    assert.throws(function() {
            coll.findAndModify({ query: {}, update: {$set: {x: 1}}}); });
    assert.throws(function() {
            coll.findAndModify({ query: {key: {$gt: ObjectId()}}, update: {$set: {x: 1}}}); });
    assert.throws(function() {
            coll.findAndModify({ query: {key: {$in: [ObjectId()]}}, update: {$set: {x: 1}}}); });
    assert.throws(function() {
            coll.findAndModify({ query: {$or: [{key: ObjectId()}, {key: ObjectId()}]},
                                 update: {$set: {x: 1}}}); });
    assert.throws(function() {
            coll.findAndModify({ query: {$and: [{key: ObjectId()}, {key: ObjectId()}]},
                                 update: {$set: {x: 1}}}); });
    assert.throws(function() {
            coll.findAndModify({ query: {'key.x': ObjectId()}, update: {$set: {x: 1}}}); });

    // Make sure failed shard key or _id extraction doesn't affect the other
    assert.eq(1, coll.findAndModify(
                    { query: {'_id.x': ObjectId(), key: 1},
                      update: {$set: {x: 1}},
                      upsert: true,
                      new: true}).x);
}

s.stop();

