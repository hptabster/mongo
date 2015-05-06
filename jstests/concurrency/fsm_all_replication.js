'use strict';

load('jstests/concurrency/fsm_libs/runner.js');

var dir = 'jstests/concurrency/fsm_workloads';

var blacklist = [
    // Disabled due to known bugs
    'yield_sort.js', // SERVER-17011 Cursor can return objects out of order if updated during query

    // Disabled due to MongoDB restrictions and/or workload restrictions

    // These workloads sometimes trigger 'Could not lock auth data update lock'
    // errors because the AuthorizationManager currently waits for only five
    // seconds to acquire the lock for authorization documents
    'auth_create_role.js',
    'auth_create_user.js',
    'auth_drop_role.js',
    'auth_drop_user.js', // SERVER-16739 OpenSSL libcrypto crash

    // Other failing tests

    'yield.js', // Failed on Linux mmap compat
    /*
2015-05-06T04:53:22.883+0000 E QUERY    Error: DBClientBase::findN: transport error: 127.0.0.1:31001 ns: admin.$cmd query: { configureFailPoint: "recordNeedsFetchFail", mode: "alwaysOn" }
    at DB.runCommand (src/mongo/shell/db.js:112:29)
    at DB.adminCommand (src/mongo/shell/db.js:121:21)
    at enableFailPoint (jstests/concurrency/fsm_workloads/yield.js:132:20)
    at jstests/concurrency/fsm_libs/cluster.js:200:13
    at Array.forEach (native)
    at executeOnMongodNodes (jstests/concurrency/fsm_libs/cluster.js:199:23)
    at Object.setup (jstests/concurrency/fsm_workloads/yield.js:130:17)
    at setupWorkload (jstests/concurrency/fsm_libs/runner.js:286:22)
    at jstests/concurrency/fsm_libs/runner.js:361:25
    at Array.forEach (native) at src/mongo/shell/db.js:112
failed to load: /data/mci/shell/src/jstests/concurrency/fsm_all_replication.js
   */
].map(function(file) { return dir + '/' + file; });

runWorkloadsSerially(ls(dir).filter(function(file) {
     return !Array.contains(blacklist, file);
}), { replication: true });
