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

].map(function(file) { return dir + '/' + file; });

//runWorkloadsSerially(ls(dir).filter(function(file) {
//    return !Array.contains(blacklist, file);
//}), { masterSlave: true });

// Failed on Windows: WT compat
/*
Workload teardown function threw an exception:
Error: socket exception [SEND_ERROR] for 127.0.0.1:31001
    at DB.runCommand (src/mongo/shell/db.js:112:29)
    at DB.adminCommand (src/mongo/shell/db.js:121:21)
    at disableFailPoint (jstests/concurrency/fsm_workloads/yield.js:164:20)
    at jstests/concurrency/fsm_libs/cluster.js:200:13
    at Array.forEach (native)
    at executeOnMongodNodes (jstests/concurrency/fsm_libs/cluster.js:199:23)
    at Object.teardown (jstests/concurrency/fsm_workloads/yield.js:162:17)
    at teardownWorkload (jstests/concurrency/fsm_libs/runner.js:294:25)
    at jstests/concurrency/fsm_libs/runner.js:375:29
    at Array.forEach (native)
----
jstests/concurrency/fsm_workloads/yield_fetch.js: Workload completed in 17012 ms
*/
