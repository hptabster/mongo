'use strict';

load('jstests/concurrency/fsm_libs/runner.js');

var dir = 'jstests/concurrency/fsm_workloads';

var blacklist = [
    // Disabled due to known bugs
    'agg_match.js', // SERVER-3645 .count() can be wrong on sharded collections
    'count.js', // SERVER-3645 .count() can be wrong on sharded collections
    'count_limit_skip.js', // SERVER-3645 .count() can be wrong on sharded collections
    'count_noindex.js', // SERVER-3645 .count() can be wrong on sharded collections
    'yield_sort.js', // SERVER-17011 Cursor can return objects out of order if updated during query
    'yield_sort_merge.js', // SERVER-17011 also applies, since this query uses SORT stage,
                           // not SORT_MERGE stage in sharded environment

    // Disabled due to MongoDB restrictions and/or workload restrictions

    // These workloads sometimes trigger 'Could not lock auth data update lock'
    // errors because the AuthorizationManager currently waits for only five
    // seconds to acquire the lock for authorization documents
    'auth_create_role.js',
    'auth_create_user.js',
    'auth_drop_role.js',
    'auth_drop_user.js', // SERVER-16739 OpenSSL libcrypto crash

    'agg_group_external.js', // uses >100MB of data, and is flaky
    'agg_sort_external.js', // uses >100MB of data, and is flaky
    'compact.js', // compact can only be run against a standalone mongod
    'compact_simultaneous_padding_bytes.js', // compact can only be run against a mongod
    'convert_to_capped_collection.js', // convertToCapped can't be run on mongos processes
    'convert_to_capped_collection_index.js', // convertToCapped can't be run on mongos processes
    'findAndModify_remove_queue.js', // remove cannot be {} for findAndModify
    'findAndModify_update_queue.js', // remove cannot be {} for findAndModify
    'group.js', // the group command cannot be issued against a sharded cluster
    'group_cond.js', // the group command cannot be issued against a sharded cluster
    'indexed_insert_eval.js', // eval doesn't work with sharded collections
    'indexed_insert_eval_nolock.js', // eval doesn't work with sharded collections
    'plan_cache_drop_database.js', // doesn't work with sharded collections
    'remove_single_document.js', // remove justOne doesn't work with sharded collections
    'remove_single_document_eval.js', // eval doesn't work with sharded collections
    'remove_single_document_eval_nolock.js', // eval doesn't work with sharded collections
    'rename_capped_collection_dbname_droptarget.js', // renameCollection cannot be used if sharded
    'rename_capped_collection_dbname_chain.js', // renameCollection cannot be used if sharded
    'rename_collection_dbname_chain.js', // renameCollection cannot be used if sharded
    'rename_collection_dbname_droptarget.js', // renameCollection cannot be used if sharded
    'update_simple_eval.js', // eval doesn't work with sharded collections
    'update_simple_eval_nolock.js', // eval doesn't work with sharded collections
    'upsert_where.js', // upsert command specified doesn't work with sharded collections
    'yield_and_hashed.js', // stagedebug can only be run against a standalone mongod
    'yield_and_sorted.js', // stagedebug can only be run against a standalone mongod

    // Other failing tests

    'update_upsert_multi_noindex.js', //failed on Windows: mmap compat, WT compat; Linux mmap compat, WT compat

].map(function(file) { return dir + '/' + file; });

runWorkloadsSerially(ls(dir).filter(function(file) {
     return !Array.contains(blacklist, file);
}), { sharded: true });
