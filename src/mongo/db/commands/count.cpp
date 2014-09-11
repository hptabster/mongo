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

#include "mongo/db/commands/count.h"

#include "mongo/db/catalog/database.h"
#include "mongo/db/client.h"
#include "mongo/db/curop.h"
#include "mongo/db/query/get_executor.h"
#include "mongo/db/query/explain.h"
#include "mongo/util/log.h"

namespace mongo {

    static CmdCount cmdCount;

    static long long applySkipLimit(long long num, const BSONObj& cmd) {
        BSONElement s = cmd["skip"];
        BSONElement l = cmd["limit"];

        if (s.isNumber()) {
            num = num - s.numberLong();
            if (num < 0) {
                num = 0;
            }
        }

        if (l.isNumber()) {
            long long limit = l.numberLong();
            if (limit < 0) {
                limit = -limit;
            }

            // 0 means no limit.
            if (limit < num && limit != 0) {
                num = limit;
            }
        }

        return num;
    }

    long long runCount(OperationContext* txn,
                       const string& ns,
                       const BSONObj &cmd,
                       string &err,
                       int &errCode) {
        // Lock 'ns'.
        Client::Context cx(txn, ns);
        Collection* collection = cx.db()->getCollection(txn, ns);
        const string& dbname = cx.db()->name();

        if (NULL == collection) {
            err = "ns missing";
            return -1;
        }

        CountRequest request;
        CmdCount* countComm = static_cast<CmdCount*>(Command::findCommand("count"));
        Status parseStatus = countComm->parseRequest(dbname, cmd, &request);
        if (!parseStatus.isOK()) {
            err = parseStatus.reason();
            errCode = parseStatus.code();
            return -1;
        }

        if (request.query.isEmpty()) {
            return applySkipLimit(collection->numRecords(txn), cmd);
        }

        PlanExecutor* rawExec;
        Status getExecStatus = getExecutorCount(txn, collection, request, &rawExec);
        if (!getExecStatus.isOK()) {
            err = getExecStatus.reason();
            errCode = parseStatus.code();
            return -1;
        }
        scoped_ptr<PlanExecutor> exec(rawExec);

        // Store the plan summary string in CurOp.
        if (NULL != txn->getCurOp()) {
            PlanSummaryStats stats;
            Explain::getSummaryStats(exec.get(), &stats);
            txn->getCurOp()->debug().planSummary = stats.summaryStr.c_str();
        }

        const ScopedExecutorRegistration safety(exec.get());

        Status execPlanStatus = exec->executePlan();
        if (!execPlanStatus.isOK()) {
            err = execPlanStatus.reason();
            errCode = execPlanStatus.code();
            return -2;
        }

        // Plan is done executing. We just need to pull the count out of the root stage.
        invariant(STAGE_COUNT == exec->getRootStage()->stageType());
        CountStage* countStage = static_cast<CountStage*>(exec->getRootStage());
        const CountStats* countStats =
            static_cast<const CountStats*>(countStage->getSpecificStats());

        return countStats->nCounted;
    }

    Status CmdCount::parseRequest(const std::string& dbname,
                                  const BSONObj& cmdObj,
                                  CountRequest* request) const {
        const string ns = parseNs(dbname, cmdObj);

        long long skip = 0;
        if (cmdObj["skip"].isNumber()) {
            skip = cmdObj["skip"].numberLong();
            if (skip < 0) {
                return Status(ErrorCodes::BadValue, "skip value is negative in count query");
            }
        }
        else if (cmdObj["skip"].ok()) {
            return Status(ErrorCodes::BadValue, "skip value is not a valid number");
        }

        long long limit = 0;
        if (cmdObj["limit"].isNumber()) {
            limit = cmdObj["limit"].numberLong();
        }
        else if (cmdObj["limit"].ok()) {
            return Status(ErrorCodes::BadValue, "limit value is not a valid number");
        }

        // For counts, limit and -limit mean the same thing.
        if (limit < 0) {
            limit = -limit;
        }

        BSONObj query;
        if (!cmdObj["query"].eoo()) {
            if (Object != cmdObj["query"].type()) {
                return Status(ErrorCodes::BadValue, "query field for count must be an object");
            }
            query = cmdObj.getObjectField("query");
        }

        BSONObj hintObj;
        if (Object == cmdObj["hint"].type()) {
            hintObj = cmdObj["hint"].Obj();
        }
        else if (String == cmdObj["hint"].type()) {
            const std::string hint = cmdObj.getStringField("hint");
            hintObj = BSON("$hint" << hint);
        }

        // Parsed correctly. Fill out 'request' with the results.
        request->ns = ns;
        request->query = query;
        request->hint = hintObj;
        request->limit = limit;
        request->skip = skip;

        return Status::OK();
    }

    bool CmdCount::run(OperationContext* txn,
                       const string& dbname,
                       BSONObj& cmdObj,
                       int, string& errmsg,
                       BSONObjBuilder& result, bool) {
        CountRequest request;
        Status parseStatus = parseRequest(dbname, cmdObj, &request);
        if (!parseStatus.isOK()) {
            return appendCommandStatus(result, parseStatus);
        }

        // Acquire the db read lock.
        Client::ReadContext ctx(txn, request.ns);
        Collection* collection = ctx.ctx().db()->getCollection(txn, request.ns);

        PlanExecutor* rawExec;
        Status getExecStatus = getExecutorCount(txn, collection, request, &rawExec);
        if (!getExecStatus.isOK()) {
            return appendCommandStatus(result, getExecStatus);
        }
        scoped_ptr<PlanExecutor> exec(rawExec);

        // Store the plan summary string in CurOp.
        if (NULL != txn->getCurOp()) {
            PlanSummaryStats stats;
            Explain::getSummaryStats(exec.get(), &stats);
            txn->getCurOp()->debug().planSummary = stats.summaryStr.c_str();
        }

        const ScopedExecutorRegistration safety(exec.get());

        Status execPlanStatus = exec->executePlan();
        if (!execPlanStatus.isOK()) {
            return appendCommandStatus(result, execPlanStatus);
        }

        // Plan is done executing. We just need to pull the count out of the root stage.
        invariant(STAGE_COUNT == exec->getRootStage()->stageType());
        CountStage* countStage = static_cast<CountStage*>(exec->getRootStage());
        const CountStats* countStats =
            static_cast<const CountStats*>(countStage->getSpecificStats());

        result.appendNumber("n", countStats->nCounted);
        return true;
    }

    Status CmdCount::explain(OperationContext* txn,
                             const std::string& dbname,
                             const BSONObj& cmdObj,
                             Explain::Verbosity verbosity,
                             BSONObjBuilder* out) const {
        CountRequest request;
        Status parseStatus = parseRequest(dbname, cmdObj, &request);
        if (!parseStatus.isOK()) {
            return parseStatus;
        }

        // Acquire the db read lock.
        Client::ReadContext ctx(txn, request.ns);
        Collection* collection = ctx.ctx().db()->getCollection(txn, request.ns);

        PlanExecutor* rawExec;
        Status getExecStatus = getExecutorCount(txn, collection, request, &rawExec);
        if (!getExecStatus.isOK()) {
            return getExecStatus;
        }
        scoped_ptr<PlanExecutor> exec(rawExec);

        const ScopedExecutorRegistration safety(exec.get());

        return Explain::explainStages(exec.get(), verbosity, out);
    }

} // namespace mongo
