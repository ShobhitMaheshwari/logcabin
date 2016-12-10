/* Copyright (c) 2012 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "build/Protocol/Raft.pb.h"
#include "Core/Debug.h"
#include "Core/ProtoBuf.h"
#include "RPC/ServerRPC.h"
#include "Server/RaftConsensus.h"
#include "Server/RaftService.h"
#include "Server/Globals.h"
#include "Server/StateMachine.h"

namespace LogCabin {
namespace Server {

RaftService::RaftService(Globals& globals)
    : globals(globals)
{
}

RaftService::~RaftService()
{
}

void
RaftService::handleRPC(RPC::ServerRPC rpc)
{
    using Protocol::Raft::OpCode;

    // Call the appropriate RPC handler based on the request's opCode.
    switch (rpc.getOpCode()) {
        case OpCode::APPEND_ENTRIES:
            appendEntries(std::move(rpc));
            break;
        case OpCode::INSTALL_SNAPSHOT:
            installSnapshot(std::move(rpc));
            break;
        case OpCode::REQUEST_VOTE:
            requestVote(std::move(rpc));
            break;
        case OpCode::REQUEST_WEIGHT:
            requestWeight(std::move(rpc));
            break;
        default:
            WARNING("Client sent request with bad op code (%u) to RaftService",
                    rpc.getOpCode());
            rpc.rejectInvalidRequest();
    }
}

std::string
RaftService::getName() const
{
    return "RaftService";
}

/**
 * Place this at the top of each RPC handler. Afterwards, 'request' will refer
 * to the protocol buffer for the request with all required fields set.
 * 'response' will be an empty protocol buffer for you to fill in the response.
 */
#define PRELUDE(rpcClass) \
    Protocol::Raft::rpcClass::Request request; \
    Protocol::Raft::rpcClass::Response response; \
    if (!rpc.getRequest(request)) \
        return;

////////// RPC handlers //////////

void
RaftService::appendEntries(RPC::ServerRPC rpc)
{
    PRELUDE(AppendEntries);
    //VERBOSE("AppendEntries:\n%s",
    //        Core::ProtoBuf::dumpString(request).c_str());
    globals.raft->handleAppendEntries(request, response);
    rpc.reply(response);
}

void
RaftService::installSnapshot(RPC::ServerRPC rpc)
{
    PRELUDE(InstallSnapshot);
    //VERBOSE("InstallSnapshot:\n%s",
    //        Core::ProtoBuf::dumpString(request).c_str());
    globals.raft->handleInstallSnapshot(request, response);
    rpc.reply(response);
}

void
RaftService::requestVote(RPC::ServerRPC rpc)
{
    PRELUDE(RequestVote);
    //VERBOSE("RequestVote:\n%s",
    //        Core::ProtoBuf::dumpString(request).c_str());
    globals.raft->handleRequestVote(request, response);
    rpc.reply(response);
}

void
RaftService::requestWeight(RPC::ServerRPC rpc)
{
    PRELUDE(RequestWeight);
//    //VERBOSE("RequestVote:\n%s",
//    //        Core::ProtoBuf::dumpString(request).c_str());
//    globals.raft->handleRequestWeight(request, response);
//    rpc.reply(response);
//}
//
//        void
//        RaftService::requestWeight(RPC::ServerRPC rpc)
//        {
//    Protocol::Client::StateMachineQuery::Request request;
//    Protocol::Client::StateMachineQuery::Response response;
//    if (!rpc.getRequest(request))
//        return;
//            std::pair<Result, uint64_t> result = globals.raft->getLastCommitIndex();
//            if (result.first == Result::RETRY || result.first == Result::NOT_LEADER) {
//                Protocol::Client::Error error;
//                error.set_error_code(Protocol::Client::Error::NOT_LEADER);
//                std::string leaderHint = globals.raft->getLeaderHint();
//                if (!leaderHint.empty())
//                    error.set_leader_hint(leaderHint);
//                rpc.returnError(error);
//                return;
//            }
//            assert(result.first == Result::SUCCESS);
//            uint64_t logIndex = result.second;
    if (globals.stateMachine)
    {
        globals.stateMachine->wait(request.prev_log_index());
        response.set_value(globals.stateMachine->query2(request.key()));

    }
    rpc.reply(response);
}


} // namespace LogCabin::Server
} // namespace LogCabin
