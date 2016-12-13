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

#include <build/Protocol/Raft.pb.h>
#include "Core/Debug.h"
#include "Core/ProtoBuf.h"
#include "Tree/ProtoBuf.h"

namespace LogCabin {
namespace Tree {
namespace ProtoBuf {

namespace PC = LogCabin::Protocol::Client;

void
readOnlyTreeRPC(const Tree& tree,
                const PC::ReadOnlyTree::Request& request,
                PC::ReadOnlyTree::Response& response)
{
    Result result;
    if (request.has_condition()) {
        result = tree.checkCondition(request.condition().path(),
                                     request.condition().contents());
    }
    if (result.status != Status::OK) {
        // condition does not match, skip
    } else if (request.has_list_directory()) {
        std::vector<std::string> children;
        result = tree.listDirectory(request.list_directory().path(),
                                    children);
        for (auto it = children.begin(); it != children.end(); ++it)
            response.mutable_list_directory()->add_child(*it);
    } else if (request.has_read()) {
        std::string contents;
        result = tree.read(request.read().path(), contents);
        response.mutable_read()->set_contents(contents);
    } else {
        PANIC("Unexpected request: %s",
              Core::ProtoBuf::dumpString(request).c_str());
    }
    response.set_status(static_cast<PC::Status>(result.status));
    if (result.status != Status::OK)
        response.set_error(result.error);
}

    void
    readOnlyTreeRPC1(const Tree& tree,
                    const PC::ReadOnlyTree::Request& request,
                    PC::ReadOnlyTree::Response& response)
    {
        Result result;
        if (request.has_condition()) {
            result = tree.checkCondition(request.condition().path(),
                                         request.condition().contents());
        }
        if (result.status != Status::OK) {
            // condition does not match, skip
        } else if (request.has_read()) {
            std::string contents;
            result = tree.read(request.read().path(), contents);
            //TODO: change contents after running ANN
            response.mutable_read()->set_contents(contents);
        } else {
            PANIC("Unexpected request: %s",
                  Core::ProtoBuf::dumpString(request).c_str());
        }
        response.set_status(static_cast<PC::Status>(result.status));
        if (result.status != Status::OK)
            response.set_error(result.error);
    }

    std::string
    readOnlyTreeRPC2(const Tree& tree, const std::string& key)
    {
        std::string contents;
        Result result = tree.read(key, contents);
        //TODO: change contents after running ANN
        return contents;
    }

    bool getState(const Tree& tree, LogCabin::Protocol::Raft::State& st)
    {
        std::string contents;
        Result result = tree.read("/wt", contents);
        if(result.status != Status::OK)
            return false;

//        using PRS = LogCabin::Protocol::Raft::State;
        st.ParseFromString(contents);
        return true;
    }

//    std::string
//    readOnlyTreeRPC(const Tree& tree,
//                    const std::string& key)
//    {
//        Result result;
//        std::vector<std::string> children;
//        std::string contents;
//        result = tree.read(key, contents);
//        return contents;
//    }

void
readWriteTreeRPC(Tree& tree,
                 const PC::ReadWriteTree::Request& request,
                 PC::ReadWriteTree::Response& response)
{
    Result result;
    if (request.has_condition()) {
        result = tree.checkCondition(request.condition().path(),
                                     request.condition().contents());
    }
    if (result.status != Status::OK) {
        // condition does not match, skip
    } else if (request.has_make_directory()) {
        result = tree.makeDirectory(request.make_directory().path());
    } else if (request.has_remove_directory()) {
        result = tree.removeDirectory(request.remove_directory().path());
    } else if (request.has_write()) {
        result = tree.write(request.write().path(),
                            request.write().contents());
    } else if (request.has_remove_file()) {
        result = tree.removeFile(request.remove_file().path());
    } else {
        PANIC("Unexpected request: %s",
              Core::ProtoBuf::dumpString(request).c_str());
    }
    response.set_status(static_cast<PC::Status>(result.status));
    if (result.status != Status::OK)
        response.set_error(result.error);
}

} // namespace LogCabin::Tree::ProtoBuf
} // namespace LogCabin::Tree
} // namespace LogCabin
