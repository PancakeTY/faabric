#include <faabric/state/FunctionStateClient.h>
#include <faabric/transport/common.h>
#include <faabric/transport/macros.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>

namespace faabric::state {

FunctionStateClient::FunctionStateClient(const std::string& userIn,
                                         const std::string& funcIn,
                                         const int parallelismIdIn,
                                         const std::string& hostIn)
  : faabric::transport::MessageEndpointClient(hostIn,
                                              STATE_ASYNC_PORT,
                                              STATE_SYNC_PORT)
  , user(userIn)
  , func(funcIn)
  , parallelismId(parallelismIdIn)
{}

void FunctionStateClient::logRequest(const std::string& op)
{
    SPDLOG_TRACE(
      "Requesting {} on {}/{}-{} at {}", op, user, func, parallelismId, host);
}

size_t FunctionStateClient::stateSize()
{
    logRequest("functionstate-size");

    faabric::FunctionStateRequest request;
    request.set_user(user);
    request.set_func(func);
    request.set_parallelismid(parallelismId);
    faabric::FunctionStateSizeResponse response;
    syncSend(faabric::state::StateCalls::FunctionSize, &request, &response);

    return response.statesize();
}

// TODO - Lock the data during pull and push

void FunctionStateClient::pullChunks(const std::vector<StateChunk>& chunks,
                                     uint8_t* bufferStart)
{
    logRequest("functionstate-pull-chunks");

    for (const auto& chunk : chunks) {
        // Prepare request
        faabric::FunctionStateChunkRequest request;
        request.set_user(user);
        request.set_func(func);
        request.set_parallelismid(parallelismId);
        request.set_offset(chunk.offset);
        request.set_chunksize(chunk.length);

        // Send request
        faabric::FunctionStatePart response;
        syncSend(faabric::state::StateCalls::FunctionPull, &request, &response);

        // Copy response data
        std::copy(response.data().begin(),
                  response.data().end(),
                  bufferStart + response.offset());
        // TODO - refine the size increase in pullChunks
    }
}

void FunctionStateClient::pushChunks(const std::vector<StateChunk>& chunks,
                                     uint32_t stateSize)
{
    logRequest("functionstate-push-chunks");

    for (const auto& chunk : chunks) {
        faabric::FunctionStatePart stateChunk;
        stateChunk.set_user(user);
        stateChunk.set_func(func);
        stateChunk.set_parallelismid(parallelismId);
        stateChunk.set_offset(chunk.offset);
        stateChunk.set_data(chunk.data, chunk.length);
        stateChunk.set_statesize(stateSize);
        faabric::EmptyResponse resp;
        syncSend(faabric::state::StateCalls::FunctionPush, &stateChunk, &resp);
    }
}

}
