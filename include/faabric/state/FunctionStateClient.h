#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/state/FunctionStateRegistry.h>
#include <faabric/state/State.h>
#include <faabric/transport/MessageEndpoint.h>
#include <faabric/transport/MessageEndpointClient.h>

namespace faabric::state {
class FunctionStateClient : public faabric::transport::MessageEndpointClient
{
  public:
    explicit FunctionStateClient(const std::string& userIn,
                                 const std::string& funcIn,
                                 const int parallelismIdIn,
                                 const std::string& hostIn);

    const std::string user;
    const std::string func;
    const int parallelismId;

    size_t stateSize();

    void pushChunks(const std::vector<StateChunk>& chunks, uint32_t stateSize);

    void pullChunks(const std::vector<StateChunk>& chunks,
                    uint8_t* bufferStart);

  private:
    // void sendStateRequest(faabric::state::FunctionStateCalls header,
    //                       const uint8_t* data,
    //                       int length);

    void logRequest(const std::string& op);
};
}
