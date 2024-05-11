#pragma once

#include <faabric/util/serialization.h>

#include <cstring> // For std::memcpy in converting strings to vectors
#include <functional>
#include <iostream>
#include <map>
#include <vector>

namespace faabric::util {

class ConsistentHashRing
{
  private:
    std::map<std::size_t, int> ring; // Hash values mapped to node IDs
    int numNodes;
    int numVirtualNodes; // Number of virtual nodes per physical node

  public:
    ConsistentHashRing(int nodes = 0, int virtualNodes = 10)
      : numNodes(nodes)
      , numVirtualNodes(virtualNodes)
    {
        initializeNodes();
    }

    void initializeNodes()
    {
        for (int i = 0; i < numNodes; ++i) {
            addNode(i); // Use addNode to maintain logic consistency
        }
    }

    void addNode(int nodeID)
    {
        if (nodeID != numNodes) {
            std::cerr << "Error: Node ID must be the next sequential integer. "
                         "Expected Node ID: "
                      << numNodes << std::endl;
            return;
        }
        for (int v = 0; v < numVirtualNodes; ++v) {
            std::string virtualNodeKey =
              "Node" + std::to_string(nodeID) + "Virtual" + std::to_string(v);
            std::vector<uint8_t> keyBytes(virtualNodeKey.begin(),
                                          virtualNodeKey.end());
            std::size_t hash = faabric::util::hashVector(keyBytes);
            ring[hash] = nodeID;
            std::cout << "Added Virtual Node " << virtualNodeKey
                      << " with hash " << hash << std::endl;
        }
        numNodes++;
    }

    void addNodes(int count)
    {
        for (int i = 0; i < count; ++i) {
            addNode(numNodes);
        }
    }

    void removeNode(int nodeID)
    {
        auto it = ring.begin();
        while (it != ring.end()) {
            if (it->second == nodeID) {
                auto toErase = it++;
                ring.erase(toErase);
            } else {
                ++it;
            }
        }
        std::cout << "Removed Node " << nodeID << std::endl;
        numNodes--;
    }

    int getNode(const std::vector<uint8_t>& keyBytes)
    {
        std::size_t hash = hashVector(keyBytes);
        auto it = ring.lower_bound(hash);
        if (it == ring.end()) {
            it = ring.begin();
        }
        return it->second;
    }
};

}