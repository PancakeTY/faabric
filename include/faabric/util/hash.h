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
    ConsistentHashRing(int nodes = 0, int virtualNodes = 50)
      : numNodes(0)
      , numVirtualNodes(virtualNodes)
    {
        addNodes(nodes);
    }

    void addNode(int nodeID)
    {
        for (int v = 0; v < numVirtualNodes; ++v) {
            std::string virtualNodeKey =
              "Node" + std::to_string(nodeID) + "Virtual" + std::to_string(v);
            std::vector<uint8_t> keyBytes(virtualNodeKey.begin(),
                                          virtualNodeKey.end());
            std::size_t hash = faabric::util::hashVector(keyBytes);
            ring[hash] = nodeID;
            // std::cout << "Added Virtual Node " << virtualNodeKey
            //           << " with hash " << hash << std::endl;
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
        bool found = false;
        while (it != ring.end()) {
            if (it->second == nodeID) {
                auto toErase = it++;
                ring.erase(toErase);
                found = true;
            } else {
                ++it;
            }
        }
        if (found) {
            std::cout << "Removed Node " << nodeID << std::endl;
            numNodes--;
        } else {
            std::cerr << "Error: Node ID " << nodeID << " not found."
                      << std::endl;
        }
    }

    int getNode(const std::vector<uint8_t>& keyBytes)
    {
        std::size_t hash = faabric::util::hashVector(keyBytes);
        // Print hash
        // std::cout << "Hash: " << hash << std::endl;
        auto it = ring.lower_bound(hash);
        if (it == ring.end()) {
            it = ring.begin();
        }
        return it->second;
    }

    std::pair<size_t, int> getHashAndNode(const std::vector<uint8_t>& keyBytes)
    {
        std::size_t hash = faabric::util::hashVector(keyBytes);
        // Print hash
        // std::cout << "Hash: " << hash << std::endl;
        auto it = ring.lower_bound(hash);
        if (it == ring.end()) {
            it = ring.begin();
        }
        return std::make_pair(hash, it->second);
    }
};

}