#include <catch2/catch.hpp>

#include "fixtures.h"

#include <faabric/util/queue.h>
#include <thread>

using namespace faabric::util;

namespace tests {

TEST_CASE("Test basic message queue enqueue and dequeue", "[message_queue]")
{
    SECTION("Basic enqueue and dequeue")
    {
        // Constants for the test
        const int functionReplica = 2;
        const int batchSize = 5;

        // Create the PartitionedStateMessageQueue object
        auto queue = PartitionedStateMessageQueue(functionReplica, batchSize);

        // Create some test messages
        auto msg1 = std::make_shared<faabric::Message>();
        msg1->set_id(1);
        auto msg2 = std::make_shared<faabric::Message>();
        msg2->set_id(2);
        auto msg3 = std::make_shared<faabric::Message>();
        msg3->set_id(3);
        auto msg4 = std::make_shared<faabric::Message>();
        msg4->set_id(4);
        auto msg5 = std::make_shared<faabric::Message>();
        msg5->set_id(5);
        auto msg6 = std::make_shared<faabric::Message>();
        msg6->set_id(6);
        auto msg7 = std::make_shared<faabric::Message>();
        msg7->set_id(7);
        auto msg8 = std::make_shared<faabric::Message>();
        msg8->set_id(8);
        auto msg9 = std::make_shared<faabric::Message>();
        msg9->set_id(9);
        auto msg10 = std::make_shared<faabric::Message>();
        msg10->set_id(10);
        auto msg11 = std::make_shared<faabric::Message>();
        msg11->set_id(11);

        // Enqueue the messages
        queue.addMessage(1, msg1); // 1
        queue.addMessage(2, msg2); // 2
        queue.addMessage(3, msg3); // 3
        queue.addMessage(1, msg4); // 4
        queue.addMessage(3, msg5);
        queue.addMessage(2, msg6);
        queue.addMessage(1, msg7);
        queue.addMessage(2, msg8);
        queue.addMessage(3, msg9);
        queue.addMessage(1, msg10);
        queue.addMessage(1, msg11);
        // Dequeue the messages
        std::vector<std::shared_ptr<faabric::Message>> messages =
          queue.getMessages();
        // Verify the size of the dequeued messages
        REQUIRE(messages.size() == batchSize);
        // Verify the order of the dequeued messages
        REQUIRE(messages[0]->id() == 1);
        REQUIRE(messages[1]->id() == 4);
        REQUIRE(messages[2]->id() == 7);
        REQUIRE(messages[3]->id() == 10);
        REQUIRE(messages[4]->id() == 2);

        // Dequeue the messages
        messages = queue.getMessages();
        // Verify the size of the dequeued messages
        REQUIRE(messages.size() == batchSize);
        // Verify the order of the dequeued messages
        REQUIRE(messages[0]->id() == 3);
        REQUIRE(messages[1]->id() == 5);
        REQUIRE(messages[2]->id() == 9);
        REQUIRE(messages[3]->id() == 6);
        REQUIRE(messages[4]->id() == 8);

        // Dequeue the messages
        messages = queue.getMessages();
        // Verify the size of the dequeued messages
        REQUIRE(messages.size() == 1);
        // Verify the order of the dequeued messages
        REQUIRE(messages[0]->id() == 11);
    }

    SECTION("Basic enqueue and dequeue with intermediate dequeue")
    {
        // Constants for the test
        const int functionReplica = 2;
        const int batchSize = 3;

        // Create the PartitionedStateMessageQueue object
        auto queue = PartitionedStateMessageQueue(functionReplica, batchSize);

        // Create some test messages
        auto msg1 = std::make_shared<faabric::Message>();
        msg1->set_id(1);
        auto msg2 = std::make_shared<faabric::Message>();
        msg2->set_id(2);
        auto msg3 = std::make_shared<faabric::Message>();
        msg3->set_id(3);
        auto msg4 = std::make_shared<faabric::Message>();
        msg4->set_id(4);
        auto msg5 = std::make_shared<faabric::Message>();
        msg5->set_id(5);
        auto msg6 = std::make_shared<faabric::Message>();
        msg6->set_id(6);
        auto msg7 = std::make_shared<faabric::Message>();
        msg7->set_id(7);
        auto msg8 = std::make_shared<faabric::Message>();
        msg8->set_id(8);
        auto msg9 = std::make_shared<faabric::Message>();
        msg9->set_id(9);
        auto msg10 = std::make_shared<faabric::Message>();
        msg10->set_id(10);
        auto msg11 = std::make_shared<faabric::Message>();
        msg11->set_id(11);

        // Enqueue the messages
        queue.addMessage(1, msg1); // 1
        std::vector<std::shared_ptr<faabric::Message>> messages =
          queue.getMessages();
        REQUIRE(messages.size() == 1);
        REQUIRE(messages[0]->id() == 1);

        // The following should be Batch 2 not Batch 1
        // If msg2 is Batch 1, then msg7 will be obtained in the next batch

        // Batch 2
        queue.addMessage(1, msg2);  
        queue.addMessage(2, msg3);
        queue.addMessage(3, msg4); 
        // Batch 3
        queue.addMessage(2, msg5);
        queue.addMessage(3, msg6);
        queue.addMessage(1, msg7);
        // Batch 4
        queue.addMessage(2, msg8);
        queue.addMessage(1, msg9);
        queue.addMessage(3, msg10);

        // Dequeue the messages
        messages = queue.getMessages();
        REQUIRE(messages.size() == batchSize);
        REQUIRE(messages[0]->id() == 2);
        REQUIRE(messages[1]->id() == 7);
        REQUIRE(messages[2]->id() == 3);

        messages = queue.getMessages();
        REQUIRE(messages.size() == batchSize);
        REQUIRE(messages[0]->id() == 4);
        REQUIRE(messages[1]->id() == 6);
        // Why 10 is here, since when trying to get the third message, all the messages in second batch are already dequeued.
        REQUIRE(messages[2]->id() == 10);

                // Dequeue the messages
        messages = queue.getMessages();
        // Verify the size of the dequeued messages
        REQUIRE(messages.size() == batchSize);
        REQUIRE(messages[0]->id() == 5);
        REQUIRE(messages[1]->id() == 8);
        REQUIRE(messages[2]->id() == 9);
    }
}
}