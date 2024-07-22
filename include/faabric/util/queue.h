#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/util/clock.h>
#include <faabric/util/exception.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>

#include <condition_variable>
#include <deque>
#include <iostream>
#include <map>
#include <mutex>
#include <numeric>
#include <queue>
#include <readerwriterqueue/readerwritercircularbuffer.h>
#include <vector>

#define DEFAULT_QUEUE_TIMEOUT_MS 5000
#define DEFAULT_QUEUE_SIZE 1024

namespace faabric::util {
class QueueTimeoutException : public faabric::util::FaabricException
{
  public:
    explicit QueueTimeoutException(std::string message)
      : FaabricException(std::move(message))
    {}
};

template<typename T>
class Queue
{
  public:
    void enqueue(T value)
    {
        UniqueLock lock(mx);

        mq.emplace(std::move(value));

        enqueueNotifier.notify_one();
    }

    void dequeueIfPresent(T* res)
    {
        UniqueLock lock(mx);

        if (!mq.empty()) {
            T value = std::move(mq.front());
            mq.pop();
            emptyNotifier.notify_one();

            *res = value;
        }
    }

    T dequeue(long timeoutMs = DEFAULT_QUEUE_TIMEOUT_MS)
    {
        UniqueLock lock(mx);

        if (timeoutMs <= 0) {
            SPDLOG_ERROR("Invalid queue timeout: {} <= 0", timeoutMs);
            throw std::runtime_error("Invalid queue timeout");
        }

        while (mq.empty()) {
            std::cv_status returnVal = enqueueNotifier.wait_for(
              lock, std::chrono::milliseconds(timeoutMs));

            // Work out if this has returned due to timeout expiring
            if (returnVal == std::cv_status::timeout) {
                throw QueueTimeoutException("Timeout waiting for dequeue");
            }
        }

        T value = std::move(mq.front());
        mq.pop();
        emptyNotifier.notify_one();

        return value;
    }

    T* peek(long timeoutMs = 0)
    {
        UniqueLock lock(mx);
        while (mq.empty()) {
            if (timeoutMs > 0) {
                std::cv_status returnVal = enqueueNotifier.wait_for(
                  lock, std::chrono::milliseconds(timeoutMs));

                if (returnVal == std::cv_status::timeout) {
                    throw QueueTimeoutException("Timeout waiting for dequeue");
                }
            } else {
                enqueueNotifier.wait(lock);
            }
        }

        return &mq.front();
    }

    void waitToDrain(long timeoutMs)
    {
        UniqueLock lock(mx);

        while (!mq.empty()) {
            if (timeoutMs > 0) {
                std::cv_status returnVal = emptyNotifier.wait_for(
                  lock, std::chrono::milliseconds(timeoutMs));

                // Work out if this has returned due to timeout expiring
                if (returnVal == std::cv_status::timeout) {
                    throw QueueTimeoutException("Timeout waiting for empty");
                }
            } else {
                emptyNotifier.wait(lock);
            }
        }
    }

    void drain()
    {
        UniqueLock lock(mx);

        while (!mq.empty()) {
            mq.pop();
        }
    }

    long size()
    {
        UniqueLock lock(mx);
        return mq.size();
    }

    void reset()
    {
        UniqueLock lock(mx);

        std::queue<T> empty;
        std::swap(mq, empty);
    }

  private:
    std::queue<T> mq;
    std::condition_variable enqueueNotifier;
    std::condition_variable emptyNotifier;
    std::mutex mx;
};

// Wrapper around moodycamel's blocking fixed capacity single producer single
// consumer queue
// https://github.com/cameron314/readerwriterqueue
template<typename T>
class FixedCapacityQueue
{
  public:
    FixedCapacityQueue(int capacity)
      : mq(capacity){};

    FixedCapacityQueue()
      : mq(DEFAULT_QUEUE_SIZE){};

    void enqueue(T value, long timeoutMs = DEFAULT_QUEUE_TIMEOUT_MS)
    {
        if (timeoutMs <= 0) {
            SPDLOG_ERROR("Invalid queue timeout: {} <= 0", timeoutMs);
            throw std::runtime_error("Invalid queue timeout");
        }

        bool success =
          mq.wait_enqueue_timed(std::move(value), timeoutMs * 1000);
        if (!success) {
            throw QueueTimeoutException("Timeout waiting for enqueue");
        }
    }

    void dequeueIfPresent(T* res) { mq.try_dequeue(*res); }

    T dequeue(long timeoutMs = DEFAULT_QUEUE_TIMEOUT_MS)
    {
        if (timeoutMs <= 0) {
            SPDLOG_ERROR("Invalid queue timeout: {} <= 0", timeoutMs);
            throw std::runtime_error("Invalid queue timeout");
        }

        T value;
        bool success = mq.wait_dequeue_timed(value, timeoutMs * 1000);
        if (!success) {
            throw QueueTimeoutException("Timeout waiting for dequeue");
        }

        return value;
    }

    T* peek(long timeoutMs = DEFAULT_QUEUE_TIMEOUT_MS)
    {
        throw std::runtime_error("Peek not implemented");
    }

    void drain(long timeoutMs = DEFAULT_QUEUE_TIMEOUT_MS)
    {
        T value;
        bool success;
        while (size() > 0) {
            success = mq.wait_dequeue_timed(value, timeoutMs * 1000);
            if (!success) {
                throw QueueTimeoutException("Timeout waiting to drain");
            }
        }
    }

    long size() { return mq.size_approx(); }

    void reset()
    {
        moodycamel::BlockingReaderWriterCircularBuffer<T> empty(
          mq.max_capacity());
        std::swap(mq, empty);
    }

  private:
    moodycamel::BlockingReaderWriterCircularBuffer<T> mq;
};

class TokenPool
{
  public:
    explicit TokenPool(int nTokens);

    int getToken();

    void releaseToken(int token);

    void reset();

    int size();

    int taken();

    int free();

  private:
    int _size;
    Queue<int> queue;
};

template<typename T>
class FixedSizeQueue
{
  private:
    std::deque<T> deque;
    size_t capacity;

  public:
    explicit FixedSizeQueue(size_t capacity)
      : capacity(capacity)
    {}

    void add(const T& value)
    {
        if (deque.size() == capacity) {
            deque.pop_front();
        }
        deque.push_back(value);
    }

    T average() const
    {
        if (deque.empty())
            return T();

        // Use long to accumulate to prevent overflow
        long sum = std::accumulate(deque.begin(), deque.end(), 0LL);
        return static_cast<T>(sum / deque.size());
    }

    void display() const
    {
        for (auto& item : deque) {
            std::cout << item << " ";
        }
        std::cout << "\n";
    }
};

template<typename T>
class ThreadSafeQueue
{
  public:
    ThreadSafeQueue() = default;

    // Disable copy constructor and assignment operator
    ThreadSafeQueue(const ThreadSafeQueue&) = delete;
    ThreadSafeQueue& operator=(const ThreadSafeQueue&) = delete;

    // Enqueue an item
    void enqueue(T item)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_queue.push(std::move(item));
        m_cond_var.notify_one();
    }

    // Dequeue an item, blocks if the queue is empty
    T dequeue()
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_cond_var.wait(lock, [this]() { return !m_queue.empty(); });
        T item = std::move(m_queue.front());
        m_queue.pop();
        return item;
    }

    // Try to dequeue an item, returns false if the queue is empty
    bool try_dequeue(T& item)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_queue.empty()) {
            return false;
        }
        item = std::move(m_queue.front());
        m_queue.pop();
        return true;
    }

    // Check if the queue is empty
    bool empty() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_queue.empty();
    }

  private:
    std::queue<T> m_queue;
    mutable std::mutex m_mutex;
    std::condition_variable m_cond_var;
};

class PartitionedStateMessageQueue
{
  public:
    PartitionedStateMessageQueue(int functionReplica, int batchSize)
      : functionReplica(functionReplica)
      , batchSize(batchSize)
    {
        // Last time is the lastest time bewtween earliest insert time and the
        // lastest invoke time.
        lastTime = faabric::util::getGlobalClock().epochMillis();
    };

    std::shared_ptr<faabric::Message> front()
    {
        if (messagesCount == 0) {
            throw std::runtime_error("Queue is empty");
        }

        return messagesQueue.begin()->second.begin()->second.front();
    }

    void addMessage(size_t hash, std::shared_ptr<faabric::Message> msg)
    {
        // If the queue is empty, which means insert the first msg, reset the
        // invoke time.
        if (messagesCount == 0) {
            resetlastTime();
        }
        // Record the message inside the queue and index table.
        messagesQueue[enqueueBatchNum][hash].push(msg);
        hashToBatchNumTable[hash].push(enqueueBatchNum);

        messagesCount++;
        enqueueBatchSize++;
        if (enqueueBatchSize == batchSize) {
            enqueueBatchNum++;
            enqueueBatchSize = 0;
        }
    }

    std::vector<std::shared_ptr<faabric::Message>> getMessages()
    {
        std::vector<std::shared_ptr<faabric::Message>> returnMessages;
        if (messagesCount == 0) {
            return returnMessages;
        }
        bool firstMsg = true;
        size_t previousHash;

        while (returnMessages.size() < batchSize) {
            std::shared_ptr<faabric::Message> currentMessage = nullptr;
            int dequeueBatchNum = messagesQueue.begin()->first;

            size_t currentHash = 0;
            int currentEnqueueBatchNum = 0;

            // Judge if the next message shared the same hash with the previous
            bool sameHash = false;
            // If it's not the first message and the message with the same hash
            // is in the batchNum range. Get the message with the same hash.
            if (!firstMsg && !hashToBatchNumTable[previousHash].empty() &&
                hashToBatchNumTable[previousHash].front() - dequeueBatchNum <
                  functionReplica) {
                sameHash = true;
            }
            firstMsg = false;

            // Get the current message.
            if (!sameHash) {
                currentEnqueueBatchNum = messagesQueue.begin()->first;
                currentHash =
                  messagesQueue[currentEnqueueBatchNum].begin()->first;
                currentMessage =
                  messagesQueue[currentEnqueueBatchNum][currentHash].front();
            }
            // If other messages shared with the same hash with in the batchNum
            // range, get them all.
            else {
                currentEnqueueBatchNum =
                  hashToBatchNumTable[previousHash].front();
                currentHash = previousHash;
                currentMessage =
                  messagesQueue[currentEnqueueBatchNum][previousHash].front();
            }

            // Clear the Queue and Table.
            messagesQueue[currentEnqueueBatchNum][currentHash].pop();
            if (messagesQueue[currentEnqueueBatchNum][currentHash].empty()) {
                messagesQueue[currentEnqueueBatchNum].erase(currentHash);
            }
            if (messagesQueue[currentEnqueueBatchNum].empty()) {
                messagesQueue.erase(currentEnqueueBatchNum);
            }
            hashToBatchNumTable[currentHash].pop();

            messagesCount--;
            returnMessages.push_back(currentMessage);

            if (messagesCount == 0) {
                // If the message queue cannot reach the batchSize, update the
                // enqueueInfo. Happens when limiation time is reached, but
                // queue is not full.
                if (returnMessages.size() < batchSize) {
                    enqueueBatchNum++;
                    enqueueBatchSize = 0;
                }
                break;
            }

            previousHash = currentHash;
        }

        return returnMessages;
    }

    int getMessagesCount() { return messagesCount; }

    int getTimeInterval()
    {
        return faabric::util::getGlobalClock().epochMillis() - lastTime;
    }
    void resetlastTime()
    {
        lastTime = faabric::util::getGlobalClock().epochMillis();
    }

  private:
    // Const Variables
    const int functionReplica;
    const int batchSize;
    long lastTime;

    // MAP<BatchNum, MAP<Hash, Messages>>
    std::map<int,
             std::map<size_t, std::queue<std::shared_ptr<faabric::Message>>>>
      messagesQueue;
    // MAP<Hash, QUEUE<BatchNum>>
    std::map<size_t, std::queue<int>> hashToBatchNumTable;

    // The batchNum used and planned for enqueue Messages.
    int enqueueBatchNum = 0;
    int enqueueBatchSize = 0;

    // The number of messages in the queue.
    int messagesCount = 0;
};

}
