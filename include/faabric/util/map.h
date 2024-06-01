#pragma once

#include <iostream>
#include <map>
#include <shared_mutex>
#include <condition_variable>
#include <utility>  // For std::move
#include <mutex>    // For std::unique_lock and std::lock

namespace faabric::util {
template<typename K, typename V>
class ThreadSafeMap
{
  private:
    std::map<K, V> map;
    mutable std::shared_mutex mtx;

  public:
    // Default constructor
    ThreadSafeMap() = default;

    // Delete copy constructor and copy assignment operator
    ThreadSafeMap(const ThreadSafeMap&) = delete;
    ThreadSafeMap& operator=(const ThreadSafeMap&) = delete;

    // Provide move constructor and move assignment operator
    ThreadSafeMap(ThreadSafeMap&& other) noexcept
    {
        std::unique_lock lock(other.mtx);
        map = std::move(other.map);
    }

    ThreadSafeMap& operator=(ThreadSafeMap&& other) noexcept
    {
        if (this != &other) {
            std::unique_lock lock1(mtx, std::defer_lock);
            std::unique_lock lock2(other.mtx, std::defer_lock);
            std::lock(lock1, lock2);
            map = std::move(other.map);
        }
        return *this;
    }

    void insert(const K& key, const V& value)
    {
        std::unique_lock lock(mtx);
        map[key] = value;
    }

    bool get(const K& key, V& value) const
    {
        std::shared_lock lock(mtx);
        auto it = map.find(key);
        if (it != map.end()) {
            value = it->second;
            return true;
        }
        return false;
    }

    void erase(const K& key)
    {
        std::unique_lock lock(mtx);
        map.erase(key);
    }

    bool contains(const K& key) const
    {
        std::shared_lock lock(mtx);
        return map.find(key) != map.end();
    }

    size_t size() const
    {
        std::shared_lock lock(mtx);
        return map.size();
    }

    // Add find method
    typename std::map<K, V>::iterator find(const K& key)
    {
        std::unique_lock lock(mtx);
        return map.find(key);
    }

    typename std::map<K, V>::const_iterator find(const K& key) const
    {
        std::shared_lock lock(mtx);
        return map.find(key);
    }

    // Add end method
    typename std::map<K, V>::iterator end()
    {
        std::unique_lock lock(mtx);
        return map.end();
    }

    typename std::map<K, V>::const_iterator end() const
    {
        std::shared_lock lock(mtx);
        return map.end();
    }

    // Add begin method
    typename std::map<K, V>::iterator begin()
    {
        std::unique_lock lock(mtx);
        return map.begin();
    }

    typename std::map<K, V>::const_iterator begin() const
    {
        std::shared_lock lock(mtx);
        return map.begin();
    }

    // Add operator[] for safe access
    V& operator[](const K& key)
    {
        std::unique_lock lock(mtx);
        return map[key];
    }

    const V& operator[](const K& key) const
    {
        std::shared_lock lock(mtx);
        return map.at(key); // Use at() for const access to ensure exception on missing key
    }

    // Add at method for bounds-checked access
    V& at(const K& key)
    {
        std::unique_lock lock(mtx);
        return map.at(key);
    }

    const V& at(const K& key) const
    {
        std::shared_lock lock(mtx);
        return map.at(key);
    }

    // Add clear method to clear the map
    void clear()
    {
        std::unique_lock lock(mtx);
        map.clear();
    }
};
}
