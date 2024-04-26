#include <faabric/util/serialization.h>

#include <functional>
#include <cstring>  // For memcpy
#include <iomanip>  // For std::setw and std::setfill

// THERE MUST BE SAME AS CPP LIMFAASM UTIL SERIALIZATION
namespace faabric::util {

// Helper function for combining hash values
inline void hashCombine(std::size_t& seed, std::size_t value)
{
    seed ^= value + 0x9e3779b9 + (seed << 6) + (seed >> 2);
}

// Function definition
std::size_t hashVector(const std::vector<uint8_t>& vec)
{
    std::size_t hashValue = 0;
    for (uint8_t byte : vec) {
        std::size_t elementHash = std::hash<uint8_t>{}(byte);
        hashCombine(hashValue, elementHash);
    }
    return hashValue;
}

// Transforms a uint8_t vector of bytes into a uint32_t
uint32_t uint8VToUint32(const std::vector<uint8_t>& bytes)
{
    uint32_t value = 0;
    for (int i = 0; i < 4; i++) {
        value = (value << 8) | bytes[i];
    }
    return value;
}
// Transforms unint32_t of bytes into a uint8_t vector
std::vector<uint8_t> uint32ToUint8V(uint32_t value)
{
    std::vector<uint8_t> bytes(4);
    for (int i = 0; i < 4; i++) {
        bytes[3 - i] = (value >> (i * 8)) & 0xFF;
    }
    return bytes;
}

// Transforms a map <string, int> into a uint8_t vector of bytes.
std::vector<uint8_t> mapIntToUint8V(const std::map<std::string, int>& map)
{
    std::vector<uint8_t> bytes;
    for (const auto& pair : map) {
        std::string key = pair.first;
        int value = pair.second;
        bytes.push_back(static_cast<uint8_t>(key.size())); // Key length
        bytes.insert(bytes.end(), key.begin(), key.end()); // Key
        // Append 'int' value as four bytes
        for (int i = 3; i >= 0; --i) {
            bytes.push_back((value >> (i * 8)) & 0xFF);
        }
    }
    return bytes;
}

// Transforms a uint8_t vector of bytes into a map <string, int>.
std::map<std::string, int> uint8VToMapInt(const std::vector<uint8_t>& bytes)
{
    std::map<std::string, int> map;
    size_t i = 0;
    while (i < bytes.size()) {
        uint8_t keyLen = bytes[i++];
        std::string key(bytes.begin() + i, bytes.begin() + i + keyLen);
        i += keyLen;
        // Read 'int' value from four bytes
        int value = 0;
        for (int j = 0; j < 4; ++j) {
            value |= (static_cast<int>(bytes[i + j]) << ((3 - j) * 8));
        }
        i += 4;
        map[key] = value;
    }
    return map;
}

std::vector<uint8_t> serializeMapBinary(
  const std::map<std::string, std::string>& map)
{
    std::vector<uint8_t> buffer;

    for (const auto& [key, value] : map) {
        // Serialize key size
        uint32_t keySize = key.size();
        uint8_t* keySizeBytes = reinterpret_cast<uint8_t*>(&keySize);
        buffer.insert(
          buffer.end(), keySizeBytes, keySizeBytes + sizeof(keySize));

        // Serialize key
        buffer.insert(buffer.end(), key.begin(), key.end());

        // Serialize value size
        uint32_t valueSize = value.size();
        uint8_t* valueSizeBytes = reinterpret_cast<uint8_t*>(&valueSize);
        buffer.insert(
          buffer.end(), valueSizeBytes, valueSizeBytes + sizeof(valueSize));

        // Serialize value
        buffer.insert(buffer.end(), value.begin(), value.end());
    }

    return buffer;
}

std::map<std::string, std::string> deserializeMapBinary(
  const std::vector<uint8_t>& buffer)
{
    std::map<std::string, std::string> map;
    size_t index = 0;

    while (index < buffer.size()) {
        // Deserialize key size
        uint32_t keySize;
        std::copy_n(&buffer[index],
                    sizeof(keySize),
                    reinterpret_cast<uint8_t*>(&keySize));
        index += sizeof(keySize);

        // Deserialize key
        std::string key(&buffer[index], &buffer[index] + keySize);
        index += keySize;

        // Deserialize value size
        uint32_t valueSize;
        std::copy_n(&buffer[index],
                    sizeof(valueSize),
                    reinterpret_cast<uint8_t*>(&valueSize));
        index += sizeof(valueSize);

        // Deserialize value
        std::string value(&buffer[index], &buffer[index] + valueSize);
        index += valueSize;

        map[std::move(key)] = std::move(value);
    }

    return map;
}

void serializeUInt32(std::vector<uint8_t>& vec, uint32_t value)
{
    vec.insert(vec.end(),
               { static_cast<uint8_t>(value >> 24),
                 static_cast<uint8_t>(value >> 16),
                 static_cast<uint8_t>(value >> 8),
                 static_cast<uint8_t>(value) });
}

std::vector<uint8_t> serializeFuncState(
  const std::map<std::string, std::vector<uint8_t>>& map)
{
    std::vector<uint8_t> serialized;
    // Pre-calculate required capacity to minimize reallocations
    size_t totalSize = 0;
    for (const auto& [key, valueVec] : map) {
        totalSize += 4 + key.size() + 4 + valueVec.size();
    }
    serialized.reserve(totalSize);

    for (const auto& [key, valueVec] : map) {
        serializeUInt32(serialized, key.size());
        serialized.insert(serialized.end(), key.begin(), key.end());
        serializeUInt32(serialized, valueVec.size());
        serialized.insert(serialized.end(), valueVec.begin(), valueVec.end());
    }

    return serialized;
}

uint32_t deserializeUInt32(const std::vector<uint8_t>& vec, size_t& index)
{
    uint32_t value = (static_cast<uint32_t>(vec[index]) << 24) |
                     (static_cast<uint32_t>(vec[index + 1]) << 16) |
                     (static_cast<uint32_t>(vec[index + 2]) << 8) |
                     (static_cast<uint32_t>(vec[index + 3]));
    index += 4;
    return value;
}

std::map<std::string, std::vector<uint8_t>> deserializeFuncState(
  const std::vector<uint8_t>& serialized)
{
    std::map<std::string, std::vector<uint8_t>> map;
    size_t index = 0;
    while (index < serialized.size()) {
        auto keyLength = deserializeUInt32(serialized, index);
        std::string key(serialized.begin() + index,
                        serialized.begin() + index + keyLength);
        index += keyLength;

        auto valueLength = deserializeUInt32(serialized, index);
        std::vector<uint8_t> valueVec(serialized.begin() + index,
                                      serialized.begin() + index + valueLength);
        index += valueLength;

        map.emplace(std::move(key), std::move(valueVec));
    }
    return map;
}

std::vector<uint8_t> serializeParState(
  const std::map<std::string, std::vector<uint8_t>>& map)
{
    return serializeFuncState(map);
}
std::map<std::string, std::vector<uint8_t>> deserializeParState(
  const std::vector<uint8_t>& bytes)
{
    return deserializeFuncState(bytes);
}

// Helper function to append an unsigned 32-bit integer to the buffer
void appendUint32(std::vector<uint8_t>& buffer, uint32_t value)
{
    uint8_t temp[4];
    std::memcpy(temp, &value, 4);
    buffer.insert(buffer.end(), temp, temp + 4);
}

// Serialize a string into a vector of uint8_t
void serializeString(std::vector<uint8_t>& buffer, const std::string& str)
{
    appendUint32(buffer, static_cast<uint32_t>(str.size())); // Length of string
    buffer.insert(buffer.end(), str.begin(), str.end()); // String characters
}

// Deserialize a string from a vector of uint8_t
std::string deserializeString(const std::vector<uint8_t>& buffer, size_t& index)
{
    uint32_t length = *reinterpret_cast<const uint32_t*>(&buffer[index]);
    index += 4;
    std::string str(buffer.begin() + index, buffer.begin() + index + length);
    index += length;
    return str;
}

// Serialize a map of strings
void serializeMap(std::vector<uint8_t>& buffer,
                  const std::map<std::string, std::string>& map)
{
    appendUint32(buffer, static_cast<uint32_t>(map.size())); // Number of pairs
    for (const auto& pair : map) {
        serializeString(buffer, pair.first);  // Serialize key
        serializeString(buffer, pair.second); // Serialize value
    }
}

// Deserialize a map of strings
std::map<std::string, std::string> deserializeMap(
  const std::vector<uint8_t>& buffer,
  size_t& index)
{
    // Handle empty buffer or index out of range
    if (buffer.size() < index + 4) {
        return {};  // Return an empty map if there's not enough data
    }

    uint32_t numPairs = *reinterpret_cast<const uint32_t*>(&buffer[index]);
    index += 4;
    std::map<std::string, std::string> map;

    for (uint32_t i = 0; i < numPairs; ++i) {
        if (buffer.size() < index + 1) {
            throw std::runtime_error("Buffer too small for expected number of pairs");
        }
        std::string key = deserializeString(buffer, index);
        std::string value = deserializeString(buffer, index);
        map[std::move(key)] = std::move(value);
    }
    return map;
}

// Serialize a nested map
void serializeNestedMap(
  std::vector<uint8_t>& buffer,
  const std::map<std::string, std::map<std::string, std::string>>& nestedMap)
{
    appendUint32(buffer, static_cast<uint32_t>(nestedMap.size())); // Number of outer pairs
    for (const auto& pair : nestedMap) {
        serializeString(buffer, pair.first); // Serialize outer key
        serializeMap(buffer, pair.second);   // Serialize inner map
    }
}

// Deserialize a nested map
std::map<std::string, std::map<std::string, std::string>> deserializeNestedMap(
  const std::vector<uint8_t>& buffer,
  size_t& index)
{
    if (buffer.size() < index + 4) {
        return {};  // Return an empty nested map if there's not enough data
    }

    uint32_t numPairs = *reinterpret_cast<const uint32_t*>(&buffer[index]);
    index += 4;
    std::map<std::string, std::map<std::string, std::string>> nestedMap;

    for (uint32_t i = 0; i < numPairs; ++i) {
        if (buffer.size() < index + 1) {
            throw std::runtime_error("Buffer too small for expected number of outer pairs");
        }
        std::string key = deserializeString(buffer, index);
        std::map<std::string, std::string> valueMap = deserializeMap(buffer, index);
        nestedMap[std::move(key)] = std::move(valueMap);
    }
    return nestedMap;
}

}