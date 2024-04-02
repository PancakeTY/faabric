#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

// THERE MUST BE SAME AS CPP LIMFAASM UTIL SERIALIZATION
// TODO - Ensure the consistency with client.cpp/serialization.h

namespace faabric::util {
// Serialiazion and Deserialization from uint8_t vetors to uint32_t
uint32_t uint8VToUint32(const std::vector<uint8_t>& bytes);
std::vector<uint8_t> uint32ToUint8V(uint32_t value);

// Serialiazion and Deserialization from uint8_t vetors to Map<string,string>
std::vector<uint8_t> mapToUint8V(const std::map<std::string, std::string>& map);
std::map<std::string, std::string> uint8VToMap(
  const std::vector<uint8_t>& bytes);

// Serialiazion and Deserialization from uint8_t vetors to Map<string,int>
std::vector<uint8_t> mapIntToUint8V(const std::map<std::string, int>& map);
std::map<std::string, int> uint8VToMapInt(const std::vector<uint8_t>& bytes);

// Serialiazion and Deserialization from function State to uint8_t vetors
std::vector<uint8_t> serializeFuncState(
  const std::map<std::string, std::vector<uint8_t>>& map);
std::map<std::string, std::vector<uint8_t>> deserializeFuncState(
  const std::vector<uint8_t>& bytes);
}
