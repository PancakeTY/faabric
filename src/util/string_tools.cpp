#include <faabric/util/string_tools.h>

#include <algorithm>
#include <string>

namespace faabric::util {

bool isAllWhitespace(const std::string& input)
{
    return std::all_of(input.begin(), input.end(), isspace);
}

bool startsWith(const std::string& input, const std::string& subStr)
{
    if (subStr.empty()) {
        return false;
    }

    return input.rfind(subStr, 0) == 0;
}

bool endsWith(std::string const& value, std::string const& ending)
{
    if (ending.empty()) {
        return false;
    } else if (ending.size() > value.size()) {
        return false;
    }
    return std::equal(ending.rbegin(), ending.rend(), value.rbegin());
}

bool contains(const std::string& input, const std::string& subStr)
{
    if (input.find(subStr) != std::string::npos) {
        return true;
    } else {
        return false;
    }
}

std::string removeSubstr(const std::string& input, const std::string& toErase)
{
    std::string output = input;

    size_t pos = output.find(toErase);

    if (pos != std::string::npos) {
        output.erase(pos, toErase.length());
    }

    return output;
}

bool stringIsInt(const std::string& input)
{
    return !input.empty() &&
           input.find_first_not_of("0123456789") == std::string::npos;
}

std::tuple<std::string, std::string, std::string> splitUserFuncPar(const std::string& input) {
    // Find the positions of the underscores
    size_t firstUnderscorePos = input.find('_');
    size_t secondUnderscorePos = input.find('_', firstUnderscorePos + 1);
    
    // Check if underscores were found
    if (firstUnderscorePos == std::string::npos || secondUnderscorePos == std::string::npos) {
        throw std::invalid_argument("Input string format is incorrect.");
    }
    
    // Extract the substrings based on the positions of the underscores
    std::string part1 = input.substr(0, firstUnderscorePos);
    std::string part2 = input.substr(firstUnderscorePos + 1, secondUnderscorePos - firstUnderscorePos - 1);
    std::string part3 = input.substr(secondUnderscorePos + 1);
    
    return std::make_tuple(part1, part2, part3);
}
}
