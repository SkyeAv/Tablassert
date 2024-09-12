// Skye Goetz (ISB) 09/11/2024 

// g++ -std=c++11 -o hashingAndRegex hashingAndRegex.cpp -lssl -lcrypto

#include <openssl/sha.h>
#include <iostream>
#include <algorithm>
#include <cctype> 
#include <iomanip>
#include <sstream>

std::string hashing(const std::string& INPUT);
std::string regex(const std::string& INPUT);

int main(){
    std::string line;
    while (std::getline(std::cin, line)) {
        std::string processed_line = hashing(line);
        std::cout << processed_line << std::endl;
        std::cout.flush();
    }
    return 0;
}

std::string hashing(const std::string& INPUT){
    std::string result;
    result = regex(INPUT);
    std::transform(result.begin(), result.end(), result.begin(), [](unsigned char c) {
        return std::tolower(c);
    });
    unsigned char hash[SHA_DIGEST_LENGTH];
    SHA1(reinterpret_cast<const unsigned char*>(result.c_str()), result.size(), hash);
    std::ostringstream hex_stream;
    hex_stream << std::hex << std::setfill('0');
    for (int i = 0; i < SHA_DIGEST_LENGTH; ++i) {
        hex_stream << std::setw(2) << static_cast<int>(hash[i]);
    }
    return hex_stream.str();
}

std::string regex(const std::string& INPUT) {
    std::string result;
    for (char c : INPUT) {
        if (std::isalnum(c)) {
            result += c;
        }
    }
    return result;
}