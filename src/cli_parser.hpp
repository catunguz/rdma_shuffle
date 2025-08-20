#pragma once

#include "rdmapp/my_asserts.hpp"

#include <algorithm>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <vector>


namespace cli {

class Parser {
    std::map<std::string, std::string> pairs;

public:
    enum Options {
        required,
        optional,
    };

    Parser(int argc, char** argv) {
        std::stringstream ss;
        ss << argv[0] << "\n";
        for (int i = 1; i < argc; i += 2) {
            pairs[argv[i]] = argv[i + 1];
            ss << "    " << argv[i] << "=" << argv[i + 1] << "\n";
        }
        std::cout << ss.rdbuf();
    }

    template <typename T>
    void parse(std::string param, T& value, Options options = Options::required) {
        if (pairs.find(param) == pairs.end()) {
            if (options == Options::optional) {
                return;
            }
            std::stringstream ss;
            ss << "Parameter " << param << " is missing.";
            throw std::invalid_argument(ss.str());
        }

        parse_arg(pairs.at(param), value);
    }

    template <typename T>
    void parse(std::string param, std::vector<T>& values, Options options = Options::required) {
        // if (!pairs.contains(param)) { // C++20
        if (pairs.find(param) == pairs.end()) {
            if (options == Options::optional) {
                return;
            }
            std::stringstream ss;
            ss << "Parameter " << param << " is missing.";
            throw std::invalid_argument(ss.str());
        }


        values.clear();
        std::istringstream ss{pairs.at(param)};
        std::string item;
        while (std::getline(ss, item, ',')) {
            T value;
            parse_arg(item, value);
            values.push_back(value);
        }
    }


private:
    template <typename T>
    void parse_arg(std::string& arg, T& value) {
        std::istringstream ss{arg};
        ss >> value;
    }


    inline uint8_t stou8(const std::string& s) {
        int i = std::stoi(s);
        if (i <= static_cast<int>(UINT8_MAX) && i >= 0) {
            return static_cast<uint8_t>(i);
        }
        throw std::out_of_range("stou8() failed");
    }
};


template <>
void Parser::parse_arg(std::string& arg, char& value);

template <>
void Parser::parse_arg(std::string& arg, uint8_t& value);

template <>
void Parser::parse_arg(std::string& arg, bool& value);

} // namespace cli