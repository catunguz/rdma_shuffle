#include "cli_parser.hpp"


namespace cli {

template <>
void Parser::parse_arg(std::string& arg, char& value) {
    rdma::ensure(arg.size() == 1);
    value = arg[0];
}

template <>
void Parser::parse_arg(std::string& arg, uint8_t& value) {
    value = stou8(arg);
}

template <>
void Parser::parse_arg(std::string& arg, bool& value) {
    std::transform(arg.begin(), arg.end(), arg.begin(), ::tolower);
    std::istringstream is(arg);
    is >> std::boolalpha >> value;
}

} // namespace cli
