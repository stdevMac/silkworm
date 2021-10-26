//
// Created by Marcos Maceo on 10/3/21.
//

#include "FixedHash.h"
namespace silkworm::db {

h128 fromUUID(std::string const& _uuid) {
    return h128{_uuid};
}

std::string toUUID(h128 const& _uuid) {
    std::string ret = toHex(_uuid.ref());
    for (auto i : {20, 16, 12, 8}) ret.insert(ret.begin() + i, '-');
    return ret;
}

}  // namespace silkworm::db