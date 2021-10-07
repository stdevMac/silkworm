//
// Created by Marcos Maceo on 10/2/21.
//

#include "StateCacheDB.h"

#include "Common.h"
#include "FixedHash.h"
using namespace std;
using namespace silkworm::db;

namespace silkworm::db {

std::unordered_map<h256, string> StateCacheDB::get() const {
    std::unordered_map<h256, std::string> ret;
    for (auto const& i : m_main)
        if (!m_enforceRefs || i.second.second > 0) ret.insert(make_pair(i.first, i.second.first));
    return ret;
}

StateCacheDB& StateCacheDB::operator=(StateCacheDB const& _c) {
    if (this == &_c) return *this;
    m_main = _c.m_main;
    return *this;
}

std::string StateCacheDB::lookup(h256 const& _h) const {
    //    auto t = m_main.size();
    auto it = m_main.find(_h.hex());
    if (it != m_main.end()) {
        return it->second.first;
    }
    return {};
}

bool StateCacheDB::exists(h256 const& _h) const {
    auto it = m_main.find(_h.hex());
    if (it != m_main.end()) return true;
    return false;
}

void StateCacheDB::insert(h256 const& _h, bytesConstRef _v) {
    auto it = m_main.find(_h.hex());
    if (it != m_main.end()) {
        it->second.first = _v.toString();
        it->second.second++;
    } else
        m_main[_h.hex()] = make_pair(_v.toString(), 1);
    auto t = m_main.size();
    (void)t;
}

bool StateCacheDB::kill(h256 const& _h) {
    if (m_main.count(_h.hex())) {
        if (m_main[_h.hex()].second > 0) {
            m_main[_h.hex()].second--;
            return true;
        }
    }
    return false;
}

void StateCacheDB::purge() {
    // purge m_main
    for (auto it = m_main.begin(); it != m_main.end();)
        if (it->second.second)
            ++it;
        else
            it = m_main.erase(it);
}

h256Hash StateCacheDB::keys() const {
    h256Hash ret;
    for (auto const& i : m_main)
        if (i.second.second) ret.insert(h256{i.first});
    return ret;
}

}  // namespace silkworm::db
