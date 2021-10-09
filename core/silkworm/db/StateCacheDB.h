//
// Created by Marcos Maceo on 10/2/21.
//

#ifndef SILKWORM_STATECACHEDB_H
#define SILKWORM_STATECACHEDB_H

#include "Common.h"
#include "FixedHash.h"
#include "RLP.h"
using namespace std;
using Slice = silkworm::db::vector_ref<char const>;

namespace silkworm::db {

class StateCacheDB {
  public:
    StateCacheDB() = default;
    StateCacheDB(StateCacheDB const& _c) { operator=(_c); }

    StateCacheDB& operator=(StateCacheDB const& _c);

    virtual ~StateCacheDB() = default;

    void clear() { m_main.clear(); }  // WARNING !!!! didn't originally clear m_refCount!!!
    std::unordered_map<silkworm::db::h256, std::string> get() const;

    virtual std::string lookup(silkworm::db::h256 const& _h) const;
    virtual bool exists(silkworm::db::h256 const& _h) const;
    virtual void insert(silkworm::db::h256 const& _h, silkworm::db::bytesConstRef _v);
    virtual bool kill(silkworm::db::h256 const& _h);
    void purge();

    h256Hash keys() const;

  protected:
    std::unordered_map<std::string, std::pair<std::string, unsigned>>  m_main;
    mutable bool m_enforceRefs = false;
};
}  // namespace silkworm::db

#endif  // SILKWORM_STATECACHEDB_H
