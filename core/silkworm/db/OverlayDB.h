//
// Created by Marcos Maceo on 10/2/21.
//

#ifndef SILKWORM_OVERLAYDB_H
#define SILKWORM_OVERLAYDB_H

#include "FixedHash.h"
#include "StateCacheDB.h"
#include "db.h"

namespace silkworm::db {

class OverlayDB : public StateCacheDB {
  public:
    explicit OverlayDB(std::unique_ptr<db::DatabaseFace> _db = nullptr)
        : m_db(_db.release(), [](db::DatabaseFace* db) {
              delete db;
          }) {}

    ~OverlayDB();

    // Copyable
    OverlayDB(OverlayDB const&) = default;
    OverlayDB& operator=(OverlayDB const&) = default;
    // Movable
    OverlayDB(OverlayDB&&) = default;
    OverlayDB& operator=(OverlayDB&&) = default;

    void commit();
    void rollback();

    std::string lookup(h256 const& _h) const;
    bool exists(h256 const& _h) const;
    bool kill(h256 const& _h);

  private:
    using StateCacheDB::clear;

    std::shared_ptr<db::DatabaseFace> m_db;
};
}  // namespace silkworm::db
#endif  // SILKWORM_OVERLAYDB_H
