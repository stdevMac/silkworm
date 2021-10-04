//
// Created by Marcos Maceo on 10/2/21.
//

#ifndef SILKWORM_MEMORYDB_H
#define SILKWORM_MEMORYDB_H

#include <iostream>
#include <unordered_map>


#include "db.h"
#include "vector_ref.h"

namespace silkworm {
namespace db {
    using Slice = vector_ref<char const>;
    class MemoryDBWriteBatch : public WriteBatchFace {
      public:
        void insert(Slice _key, Slice _value) override;
        void kill(Slice _key) override;

        std::unordered_map<std::string, std::string>& writeBatch() { return m_batch; }
        size_t size() { return m_batch.size(); }

      private:
        std::unordered_map<std::string, std::string> m_batch;
    };

    class MemoryDB : public DatabaseFace {
      public:
        std::string lookup(Slice _key) const override;
        bool exists(Slice _key) const override;
        void insert(Slice _key, Slice _value) override;
        void kill(Slice _key) override;

        std::unique_ptr<WriteBatchFace> createWriteBatch() const override;
        void commit(std::unique_ptr<WriteBatchFace> _batch) override;

        // A database must implement the `forEach` method that allows the caller
        // to pass in a function `f`, which will be called with the key and value
        // of each record in the database. If `f` returns false, the `forEach`
        // method must return immediately.
        void forEach(std::function<bool(Slice, Slice)> _f) const override;

        size_t size() const { return m_db.size(); }

      private:
        std::unordered_map<std::string, std::string> m_db;
    };
}  // namespace db
}  // namespace silkworm

#endif  // SILKWORM_MEMORYDB_H
