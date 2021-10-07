//
// Created by Marcos Maceo on 10/2/21.
//

#include "MemoryDB.h"

namespace silkworm {
namespace db {

    void MemoryDBWriteBatch::insert(Slice _key, Slice _value) { m_batch[_key.toString()] = _value.toString(); }

    void MemoryDBWriteBatch::kill(Slice _key) { m_batch.erase(_key.toString()); }

    std::string MemoryDB::lookup(Slice _key) const {
        auto const& it = m_db.find(_key.toString());
        if (it != m_db.end()) return it->second;
        return {};
    }

    bool MemoryDB::exists(Slice _key) const { return m_db.count(_key.toString()) != 0; }

    void MemoryDB::insert(Slice _key, Slice _value) { m_db[_key.toString()] = _value.toString(); }

    void MemoryDB::kill(Slice _key) { m_db.erase(_key.toString()); }

    void MemoryDB::commit(std::unique_ptr<WriteBatchFace> _batch) {
        if (!_batch) {
            //            SILKWORM_LOG(LogLevel::Info) << "Cannot commit null batch" << std::endl;
        }

        auto* batchPtr = dynamic_cast<MemoryDBWriteBatch*>(_batch.get());
        if (!batchPtr) {
            //            SILKWORM_LOG(LogLevel::Info) << "Invalid batch type passed to MemoryDB::commit" << std::endl;
        }
        auto const& batch = batchPtr->writeBatch();
        for (auto& e : batch) {
            m_db.insert({e.first, e.second});
        }
    }

}  // namespace db
}  // namespace silkworm