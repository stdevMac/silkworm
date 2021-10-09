////
//// Created by Marcos Maceo on 10/2/21.
////
//
//#include "OverlayDB.h"
//
//#include "FixedHash.h"
//
//namespace silkworm::db {
//namespace {
//    inline db::Slice toSlice(h256 const& _h) { return db::Slice(reinterpret_cast<char const*>(_h.data()), _h.size); }
//
////    inline db::Slice toSlice(std::string const& _str) { return db::Slice(_str.data(), _str.size()); }
////
////    inline db::Slice toSlice(bytes const& _b) { return db::Slice(reinterpret_cast<char const*>(&_b[0]), _b.size()); }
//
//}  // namespace
//
//OverlayDB::~OverlayDB() = default;
//
//void OverlayDB::commit() {
////    if (m_db) {
////        auto writeBatch = m_db->createWriteBatch();
////        {
////            for (auto const& i : m_main) {
////                //                if (i.second.second) writeBatch->insert(i.first, i.second.first);
////            }
////        }
////
////        for (unsigned i = 0; i < 10; ++i) {
////            //            try {
////            //                m_db->commit(std::move(writeBatch));
////            //                break;
////            //            } catch (const std::exception& ex) {
////            //                if (i == 9) {
////            //                    SILKWORM_LOG(LogLevel::Info) << "Fail writing to state database. Bombing out." <<
////            //                    std::endl; exit(-1);
////            //                }
////            //                SILKWORM_LOG(LogLevel::Info) << "Error writing to state database" << std::endl;
////            //            }
////        }
////        m_main.clear();
////    }
//}
//
//void OverlayDB::rollback() { m_main.clear(); }
//
//std::string OverlayDB::lookup(h256 const& _h) const {
//    auto ret = StateCacheDB::lookup(_h);
//    if (!ret.empty() || !m_db) return ret;
//    return m_db->lookup(toSlice(_h));
//}
//
//bool OverlayDB::exists(h256 const& _h) const {
//    if (StateCacheDB::exists(_h)) return true;
//    return m_db && m_db->exists(toSlice(_h));
//}
//
//bool OverlayDB::kill(h256 const& _h) {
//    if (!StateCacheDB::kill(_h)) {
//        if (m_db) {
//            if (!m_db->exists(toSlice(_h))) {
//                // No point node ref decreasing for EmptyTrie since we never bother incrementing it
//                // in the first place for empty storage tries.
//            }
//        }
//    }
//    return true;
//}
//}  // namespace silkworm::db