#include "TrieDB.h"

namespace silkworm::db {
template <class KeyType, class DB>
using SecureTrieDB = SpecificTrieDB<HashedGenericTrieDB<DB>, KeyType>;

}
