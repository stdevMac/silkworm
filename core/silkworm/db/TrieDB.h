//
// Created by Marcos Maceo on 10/3/21.
//

#ifndef SILKWORM_TRIEDB_H
#define SILKWORM_TRIEDB_H

#include <memory>

#include "FixedHash.h"
#include "TrieCommon.h"
#include "sha3.h"
using namespace silkworm::db;
namespace silkworm::db {

enum class Verification { Skip, Normal };

template <class _DB>
class GenericTrieDB {
  public:
    using DB = _DB;

    explicit GenericTrieDB(DB* _db = nullptr) : m_db(_db) {}
    GenericTrieDB(DB* _db, h256 const& _root, Verification _v = Verification::Normal) { open(_db, _root, _v); }
    ~GenericTrieDB() {}

    void open(DB* _db) { m_db = _db; }
    void open(DB* _db, h256 const& _root, Verification _v = Verification::Normal) {
        m_db = _db;
        setRoot(_root, _v);
    }

    void init() {
        setRoot(forceInsertNode(&RLPNull));
        assert(node(m_root).size());
    }

    void setRoot(h256 const& _root, Verification _v = Verification::Normal) {
        m_root = _root;

        if (_v == Verification::Normal) {
            if (m_root == EmptyTrie && !m_db->exists(m_root)) init();
        }
    }

    /// True if the trie is uninitialised (i.e. that the DB doesn't contain the root node).
    bool isNull() const { return !node(m_root).size(); }
    /// True if the trie is initialised but empty (i.e. that the DB contains the root node which is empty).
    bool isEmpty() const { return m_root == EmptyTrie && node(m_root).size(); }

    h256 const& root() const {
        return m_root;
    }  // patch the root in the case of the empty trie. TODO: handle this properly.

    std::string at(bytes const& _key) const { return at(&_key); }
    std::string at(bytesConstRef _key) const;
    void insert(bytes const& _key, bytes const& _value) { insert(&_key, &_value); }
    void insert(bytesConstRef _key, bytes const& _value) { insert(_key, &_value); }
    void insert(bytes const& _key, bytesConstRef _value) { insert(&_key, _value); }
    void insert(bytesConstRef _key, bytesConstRef _value);
    void remove(bytes const& _key) { remove(&_key); }
    void remove(bytesConstRef _key);
    bool contains(bytes const& _key) const { return contains(&_key); }
    bool contains(bytesConstRef _key) const { return !at(_key).empty(); }

    class iterator {
      public:
        using value_type = std::pair<bytesConstRef, bytesConstRef>;

        iterator() {}
        explicit iterator(GenericTrieDB const* _db);
        iterator(GenericTrieDB const* _db, bytesConstRef _key);

        iterator& operator++() {
            next();
            return *this;
        }

        value_type operator*() const { return at(); }
        value_type operator->() const { return at(); }

        bool operator==(iterator const& _c) const { return _c.m_trail == m_trail; }
        bool operator!=(iterator const& _c) const { return _c.m_trail != m_trail; }

        value_type at() const;

      private:
        void next();
        void next(NibbleSlice _key);

        struct Node {
            std::string rlp;
            std::string key;  // as hexPrefixEncoding.
            byte child;       // 255 -> entering, 16 -> actually at the node, 17 -> exiting, 0-15 -> actual children.

            // 255 -> 16 -> 0 -> 1 -> ... -> 15 -> 17

            void setChild(unsigned _i) { child = _i; }
            void setFirstChild() { child = 16; }
            void incrementChild() { child = child == 16 ? 0 : child == 15 ? 17 : (child + 1); }

            bool operator==(Node const& _c) const { return rlp == _c.rlp && key == _c.key && child == _c.child; }
            bool operator!=(Node const& _c) const { return !operator==(_c); }
        };

      protected:
        std::vector<Node> m_trail;
        GenericTrieDB<DB> const* m_that;
    };

    iterator begin() const { return iterator(this); }
    iterator end() const { return iterator(); }

    iterator lower_bound(bytesConstRef _key) const { return iterator(this, _key); }

    /// Used for debugging, scans the whole trie.
    void descendKey(h256 const& _k, h256Hash& _keyMask, bool _wasExt, std::ostream* _out, int _indent = 0) const {
        _keyMask.erase(_k);
        if (_k == m_root && _k == EmptyTrie)  // root allowed to be empty
            return;
        std::string const s = node(_k);
        RLP r = RLP(s);
        descendList(r, _keyMask, _wasExt, _out, _indent);  // if not, it must be a list
    }

    /// Used for debugging, scans the whole trie.
    void descendEntry(RLP const& _r, h256Hash& _keyMask, bool _wasExt, std::ostream* _out, int _indent) const {
        if (_r.isData() && _r.size() == 32)
            descendKey(_r.toHash<h256>(), _keyMask, _wasExt, _out, _indent);
        else if (_r.isList())
            descendList(_r, _keyMask, _wasExt, _out, _indent);
    }

    /// Used for debugging, scans the whole trie.
    void descendList(RLP const& _r, h256Hash& _keyMask, bool _wasExt, std::ostream* _out, int _indent) const {
        if (_r.isList() && _r.itemCount() == 2 && (!_wasExt || _out)) {
            if (_out)
                (*_out) << std::string(static_cast<unsigned long>(_indent * 2), ' ') << (_wasExt ? "!2 " : "2  ")
                        << sha3(_r.data()) << ": " << _r << "\n";
            if (!isLeaf(_r))  // don't go down leaves
                descendEntry(_r[1], _keyMask, true, _out, _indent + 1);
        } else if (_r.isList() && _r.itemCount() == 17) {
            if (_out)
                (*_out) << std::string(static_cast<unsigned long>(_indent * 2), ' ') << "17 " << sha3(_r.data()) << ": "
                        << _r << "\n";
            for (unsigned i = 0; i < 16; ++i)
                if (!_r[i].isEmpty())  // 16 branches are allowed to be empty
                    descendEntry(_r[i], _keyMask, false, _out, _indent + 1);
        }
    }

    /// Used for debugging, scans the whole trie.
    h256Hash leftOvers(std::ostream* _out = nullptr) const {
        h256Hash k = m_db->keys();
        descendKey(m_root, k, false, _out);
        return k;
    }

    /// Used for debugging, scans the whole trie.
    void debugStructure(std::ostream& _out) const { leftOvers(&_out); }

    /// Used for debugging, scans the whole trie.
    /// @param _requireNoLeftOvers if true, requires that all keys are reachable.
    bool check(bool _requireNoLeftOvers) const {
        try {
            return leftOvers().empty() || !_requireNoLeftOvers;
        } catch (...) {
            return false;
        }
    }

    /// Get the underlying database.
    /// @warning This can be used to bypass the trie code. Don't use these unless you *really*
    /// know what you're doing.
    DB const* db() const { return m_db; }
    DB* db() { return m_db; }

  private:
    RLPStream& streamNode(RLPStream& _s, bytes const& _b);

    std::string atAux(RLP const& _here, NibbleSlice _key) const;

    void mergeAtAux(RLPStream& _out, RLP const& _replace, NibbleSlice _key, bytesConstRef _value);
    bytes mergeAt(RLP const& _replace, NibbleSlice _k, bytesConstRef _v, bool _inLine = false);
    bytes mergeAt(RLP const& _replace, h256 const& _replaceHash, NibbleSlice _k, bytesConstRef _v,
                  bool _inLine = false);

    bool deleteAtAux(RLPStream& _out, RLP const& _replace, NibbleSlice _key);
    bytes deleteAt(RLP const& _replace, NibbleSlice _k);

    // in: null (DEL)  -- OR --  [_k, V] (DEL)
    // out: [_k, _s]
    // -- OR --
    // in: [V0, ..., V15, S16] (DEL)  AND  _k == {}
    // out: [V0, ..., V15, _s]
    bytes place(RLP const& _orig, NibbleSlice _k, bytesConstRef _s);

    // in: [K, S] (DEL)
    // out: null
    // -- OR --
    // in: [V0, ..., V15, S] (DEL)
    // out: [V0, ..., V15, null]
    bytes remove(RLP const& _orig);

    // in: [K1 & K2, V] (DEL) : nibbles(K1) == _s, 0 < _s <= nibbles(K1 & K2)
    // out: [K1, H] ; [K2, V] => H (INS)  (being  [K1, [K2, V]]  if necessary)
    bytes cleve(RLP const& _orig, unsigned _s);

    // in: [K1, H] (DEL) ; H <= [K2, V] (DEL)  (being  [K1, [K2, V]] (DEL)  if necessary)
    // out: [K1 & K2, V]
    bytes graft(RLP const& _orig);

    // in: [V0, ... V15, S] (DEL)
    // out1: [k{i}, Vi]    where i < 16
    // out2: [k{}, S]      where i == 16
    bytes merge(RLP const& _orig, byte _i);

    // in: [k{}, S] (DEL)
    // out: [null ** 16, S]
    // -- OR --
    // in: [k{i}, N] (DEL)
    // out: [null ** i, N, null ** (16 - i)]
    // -- OR --
    // in: [k{i}K, V] (DEL)
    // out: [null ** i, H, null ** (16 - i)] ; [K, V] => H (INS)  (being [null ** i, [K, V], null ** (16 - i)]  if
    // necessary)
    bytes branch(RLP const& _orig);

    bool isTwoItemNode(RLP const& _n) const;
    std::string deref(RLP const& _n) const;

    std::string node(h256 const& _h) const { return m_db->lookup(_h); }

    // These are low-level node insertion functions that just go straight through into the DB.
    h256 forceInsertNode(bytesConstRef _v) {
        auto h = sha3(_v);
        forceInsertNode(h, _v);
        return h;
    }
    void forceInsertNode(h256 const& _h, bytesConstRef _v) { m_db->insert(_h, _v); }
    void forceKillNode(h256 const& _h) { m_db->kill(_h); }

    // This are semantically-aware node insertion functions that only kills when the node's
    // data is < 32 bytes. It can safely be used when pruning the trie but won't work correctly
    // for the special case of the root (which is always looked up via a hash). In that case,
    // use forceKillNode().
    void killNode(RLP const& _d) {
        if (_d.data().size() >= 32) forceKillNode(sha3(_d.data()));
    }
    void killNode(RLP const& _d, h256 const& _h) {
        if (_d.data().size() >= 32) forceKillNode(_h);
    }

    h256 m_root;
    DB* m_db = nullptr;
};

template <class DB>
std::ostream& operator<<(std::ostream& _out, GenericTrieDB<DB> const& _db) {
    for (auto const& i : _db)
        _out << escaped(i.first.toString(), false) << ": " << escaped(i.second.toString(), false) << std::endl;
    return _out;
}

/**
 * Different view on a GenericTrieDB that can use different key types.
 */
template <class Generic, class _KeyType>
class SpecificTrieDB : public Generic {
  public:
    using DB = typename Generic::DB;
    using KeyType = _KeyType;

    SpecificTrieDB(DB* _db = nullptr) : Generic(_db) {}
    SpecificTrieDB(DB* _db, h256 _root, Verification _v = Verification::Normal) : Generic(_db, _root, _v) {}

    std::string operator[](KeyType _k) const { return at(_k); }

    bool contains(KeyType _k) const {
        return Generic::contains(bytesConstRef(reinterpret_cast<byte const*>(&_k), sizeof(KeyType)));
    }
    std::string at(KeyType _k) const {
        return Generic::at(bytesConstRef(reinterpret_cast<byte const*>(&_k), sizeof(KeyType)));
    }
    void insert(KeyType _k, bytesConstRef _value) {
        Generic::insert(bytesConstRef(reinterpret_cast<byte const*>(&_k), sizeof(KeyType)), _value);
    }
    void insert(KeyType _k, bytes const& _value) { insert(_k, bytesConstRef(&_value)); }
    void remove(KeyType _k) { Generic::remove(bytesConstRef(reinterpret_cast<byte const*>(&_k), sizeof(KeyType))); }

    class iterator : public Generic::iterator {
      public:
        using Super = typename Generic::iterator;
        using value_type = std::pair<KeyType, bytesConstRef>;

        iterator() {}
        iterator(Generic const* _db) : Super(_db) {}
        iterator(Generic const* _db, bytesConstRef _k) : Super(_db, _k) {}

        value_type operator*() const { return at(); }
        value_type operator->() const { return at(); }

        value_type at() const;
    };

    iterator begin() const { return this; }
    iterator end() const { return iterator(); }
    iterator lower_bound(KeyType _k) const {
        return iterator(this, bytesConstRef(reinterpret_cast<byte const*>(&_k), sizeof(KeyType)));
    }
};
}  // namespace silkworm::db

template <class _DB>
class HashedGenericTrieDB : private SpecificTrieDB<GenericTrieDB<_DB>, h256> {
    using Super = SpecificTrieDB<GenericTrieDB<_DB>, h256>;

  public:
    using DB = _DB;

    HashedGenericTrieDB(DB* _db = nullptr) : Super(_db) {}
    HashedGenericTrieDB(DB* _db, h256 _root, Verification _v = Verification::Normal) : Super(_db, _root, _v) {}

    using Super::init;
    using Super::open;
    using Super::setRoot;

    /// True if the trie is uninitialised (i.e. that the DB doesn't contain the root node).
    using Super::isNull;
    /// True if the trie is initialised but empty (i.e. that the DB contains the root node which is empty).
    using Super::isEmpty;

    using Super::db;
    using Super::root;

    using Super::check;
    using Super::debugStructure;
    using Super::leftOvers;

    std::string at(bytesConstRef _key) const { return Super::at(sha3(_key)); }
    bool contains(bytesConstRef _key) const { return Super::contains(sha3(_key)); }
    void insert(bytesConstRef _key, bytesConstRef _value) { Super::insert(sha3(_key), _value); }
    void remove(bytesConstRef _key) { Super::remove(sha3(_key)); }

    // empty from the PoV of the iterator interface; still need a basic iterator impl though.
    class iterator {
      public:
        using value_type = std::pair<bytesConstRef, bytesConstRef>;

        iterator() {}
        iterator(HashedGenericTrieDB const*) {}
        iterator(HashedGenericTrieDB const*, bytesConstRef) {}

        iterator& operator++() { return *this; }
        value_type operator*() const { return value_type(); }
        value_type operator->() const { return value_type(); }

        bool operator==(iterator const&) const { return true; }
        bool operator!=(iterator const&) const { return false; }

        value_type at() const { return value_type(); }
    };
    iterator begin() const { return iterator(); }
    iterator end() const { return iterator(); }
    iterator lower_bound(bytesConstRef) const { return iterator(); }
};

#endif  // SILKWORM_TRIEDB_H
