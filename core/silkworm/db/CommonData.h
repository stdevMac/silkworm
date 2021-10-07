//
// Created by Marcos Maceo on 10/3/21.
//

#ifndef SILKWORM_COMMON_DATA_H
#define SILKWORM_COMMON_DATA_H

#include <algorithm>
#include <cstring>
#include <string>
#include <type_traits>
#include <unordered_set>
#include <vector>

#include "Common.h"

namespace silkworm::db {

// String conversion functions, mainly to/from hex/nibble/byte representations.

enum class WhenError {
    Throw = 1,
};

template <class Iterator>
std::string toHex(Iterator _it, Iterator _end, std::string const& _prefix) {
    typedef std::iterator_traits<Iterator> traits;
    static_assert(sizeof(typename traits::value_type) == 1, "toHex needs byte-sized element type");

    static char const* hexdigits = "0123456789abcdef";
    unsigned long off = _prefix.size();
    long dist = std::distance(_it, _end);
    std::string hex(static_cast<unsigned long>(long(dist * 2)) + off, '0');
    hex.replace(0, off, _prefix);
    for (; _it != _end; _it++) {
        hex[off++] = hexdigits[(*_it >> 4) & 0x0f];
        hex[off++] = hexdigits[*_it & 0x0f];
    }
    return hex;
}

/// Convert a series of bytes to the corresponding hex string.
/// @example toHex("A\x69") == "4169"
template <class T>
std::string toHex(T const& _data) {
    return toHex(_data.begin(), _data.end(), "");
}

/// Converts a (printable) ASCII hex string into the corresponding byte stream.
/// @example fromHex("41626261") == asBytes("Abba")
/// If _throw = ThrowType::DontThrow, it replaces bad hex characters with 0's, otherwise it will throw an exception.
bytes fromHex(std::string const& _s);

bytesConstRef getBytesConstRefFromHex(std::string const& _s);

bytesConstRef getBytesConstRef(std::vector<uint8_t> ret);

/// @returns true if @a _s is a hex string.
bool isHex(std::string const& _s) noexcept;

/// Converts byte array to a string containing the same (binary) data. Unless
/// the byte array happens to contain ASCII data, this won't be printable.
inline std::string asString(bytes const& _b) {
    return std::string(reinterpret_cast<char const*>(_b.data()), reinterpret_cast<char const*>(_b.data() + _b.size()));
}

/// Converts a string to a byte array containing the string's (byte) data.
inline bytes asBytes(std::string const& _b) {
    return bytes(reinterpret_cast<byte const*>(_b.data()), reinterpret_cast<byte const*>(_b.data() + _b.size()));
}

// Big-endian to/from host endian conversion functions.

/// Converts a templated integer value to the big-endian byte-stream represented on a templated collection.
/// The size of the collection object will be unchanged. If it is too small, it will not represent the
/// value properly, if too big then the additional elements will be zeroed out.
/// @a Out will typically be either std::string or bytes.
/// @a T will typically by unsigned, u160, u256 or bigint.
template <class T, class Out>
inline void toBigEndian(T _val, Out& o_out) {
    for (auto i = o_out.size(); i != 0; _val >>= 8, i--) {
        T v = _val & T(0xff);
        o_out[i - 1] = reinterpret_cast<typename Out::value_type(uint8_t)>(v);
    }
}

/// Converts a big-endian byte-stream represented on a templated collection to a templated integer value.
/// @a _In will typically be either std::string or bytes.
/// @a T will typically by unsigned, u160, u256 or bigint.
template <class T, class _In>
inline T fromBigEndian(_In const& _bytes) {
    T ret = T(0);
    for (auto i : _bytes) ret = T((ret << 8) | byte(i));
    return ret;
}

///// Convenience functions for toBigEndian
// inline std::string toBigEndianString(u256 _val) {
//     std::string ret(32, '\0');
//     toBigEndian(_val, ret);
//     return ret;
// }
// inline std::string toBigEndianString(u160 _val) {
//     std::string ret(20, '\0');
//     toBigEndian(_val, ret);
//     return ret;
// }
// inline bytes toBigEndian(u256 _val) {
//     bytes ret(32);
//     toBigEndian(_val, ret);
//     return ret;
// }
// inline bytes toBigEndian(u160 _val) {
//     bytes ret(20);
//     toBigEndian(_val, ret);
//     return ret;
// }


// Algorithms for string and string-like collections.

/// Escapes a string into the C-string representation.
/// @p _all if true will escape all characters, not just the unprintable ones.
std::string escaped(std::string const& _s, bool _all = true);

/// Determine bytes required to encode the given integer value. @returns 0 if @a _i is zero.
template <class T>
inline unsigned bytesRequired(T _i) {
    unsigned i = 0;
    for (; _i != 0; ++i, _i >>= 8) {
    }
    return i;
}


/// Concatenate the contents of a container onto a vector.
template <class T, class U>
inline std::vector<T>& operator+=(std::vector<T>& _a, U const& _b) {
    _a.insert(_a.end(), std::begin(_b), std::end(_b));
    return _a;
}

/// Insert the contents of a container into a set
template <class T, class U>
std::set<T>& operator+=(std::set<T>& _a, U const& _b) {
    _a.insert(std::begin(_b), std::end(_b));
    return _a;
}

/// Insert the contents of a container into an unordered_set
template <class T, class U>
std::unordered_set<T>& operator+=(std::unordered_set<T>& _a, U const& _b) {
    _a.insert(std::begin(_b), std::end(_b));
    return _a;
}

/// Insert the contents of a container into a set
template <class T, class U>
std::set<T> operator+(std::set<T> _a, U const& _b) {
    return _a += _b;
}

/// Insert the contents of a container into an unordered_set
template <class T, class U>
std::unordered_set<T> operator+(std::unordered_set<T> _a, U const& _b) {
    return _a += _b;
}

/// Concatenate the contents of a container onto a vector
template <class T, class U>
std::vector<T> operator+(std::vector<T> _a, U const& _b) {
    return _a += _b;
}


template <class T, class U>
std::vector<U> valuesOf(std::map<T, U> const& _m) {
    std::vector<U> ret;
    ret.reserve(_m.size());
    for (auto const& i : _m) ret.push_back(i.second);
    return ret;
}

template <class T, class U>
std::vector<U> valuesOf(std::unordered_map<T, U> const& _m) {
    std::vector<U> ret;
    ret.reserve(_m.size());
    for (auto const& i : _m) ret.push_back(i.second);
    return ret;
}

template <class T, class V>
bool contains(T const& _t, V const& _v) {
    return std::end(_t) != std::find(std::begin(_t), std::end(_t), _v);
}

template <class V>
bool contains(std::unordered_set<V> const& _set, V const& _v) {
    return _set.find(_v) != _set.end();
}

template <class K, class V>
bool contains(std::unordered_map<K, V> const& _map, K const& _k) {
    return _map.find(_k) != _map.end();
}

template <class V>
bool contains(std::set<V> const& _set, V const& _v) {
    return _set.find(_v) != _set.end();
}

template <class K, class V>
bool contains(std::map<K, V> const& _map, K const& _k) {
    return _map.find(_k) != _map.end();
}
}  // namespace silkworm::db

#endif  // SILKWORM_COMMON_DATA_H
