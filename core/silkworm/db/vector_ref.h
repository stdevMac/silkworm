//
// Created by Marcos Maceo on 10/3/21.
//

#ifndef SILKWORM_VECTOR_REF_H
#define SILKWORM_VECTOR_REF_H

#include <atomic>
#include <cassert>
#include <cstring>
#include <string>
#include <type_traits>
#include <vector>

namespace silkworm::db {

/**
 * A modifiable reference to an existing object or vector in memory.
 */
template <class T>
class vector_ref {
  public:
    using value_type = T;
    using element_type = T;
    using mutable_value_type =
        typename std::conditional<std::is_const<T>::value, typename std::remove_const<T>::type, T>::type;

    static_assert(std::is_pod<value_type>::value,
                  "vector_ref can only be used with PODs due to its low-level treatment of data.");

    vector_ref() : m_data(nullptr), m_count(0) {}
    /// Creates a new vector_ref to point to @a _count elements starting at @a _data.
    vector_ref(T* _data, size_t _count) : m_data(_data), m_count(_count) {}
    /// Creates a new vector_ref pointing to the data part of a string (given as pointer).
    explicit vector_ref(
        typename std::conditional<std::is_const<T>::value, std::string const*, std::string*>::type _data)
        : m_data(reinterpret_cast<T*>(_data->data())), m_count(_data->size() / sizeof(T)) {}
    /// Creates a new vector_ref pointing to the data part of a vector (given as pointer).
    explicit vector_ref(
        typename std::conditional<std::is_const<T>::value, std::vector<typename std::remove_const<T>::type> const*,
                                  std::vector<T>*>::type _data)
        : m_data(_data->data()), m_count(_data->size()) {}
    /// Creates a new vector_ref pointing to the data part of a string (given as reference).
    explicit vector_ref(
        typename std::conditional<std::is_const<T>::value, std::string const&, std::string&>::type _data)
        : m_data(reinterpret_cast<T*>(_data.data())), m_count(_data.size() / sizeof(T)) {}
    explicit operator bool() const { return m_data && m_count; }

    bool contentsEqual(std::vector<mutable_value_type> const& _c) const {
        if (!m_data || m_count == 0)
            return _c.empty();
        else
            return _c.size() == m_count && !memcmp(_c.data(), m_data, m_count * sizeof(T));
    }
    [[nodiscard]] std::string toString() const {
        return {reinterpret_cast<char const*>(m_data), reinterpret_cast<char const*>(m_data) + m_count * sizeof(T)};
    }

    template <class T2>
    explicit operator vector_ref<T2>() const {
        assert(m_count * sizeof(T) / sizeof(T2) * sizeof(T2) / sizeof(T) == m_count);
        return vector_ref<T2>(reinterpret_cast<T2*>(m_data), m_count * sizeof(T) / sizeof(T2));
    }
    explicit operator vector_ref<T const>() const { return vector_ref<T const>(m_data, m_count); }

    T* data() const { return m_data; }
    /// @returns the number of elements referenced (not necessarily number of bytes).
    [[nodiscard]] size_t count() const { return m_count; }
    /// @returns the number of elements referenced (not necessarily number of bytes).
    [[nodiscard]] size_t size() const { return m_count; }
    [[nodiscard]] bool empty() const { return !m_count; }
    /// @returns a new vector_ref pointing at the next chunk of @a size() elements.
    vector_ref<T> next() const {
        if (!m_data)
            return *this;
        else
            return vector_ref<T>(m_data + m_count, m_count);
    }
    /// @returns a new vector_ref which is a shifted and shortened view of the original data.
    /// If this goes out of bounds in any way, returns an empty vector_ref.
    /// If @a _count is ~size_t(0), extends the view to the end of the data.
    vector_ref<T> cropped(size_t _begin, size_t _count) const {
        if (m_data && _begin <= m_count && _count <= m_count && _begin + _count <= m_count)
            return vector_ref<T>(m_data + _begin, _count == ~size_t(0) ? m_count - _begin : _count);
        else
            return vector_ref<T>();
    }
    /// @returns a new vector_ref which is a shifted view of the original data (not going beyond it).
    vector_ref<T> cropped(size_t _begin) const {
        if (m_data && _begin <= m_count)
            return vector_ref<T>(m_data + _begin, m_count - _begin);
        else
            return vector_ref<T>();
    }
    template <class T1>
    bool overlapsWith(vector_ref<T1> _t) const {
        void const* f1 = data();
        void const* t1 = data() + size();
        void const* f2 = _t.data();
        void const* t2 = _t.data() + _t.size();
        return f1 < t2 && t1 > f2;
    }

    void copyTo(vector_ref<typename std::remove_const<T>::type> _t) const {
        if (overlapsWith(_t))
            memmove(_t.data(), m_data, std::min(_t.size(), m_count) * sizeof(T));
        else
            memcpy(_t.data(), m_data, std::min(_t.size(), m_count) * sizeof(T));
    }

    void retarget(T* _d, size_t _s) {
        m_data = _d;
        m_count = _s;
    }
    void retarget(std::vector<T> const& _t) {
        m_data = _t.data();
        m_count = _t.size();
    }

    /// Securely overwrite the memory.
    /// @note adapted from OpenSSL's implementation.
    void cleanse() {
        //        static std::atomic<unsigned char> s_cleanseCounter{0u};
        //        uint8_t* buffer{ = reinterpret_cast<uint8_t*>(begin())};
        //        auto* p = std::memcpy(buffer, begin(), size);
        //        size_t const len = reinterpret_cast<uint8_t*>(end()) - p;
        //        size_t loop = len;
        //        size_t count = s_cleanseCounter;
        //        while (loop--) {
        //            *(p++) = uint8_t(count);
        //            count += (17 + size_t(p[len - loop] & 0xf));
        //        }
        //        p = reinterpret_cast<uint8_t*>(memchr(reinterpret_cast<uint8_t*>(begin()), uint8_t(count), len));
        //        if (p) count += (63 + size_t(p));
        //        s_cleanseCounter = uint8_t(count);
        //        memset(reinterpret_cast<uint8_t*>(begin()), 0, len);
    }

    T* begin() { return m_data; }
    T* end() { return m_data + m_count; }
    T const* begin() const { return m_data; }
    T const* end() const { return m_data + m_count; }

    T& operator[](size_t _i) {
        assert(m_data);
        assert(_i < m_count);
        return m_data[_i];
    }
    T const& operator[](size_t _i) const {
        assert(m_data);
        assert(_i < m_count);
        return m_data[_i];
    }

    bool operator==(vector_ref<T> const& _cmp) const { return m_data == _cmp.m_data && m_count == _cmp.m_count; }
    bool operator!=(vector_ref<T> const& _cmp) const { return !operator==(_cmp); }

    void reset() {
        m_data = nullptr;
        m_count = 0;
    }

  private:
    T* m_data;
    size_t m_count;
};

template <class T>
vector_ref<T const> ref(T const& _t) {
    return vector_ref<T const>(&_t, 1);
}
template <class T>
vector_ref<T> ref(T& _t) {
    return vector_ref<T>(&_t, 1);
}
template <class T>
vector_ref<T const> ref(std::vector<T> const& _t) {
    return vector_ref<T const>(&_t);
}
template <class T>
vector_ref<T> ref(std::vector<T>& _t) {
    return vector_ref<T>(&_t);
}

}  // namespace silkworm::db

#endif  // SILKWORM_VECTOR_REF_H
