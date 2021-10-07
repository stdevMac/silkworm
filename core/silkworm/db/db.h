//
// Created by Marcos Maceo on 10/2/21.
//
// Aleth: Ethereum C++ client, tools and libraries.
// Copyright 2014-2019 Aleth Authors.
// Licensed under the GNU General Public License, Version 3.

#pragma once

#include <memory>
#include <string>

#include "vector_ref.h"
namespace silkworm {
namespace db {
    using Slice = vector_ref<char const>;
    // WriteBatchFace implements database write batch for a specific concrete
    // database implementation.
    class WriteBatchFace {
      public:
        virtual ~WriteBatchFace() = default;

        virtual void insert(Slice _key, Slice _value) = 0;
        virtual void kill(Slice _key) = 0;

      protected:
        WriteBatchFace() = default;
        // Noncopyable
        WriteBatchFace(WriteBatchFace const&) = delete;
        WriteBatchFace& operator=(WriteBatchFace const&) = delete;
        // Nonmovable
        WriteBatchFace(WriteBatchFace&&) = delete;
        WriteBatchFace& operator=(WriteBatchFace&&) = delete;
    };

    class DatabaseFace {
      public:
        virtual ~DatabaseFace() = default;
        virtual std::string lookup(Slice _key) const = 0;
        virtual bool exists(Slice _key) const = 0;
        virtual void insert(Slice _key, Slice _value) = 0;
        virtual void kill(Slice _key) = 0;

        virtual void commit(std::unique_ptr<WriteBatchFace> _batch) = 0;

        // A database must implement the `forEach` method that allows the caller
        // to pass in a function `f`, which will be called with the key and value
        // of each record in the database. If `f` returns false, the `forEach`
        // method must return immediately.
    };

    enum class DatabaseStatus { NotFound = 1, Corruption = 2, Unknown = 6 };

}  // namespace db
}  // namespace silkworm
