//
// Created by Marcos Maceo on 10/2/21.
//

#include "OverlayState.h"

#include <algorithm>

#include <absl/container/btree_set.h>

#include <silkworm/common/endian.hpp>
#include <silkworm/types/log_cbor.hpp>
#include <silkworm/types/receipt_cbor.hpp>

#include "access_layer.hpp"
#include "buffer.hpp"
#include "tables.hpp"

namespace silkworm::db {

void OverlayState::bump_batch_size(size_t key_len, size_t value_len) {
    (void)(key_len + value_len);
    throw std::runtime_error(std::string(__FUNCTION__).append(" not yet implemented"));
}

void OverlayState::begin_block(uint64_t block_number) {
    (void)block_number;
    throw std::runtime_error(std::string(__FUNCTION__).append(" not yet implemented"));
}

void OverlayState::update_account(const evmc::address& address, std::optional<Account> initial,
                                  std::optional<Account> current) {
    (void)address;
    (void)initial;
    (void)current;
    throw std::runtime_error(std::string(__FUNCTION__).append(" not yet implemented"));
}

void OverlayState::update_account_code(const evmc::address& address, uint64_t incarnation,
                                       const evmc::bytes32& code_hash, ByteView code) {
    (void)address;
    (void)incarnation;
    (void)code_hash;
    (void)code;
    throw std::runtime_error(std::string(__FUNCTION__).append(" not yet implemented"));
}

void OverlayState::update_storage(const evmc::address& address, uint64_t incarnation, const evmc::bytes32& location,
                                  const evmc::bytes32& initial, const evmc::bytes32& current) {
    (void)address;
    (void)incarnation;
    (void)location;
    (void)initial;
    (void)current;
    throw std::runtime_error(std::string(__FUNCTION__).append(" not yet implemented"));
}

void OverlayState::write_to_state_table() {
    throw std::runtime_error(std::string(__FUNCTION__).append(" not yet implemented"));
}

void OverlayState::write_to_db() { throw std::runtime_error(std::string(__FUNCTION__).append(" not yet implemented")); }

// Erigon WriteReceipts in core/rawdb/accessors_chain.go
void OverlayState::insert_receipts(uint64_t block_number, const std::vector<Receipt>& receipts) {
    (void)block_number;
    (void)receipts;
    throw std::runtime_error(std::string(__FUNCTION__).append(" not yet implemented"));
}

evmc::bytes32 OverlayState::state_root_hash() const {
    throw std::runtime_error(std::string(__FUNCTION__).append(" not yet implemented"));
}

uint64_t OverlayState::current_canonical_block() const {
    throw std::runtime_error(std::string(__FUNCTION__).append(" not yet implemented"));
}

std::optional<evmc::bytes32> OverlayState::canonical_hash(uint64_t) const {
    throw std::runtime_error(std::string(__FUNCTION__).append(" not yet implemented"));
}

void OverlayState::canonize_block(uint64_t, const evmc::bytes32&) {
    throw std::runtime_error(std::string(__FUNCTION__).append(" not yet implemented"));
}

void OverlayState::decanonize_block(uint64_t) {
    throw std::runtime_error(std::string(__FUNCTION__).append(" not yet implemented"));
}

void OverlayState::insert_block(const Block& block, const evmc::bytes32& hash) {
    (void)block;
    (void)hash;
    throw std::runtime_error(std::string(__FUNCTION__).append(" not yet implemented"));
}

std::optional<intx::uint256> OverlayState::total_difficulty(uint64_t block_number,
                                                            const evmc::bytes32& block_hash) const noexcept {
    (void)block_hash;
    return intx::uint256(block_number);
}

std::optional<BlockHeader> OverlayState::read_header(uint64_t block_number,
                                                     const evmc::bytes32& block_hash) const noexcept {
    (void)block_hash;
    (void)block_number;
    return BlockHeader{};
}

std::optional<BlockBody> OverlayState::read_body(uint64_t block_number,
                                                 const evmc::bytes32& block_hash) const noexcept {
    (void)block_number;
    (void)block_hash;
    return BlockBody{};
}

std::optional<Account> OverlayState::read_account(const evmc::address& address) const noexcept {
    auto it = m_cache.find(address);
    if (it != m_cache.end()) return it->second;

    if (m_nonExistingAccountsCache.count(address)) return Account{};

    // Populate basic info.
    string stateBack = m_state.at(address);
    if (stateBack.empty()) {
        m_nonExistingAccountsCache.insert(address);
        return Account{};
    }
    auto [account, result]{decode_account_from_storage(ByteView{*from_hex(stateBack)})};
    rlp::err_handler(result);

    return account;
}

ByteView OverlayState::read_code(const evmc::bytes32& code_hash) const noexcept {
    if (auto it{hash_to_code_.find(code_hash)}; it != hash_to_code_.end()) {
        return it->second;
    }
    std::optional<ByteView> code{db::read_code(txn_, code_hash)};
    if (code.has_value()) {
        return *code;
    } else {
        return {};
    }
}

evmc::bytes32 OverlayState::read_storage(const evmc::address& address, uint64_t incarnation,
                                         const evmc::bytes32& location) const noexcept {
    if (auto it1{storage_.find(address)}; it1 != storage_.end()) {
        if (auto it2{it1->second.find(incarnation)}; it2 != it1->second.end()) {
            if (auto it3{it2->second.find(location)}; it3 != it2->second.end()) {
                return it3->second;
            }
        }
    }

    return db::read_storage(txn_, address, incarnation, location, historical_block_);
}

uint64_t OverlayState::previous_incarnation(const evmc::address& address) const noexcept {
    if (auto it{incarnations_.find(address)}; it != incarnations_.end()) {
        return it->second;
    }
    std::optional<uint64_t> incarnation{db::read_previous_incarnation(txn_, address, historical_block_)};
    return incarnation ? *incarnation : 0;
}

void OverlayState::unwind_state_changes(uint64_t) {
    throw std::runtime_error(std::string(__FUNCTION__).append(" not yet implemented"));
}

}  // namespace silkworm::db
