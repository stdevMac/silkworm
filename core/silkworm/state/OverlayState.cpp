//
// Created by Marcos Maceo on 10/2/21.
//

#include "OverlayState.h"

#include <algorithm>

#include <absl/container/btree_set.h>

#include <silkworm/common/endian.hpp>
//#include <silkworm/types/log_cbor.hpp>
//#include <silkworm/types/receipt_cbor.hpp>
//
//#include "silkworm/db/access_layer.hpp"
//#include "silkworm/db/buffer.hpp"
//#include "silkworm/db/tables.hpp"

namespace silkworm {

void OverlayState::begin_block(uint64_t block_number) { (void)block_number; }

void OverlayState::update_account(const evmc::address& address, std::optional<Account> initial,
                                  std::optional<Account> current) {
    (void)address;
    (void)initial;
    (void)current;
}

void OverlayState::update_account_code(const evmc::address& address, uint64_t incarnation,
                                       const evmc::bytes32& code_hash, ByteView code) {
    (void)address;
    (void)incarnation;
    (void)code_hash;
    (void)code;
}

void OverlayState::update_storage(const evmc::address& address, uint64_t incarnation, const evmc::bytes32& location,
                                  const evmc::bytes32& initial, const evmc::bytes32& current) {
    (void)address;
    (void)incarnation;
    (void)location;
    (void)initial;
    (void)current;
}

// Erigon WriteReceipts in core/rawdb/accessors_chain.go
void OverlayState::insert_receipts(uint64_t block_number, const std::vector<Receipt>& receipts) {
    (void)block_number;
    (void)receipts;
}

evmc::bytes32 OverlayState::state_root_hash() const { return {}; }

uint64_t OverlayState::current_canonical_block() const { return {}; }

std::optional<evmc::bytes32> OverlayState::canonical_hash(uint64_t) const { return {}; }

void OverlayState::canonize_block(uint64_t, const evmc::bytes32&) {}

void OverlayState::decanonize_block(uint64_t) {}

void OverlayState::insert_block(const Block& block, const evmc::bytes32& hash) {
    (void)block;
    (void)hash;
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

    Address address1{to_hex(address)};

    // Populate basic info.
    string stateBack = m_state.at(address1);
    if (stateBack.empty()) {
        m_nonExistingAccountsCache.insert(address);
        return Account{};
    }
    auto [account, result]{decode_account_from_storage(ByteView{*from_hex(stateBack)})};
    (void)result;
    return account;
}

ByteView OverlayState::read_code(const evmc::bytes32& code_hash) const noexcept {
    auto it{code_.find(code_hash)};
    if (it == code_.end()) {
        return {};
    }
    return it->second;
}

evmc::bytes32 OverlayState::read_storage(const evmc::address& address, uint64_t incarnation,
                                         const evmc::bytes32& location) const noexcept {
    auto it1{storage_.find(address)};
    if (it1 == storage_.end()) {
        return {};
    }
    auto it2{it1->second.find(incarnation)};
    if (it2 == it1->second.end()) {
        return {};
    }
    auto it3{it2->second.find(location)};
    if (it3 == it2->second.end()) {
        return {};
    }
    return it3->second;
}

uint64_t OverlayState::previous_incarnation(const evmc::address& address) const noexcept {
    auto it{prev_incarnations_.find(address)};
    if (it == prev_incarnations_.end()) {
        return 0;
    }
    return it->second;
}

void OverlayState::unwind_state_changes(uint64_t) {}

}  // namespace silkworm
