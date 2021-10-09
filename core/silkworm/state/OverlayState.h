//
// Created by Marcos Maceo on 10/2/21.
//

#ifndef SILKWORM_OVERLAYSTATE_H
#define SILKWORM_OVERLAYSTATE_H

#include <iostream>  // std::cout
#include <sstream>   // std::stringstream, std::stringbuf
#include <string>    // std::string
#include <unordered_map>
#include <vector>

#include <silkworm/db/SecureTrieDB.h>
#include <silkworm/db/StateCacheDB.h>
#include <silkworm/state/intra_block_state.hpp>
#include <silkworm/state/state.hpp>
#include <silkworm/trie/hash_builder.hpp>
#include <silkworm/types/account.hpp>
#include <silkworm/types/block.hpp>
#include <silkworm/types/receipt.hpp>

namespace silkworm {

using namespace silkworm::db;

using Address = h160;

class OverlayState : public State {
  private:
    // address -> initial value
    using AccountChanges = std::unordered_map<evmc::address, std::optional<Account>>;

    // address -> incarnation -> location -> initial value
    using StorageChanges =
        std::unordered_map<evmc::address,
                           std::unordered_map<uint64_t, std::unordered_map<evmc::bytes32, evmc::bytes32>>>;

  public:
    explicit OverlayState(StateCacheDB& odb) : m_db{odb}, m_state(&odb) { m_state.init(); }

  public:
    OverlayState(void);
    std::optional<Account> read_account(const evmc::address& address) const noexcept;

    ByteView read_code(const evmc::bytes32& code_hash) const noexcept;

    evmc::bytes32 read_storage(const evmc::address& address, uint64_t incarnation,
                               const evmc::bytes32& location) const noexcept;

    uint64_t previous_incarnation(const evmc::address& address) const noexcept;

    std::optional<BlockHeader> read_header(uint64_t block_number, const evmc::bytes32& block_hash) const noexcept;

    std::optional<BlockBody> read_body(uint64_t block_number, const evmc::bytes32& block_hash) const noexcept;

    std::optional<intx::uint256> total_difficulty(uint64_t block_number,
                                                  const evmc::bytes32& block_hash) const noexcept;

    evmc::bytes32 state_root_hash() const;

    uint64_t current_canonical_block() const;

    std::optional<evmc::bytes32> canonical_hash(uint64_t block_number) const;

    void insert_witness();

    void insert_block(const Block& block, const evmc::bytes32& hash);

    void canonize_block(uint64_t block_number, const evmc::bytes32& block_hash);

    void decanonize_block(uint64_t block_number);

    void insert_receipts(uint64_t block_number, const std::vector<Receipt>& receipts);

    void begin_block(uint64_t block_number);

    void update_account(const evmc::address& address, std::optional<Account> initial, std::optional<Account> current);

    void update_account_code(const evmc::address& address, uint64_t incarnation, const evmc::bytes32& code_hash,
                             ByteView code);

    void update_storage(const evmc::address& address, uint64_t incarnation, const evmc::bytes32& location,
                        const evmc::bytes32& initial, const evmc::bytes32& current);

    void unwind_state_changes(uint64_t block_number);

    size_t number_of_accounts() const { return 0; }

    void set_root(h256 root) { m_state.setRoot(root); }

    void debug_tree() {
        std::ostream _out{0};
        m_state.debugStructure(_out);
        std::stringstream ss{};
        ss << _out.rdbuf();
        std::string myString = ss.str();
    }

    size_t storage_size(const evmc::address& address, uint64_t incarnation) const;

    const std::unordered_map<uint64_t, AccountChanges>& account_changes() const { return account_changes_; }
    const std::unordered_map<evmc::address, Account>& accounts() const { return accounts_; }

  private:
    /// Our overlay for the state tree.
    StateCacheDB m_db;
    /// Our state tree, as an OverlayDB Database.
    SecureTrieDB<Address, StateCacheDB> m_state;
    /// Our address cache. This stores the states of each address that has (or at least might have)
    /// been changed.
    mutable std::unordered_map<evmc::address, Account> m_cache;
    /// Tracks addresses that are known to not exist.
    mutable std::set<evmc::address> m_nonExistingAccountsCache;

    evmc::bytes32 account_storage_root(const evmc::address& address, uint64_t incarnation) const;

    std::unordered_map<evmc::address, Account> accounts_;

    // hash -> code
    std::unordered_map<evmc::bytes32, Bytes> code_;

    std::unordered_map<evmc::address, uint64_t> prev_incarnations_;

    // address -> incarnation -> location -> value
    std::unordered_map<evmc::address, std::unordered_map<uint64_t, std::unordered_map<evmc::bytes32, evmc::bytes32>>>
        storage_;

    // block number -> hash -> header
    std::vector<std::unordered_map<evmc::bytes32, BlockHeader>> headers_;

    // block number -> hash -> body
    std::vector<std::unordered_map<evmc::bytes32, BlockBody>> bodies_;

    // block number -> hash -> total difficulty
    std::vector<std::unordered_map<evmc::bytes32, intx::uint256>> difficulty_;

    std::vector<evmc::bytes32> canonical_hashes_;

    std::unordered_map<uint64_t, AccountChanges> account_changes_;  // per block
    std::unordered_map<uint64_t, StorageChanges> storage_changes_;  // per block

    uint64_t block_number_{0};
};

}  // namespace silkworm

#endif  // SILKWORM_OVERLAYSTATE_H
