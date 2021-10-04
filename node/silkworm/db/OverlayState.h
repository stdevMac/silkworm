//
// Created by Marcos Maceo on 10/2/21.
//

#ifndef SILKWORM_OVERLAYSTATE_H
#define SILKWORM_OVERLAYSTATE_H
#include <cassert>
#include <optional>
#include <vector>

#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

#include <silkworm/db/OverlayDB.h>
#include <silkworm/db/util.hpp>
#include <silkworm/state/state.hpp>
#include <silkworm/trie/hash_builder.hpp>
#include <silkworm/types/account.hpp>
#include <silkworm/types/block.hpp>
#include <silkworm/types/receipt.hpp>
#include <silkworm/db/SecureTrieDB.h>

namespace silkworm::db {

using Address = h160;

class OverlayState : public State {
  public:
    explicit OverlayState(OverlayDB odb, mdbx::txn& txn) : m_db{odb}, txn_{txn} {}

    std::optional<Account> read_account(const evmc::address& address) const noexcept override;

    ByteView read_code(const evmc::bytes32& code_hash) const noexcept override;

    evmc::bytes32 read_storage(const evmc::address& address, uint64_t incarnation,
                               const evmc::bytes32& location) const noexcept override;

    /** Previous non-zero incarnation of an account; 0 if none exists. */
    uint64_t previous_incarnation(const evmc::address& address) const noexcept override;

    std::optional<BlockHeader> read_header(uint64_t block_number,
                                           const evmc::bytes32& block_hash) const noexcept override;

    std::optional<BlockBody> read_body(uint64_t block_number, const evmc::bytes32& block_hash) const noexcept override;

    std::optional<intx::uint256> total_difficulty(uint64_t block_number,
                                                  const evmc::bytes32& block_hash) const noexcept override;

    evmc::bytes32 state_root_hash() const override;

    uint64_t current_canonical_block() const override;

    std::optional<evmc::bytes32> canonical_hash(uint64_t block_number) const override;

    ///@}

    void insert_block(const Block& block, const evmc::bytes32& hash) override;

    void canonize_block(uint64_t block_number, const evmc::bytes32& block_hash) override;

    void decanonize_block(uint64_t block_number) override;

    void insert_receipts(uint64_t block_number, const std::vector<Receipt>& receipts) override;

    /** @name State changes
     *  Change sets are backward changes of the state, i.e. account/storage values <em>at the beginning of a block</em>.
     */
    ///@{

    /** Mark the beginning of a new block.
     * Must be called prior to calling update_account/update_account_code/update_storage.
     */
    void begin_block(uint64_t block_number) override;

    void update_account(const evmc::address& address, std::optional<Account> initial,
                        std::optional<Account> current) override;

    void update_account_code(const evmc::address& address, uint64_t incarnation, const evmc::bytes32& code_hash,
                             ByteView code) override;

    void update_storage(const evmc::address& address, uint64_t incarnation, const evmc::bytes32& location,
                        const evmc::bytes32& initial, const evmc::bytes32& current) override;

    void unwind_state_changes(uint64_t block_number) override;

    ///@}

    /// Account (backward) changes per block
    const absl::btree_map<uint64_t, AccountChanges>& account_changes() const { return account_changes_; }

    /// Storage (backward) changes per block
    const absl::btree_map<uint64_t, StorageChanges>& storage_changes() const { return storage_changes_; }

    /** Approximate size of accumulated DB changes in bytes.*/
    size_t current_batch_size() const noexcept { return batch_size_; }

    void write_to_db();

  private:
    void write_to_state_table();

    void bump_batch_size(size_t key_len, size_t value_len);

    /// Our overlay for the state tree.
    OverlayDB m_db;
    /// Our state tree, as an OverlayDB DB.
    SecureTrieDB<evmc::address, OverlayDB> m_state;
    /// Our address cache. This stores the states of each address that has (or at least might have)
    /// been changed.
    mutable std::unordered_map<evmc::address, Account> m_cache;
    /// Tracks addresses that are known to not exist.
    mutable std::set<evmc::address> m_nonExistingAccountsCache;


    mdbx::txn& txn_;
//    uint64_t prune_from_;
    std::optional<uint64_t> historical_block_{};

    absl::btree_map<Bytes, BlockHeader> headers_{};
    absl::btree_map<Bytes, BlockBody> bodies_{};
    absl::btree_map<Bytes, intx::uint256> difficulty_{};

    absl::flat_hash_map<evmc::address, std::optional<Account>> accounts_;

    // address -> incarnation -> location -> value
    absl::flat_hash_map<evmc::address, absl::btree_map<uint64_t, absl::flat_hash_map<evmc::bytes32, evmc::bytes32>>>
        storage_;

    absl::btree_map<uint64_t, AccountChanges> account_changes_;  // per block
    absl::btree_map<uint64_t, StorageChanges> storage_changes_;  // per block

    absl::btree_map<evmc::address, uint64_t> incarnations_;
    absl::btree_map<evmc::bytes32, Bytes> hash_to_code_;
    absl::btree_map<Bytes, evmc::bytes32> storage_prefix_to_code_hash_;
    absl::btree_map<Bytes, Bytes> receipts_;
    absl::btree_map<Bytes, Bytes> logs_;

    size_t batch_size_{0};

    // Current block stuff
//    uint64_t block_number_{0};
    absl::flat_hash_set<evmc::address> changed_storage_;
};

}  // namespace silkworm::db

#endif  // SILKWORM_OVERLAYSTATE_H
