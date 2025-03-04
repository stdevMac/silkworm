/*
   Copyright 2020-2021 The Silkworm Authors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#include <catch2/catch.hpp>
#include <ethash/keccak.hpp>

#include <silkworm/chain/protocol_param.hpp>
#include <silkworm/common/directories.hpp>
#include <silkworm/common/test_util.hpp>
#include <silkworm/db/buffer.hpp>
#include <silkworm/db/stages.hpp>
#include <silkworm/execution/address.hpp>
#include <silkworm/execution/execution.hpp>
#include <silkworm/rlp/encode.hpp>
#include <silkworm/trie/vector_root.hpp>
#include <silkworm/types/account.hpp>
#include <silkworm/types/block.hpp>

#include "stagedsync.hpp"

using namespace silkworm;
using namespace silkworm::consensus;

TEST_CASE("Unwind Execution") {
    TemporaryDirectory tmp_dir;
    DataDirectory data_dir{tmp_dir.path()};

    // Initialize temporary Database
    db::EnvConfig db_config{data_dir.chaindata().path().string(), /*create*/ true};
    db_config.inmemory = true;
    auto env{db::open_env(db_config)};
    stagedsync::TransactionManager txn{env};
    db::table::create_all(*txn);

    // ---------------------------------------
    // Prepare
    // ---------------------------------------

    uint64_t block_number{1};
    auto miner{0x5a0b54d5dc17e0aadc383d2db43b0a0d3e029c4c_address};

    Block block{};
    block.header.number = block_number;
    block.header.beneficiary = miner;
    block.header.gas_limit = 100'000;
    block.header.gas_used = 98'824;

    static constexpr auto kEncoder = [](Bytes& to, const Receipt& r) { rlp::encode(to, r); };
    std::vector<Receipt> receipts{
        {Transaction::Type::kEip1559, true, block.header.gas_used, {}, {}},
    };
    block.header.receipts_root = trie::root_hash(receipts, kEncoder);

    // This contract initially sets its 0th storage to 0x2a
    // and its 1st storage to 0x01c9.
    // When called, it updates its 0th storage to the input provided.
    Bytes contract_code{*from_hex("600035600055")};
    Bytes deployment_code{*from_hex("602a6000556101c960015560068060166000396000f3") + contract_code};

    block.transactions.resize(1);
    block.transactions[0].data = deployment_code;
    block.transactions[0].gas_limit = block.header.gas_limit;
    block.transactions[0].type = Transaction::Type::kEip1559;
    block.transactions[0].max_priority_fee_per_gas = 0;
    block.transactions[0].max_fee_per_gas = 20 * kGiga;

    auto sender{0xb685342b8c54347aad148e1f22eff3eb3eb29391_address};
    block.transactions[0].r = 1;  // dummy
    block.transactions[0].s = 1;  // dummy
    block.transactions[0].from = sender;

    db::Buffer buffer{*txn, 0};
    Account sender_account{};
    sender_account.balance = kEther;
    buffer.update_account(sender, std::nullopt, sender_account);

    // ---------------------------------------
    // Execute first block
    // ---------------------------------------
    REQUIRE(execute_block(block, buffer, test::kLondonConfig) == ValidationResult::kOk);
    auto contract_address{create_address(sender, /*nonce=*/0)};

    // ---------------------------------------
    // Execute second block
    // ---------------------------------------

    std::string new_val{"000000000000000000000000000000000000000000000000000000000000003e"};

    block_number = 2;
    block.header.number = block_number;
    block.header.gas_used = 26'149;
    receipts[0].cumulative_gas_used = block.header.gas_used;
    block.header.receipts_root = trie::root_hash(receipts, kEncoder);

    block.transactions[0].nonce = 1;
    block.transactions[0].value = 1000;

    block.transactions[0].to = contract_address;
    block.transactions[0].data = *from_hex(new_val);
    block.transactions[0].max_priority_fee_per_gas = 20 * kGiga;

    REQUIRE(execute_block(block, buffer, test::kLondonConfig) == ValidationResult::kOk);

    // ---------------------------------------
    // Execute third block
    // ---------------------------------------

    new_val = "000000000000000000000000000000000000000000000000000000000000003b";

    block_number = 3;
    block.header.number = block_number;

    block.transactions[0].nonce = 2;
    block.transactions[0].data = *from_hex(new_val);

    REQUIRE(execute_block(block, buffer, test::kLondonConfig) == ValidationResult::kOk);

    db::stages::write_stage_progress(*txn, db::stages::kExecutionKey, 3);
    buffer.write_to_db();

    // ---------------------------------------
    // Unwind second block and checks if state is first block
    // ---------------------------------------
    REQUIRE_NOTHROW(stagedsync::check_stagedsync_error(stagedsync::unwind_execution(txn, data_dir.etl().path(), 1)));

    db::Buffer buffer2{*txn, 0};

    std::optional<Account> contract_account{buffer2.read_account(contract_address)};
    REQUIRE(contract_account);
    CHECK((*contract_account).balance == 0);

    std::optional<Account> current_sender{buffer2.read_account(sender)};
    REQUIRE(current_sender != std::nullopt);
    CHECK((*current_sender).balance == kEther);
    CHECK((*current_sender).nonce == 1);

    ethash::hash256 code_hash{keccak256(contract_code)};
    CHECK(to_hex(contract_account->code_hash) == to_hex(full_view(code_hash.bytes)));

    evmc::bytes32 storage_key0{};
    evmc::bytes32 storage0{buffer2.read_storage(contract_address, kDefaultIncarnation, storage_key0)};
    CHECK(to_hex(storage0) == "000000000000000000000000000000000000000000000000000000000000002a");
}
