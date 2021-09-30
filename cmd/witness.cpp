/*
   Copyright 2021 The Silkworm Authors

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

/*
Roughly corresponds to
https://github.com/ledgerwatch/erigon/tree/master/eth/stagedsync#stage-7-compute-state-root-stage

At the moment only full regeneration is supported, not incremental update.

The previous Generate Hashed State Stage must be performed prior to calling this executable.
*/

#include <CLI/CLI.hpp>

#include <silkworm/common/directories.hpp>
#include <silkworm/common/log.hpp>
#include <silkworm/db/mdbx.hpp>
#include <silkworm/db/tables.hpp>
#include <silkworm/trie/intermediate_hashes.hpp>

using namespace silkworm;

int main(int argc, char* argv[]) {
    CLI::App app{"Generate account & storage tries in the DB and compute the state root"};

    namespace fs = std::filesystem;
    using namespace silkworm;

    std::string chaindata{DataDirectory{}.chaindata().path().string()};
    app.add_option("--chaindata", chaindata, "Path to a database populated by Erigon", true)
    ->check(CLI::ExistingDirectory);

    CLI11_PARSE(app, argc, argv);

    SILKWORM_LOG(LogLevel::Info) << "Regenerating account & storage tries. DB: " << chaindata << std::endl;

    try {
        auto data_dir{DataDirectory::from_chaindata(chaindata)};
        data_dir.deploy();
        db::EnvConfig db_config{data_dir.chaindata().path().string()};
        auto env{db::open_env(db_config)};
        auto txn{env.start_write()};
        auto key = mdbx::slice("0xc4423b2e0efd9464cbd84ecb87c57298dc616e08d01ece2ccc745b148add51eb");
        auto state{db::open_cursor(txn, db::table::kTrieOfAccounts)};
        auto p = state.find(key, false);
        if (!p) {
            SILKWORM_LOG(LogLevel::Info) << "HERE" << std::endl;
            txn.insert(db::open_map(txn, db::table::kTrieOfAccounts), key,
                       mdbx::slice("f90211a0f783de3bc9b4b457f8d0a3d258a34063dcaf2f67bfa57d6cdd3b2655e9271a9da0307710d65"
                                   "e30277fe37d52206cee"
                                   "a5b9706af07fe5bd773fb7e8739eb9816e39a000dce9dd749a8219d3d3735a53a9cc76f6b0c16e33205"
                                   "6fa92c9a57293da4c36"
                                   "a005edc1fe3922fd1ed265f0f102500c4352435b2d144ad27831498687b16a9ae6a0f19c4a4bf18b498"
                                   "052d073ae602cb2145a"
                                   "bdc76f5706d05353359e0700989c18a039ccb9315774b395472f8ec6caba33633b7308c0aeeac3a77e5"
                                   "5f053b225a6d1a08c64"
                                   "dcc7d64bd058c6f03c5b5b06533872a8537783f5b1d13e9edd339aa61697a0fe2a1343d502ecc6bf254"
                                   "d8854c5e39d67616f18"
                                   "bfb1e61630930e9e65f347efa06e9d3210ce48ed67419d7c1505dee32984b141ae708daad7d8efdc282"
                                   "af85df7a0465fa9ca5a"
                                   "76e573de50062e3778bc69f81f90154829694f50baa2576c5cf9bca054a7ba7b3d4259e732f874e3e3d"
                                   "34ec66d1f152b1ebca7"
                                   "97f9713f750b26c3eaa06f6253af13b746ffbd6538d44efb18d308305f84ca0a4adb6146dd8c2149a5c"
                                   "6a0aec4de626176d8e7"
                                   "31ee83ce3773fd796b8e98a5004e2aa68201e783a7338d66a0b15df79752d604178914cf0e16b8331cc"
                                   "19e0bd76a381a24c148"
                                   "6b7e306b1f89a0dfa8c08348d60eccb4e46a32dc1ff337f0ed5da51b5a1bc828d72216d2789043a0717"
                                   "66127b0819b61f134e9"
                                   "86bb8f9687d681f2c267bc7f9f6ad21baa41b9e4ae80"));
            auto found = state.find(key, false);
            if (found) {
                SILKWORM_LOG(LogLevel::Info) << "Inserted and found" << std::endl;
            } else {
                SILKWORM_LOG(LogLevel::Info) << "Not Found" << std::endl;
            }

        }
        evmc::bytes32 state_root{trie::regenerate_intermediate_hashes(txn, data_dir.etl().path().string().c_str())};
        SILKWORM_LOG(LogLevel::Info) << "State root " << to_hex(state_root) << std::endl;
        txn.commit();

    } catch (const std::exception& ex) {
        SILKWORM_LOG(LogLevel::Error) << ex.what() << std::endl;
        return -5;
    }

    return 0;
}
