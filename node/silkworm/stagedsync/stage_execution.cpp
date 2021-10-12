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

#include <filesystem>
#include <string>

#include <core/silkworm/state/OverlayState.h>

#include <silkworm/chain/config.hpp>
#include <silkworm/common/endian.hpp>
#include <silkworm/common/log.hpp>
#include <silkworm/common/stopwatch.hpp>
#include <silkworm/consensus/engine.hpp>
#include <silkworm/db/CommonData.h>
#include <silkworm/db/FixedHash.h>
#include <silkworm/db/OverlayDB.h>
#include <silkworm/db/access_layer.hpp>
#include <silkworm/db/buffer.hpp>
#include <silkworm/db/stages.hpp>
#include <silkworm/execution/processor.hpp>

#include "stagedsync.hpp"

using namespace silkworm;
using namespace silkworm::db;

namespace silkworm::stagedsync {

void insert_witness(mdbx::txn& txn, db::StateCacheDB& odb) {
    (void)odb;
    auto state{db::open_cursor(txn, db::table::kTrieOfAccounts)};
    mdbx::slice value{};
    auto key = mdbx::slice("0xbc4da42c51a54602e97563f49e236495284a4efdd4514d2de45f25b6f1ffd5a8");
    value = mdbx::slice(
        "f90211a02b647cb807a3fe8a21d1e6bd9c079e8c8e27d41bfea397f673ffdaf1793383c2a06be39feb8b104bae"
        "d9da563a0428e846bce11d8c02b81b9f5c5452b083b7e082a0d582d771e4e023d0c11329d1904c755c70d2ade5"
        "bb8b4619db17189931d2b45ea086673bbd6c985ddade2c83277a4ba17d124e7f27bdb8f3b0040a81fddcfa63d1"
        "a032ab0e6427a1aa2b30094d07dcd3be1ffaa58e9a2a7eb77385dcdf2c327a9fc2a0b468e2fc9424bf8bcf64cc"
        "3100123f6ddb417c4678736211d9dc315d0fdae53ea0d8658c63e36d6464c9a4e9b1793e4d07fbadd0ab590362"
        "d95bb9900303b9a1baa0ef0bc769d91a7cd9f173878e7ca5916816742d10f356aa5da3a7aa548f85eef3a05a08"
        "1ca5d6a0f654506122d8cfa404d8bc28e6f8642befffa985e109bc6237ada0146b29cb6aa77689e0e80ed74e85"
        "931fd7987d865422a3fb32da20cf6c1f751da0ab0bbef0fe4b3b9ee60dd05f933a0cae50b9076ca3badb8f01b6"
        "fd59c1ca8ff8a0dd2fb81d97f565eb14cf0172ca3f4e79711804178adce020c45bd51e44065f77a00cd280b835"
        "4ecf96060702eba54ca02112ed8e46abc4968caa27756cda7aba22a0d057926d397ae4ad419fdb3b2f025dffb6"
        "5f8637a7ca4f15b66832bca1f5d130a0976f8be12bf9d64b02da5df38a631bda7052ea05a8e789fcfd1d16f21d"
        "341b35a0d5f52284d31ba2841b854af70c0f589b05177052c0c154cdac57ea687f6632b080");
    state.put(key, &value, MDBX_put_flags_t::MDBX_NOOVERWRITE);
    key = mdbx::slice("0x0cd280b8354ecf96060702eba54ca02112ed8e46abc4968caa27756cda7aba22");
    value = mdbx::slice(
        "f90211a05bb628177c51d51037059156bc38749b184010b19cfb2bccf2984c035c7ca4dda0de7e2901f1149a8b"
        "d665b7c533eebb5bf4b693800f92d5e6e5780cf7ec1103dba0534c416a7c0a6bd3b91cfb29440aa6c4b0075f8b"
        "13ffc55f37e9113bc706e2a0a04e338dbc806c6aeba5e866c3a5b89fe5636314081b79f4c7f780b068774a60f7"
        "a08a9fc963d8f38ac3c4a442202a5cdd37eeee607783ba0cedf269acb2cf317f73a01b31a2e7808720e249a78f"
        "ebfb187946852dd0e6d4203673b6889e3744fec4eaa09e2336fd03941004eecb202833190a973e2fe3c2d838d0"
        "3c8f17db5f1733711fa0db272d84889a98c4e06f7f6ee1d99bb4ab55c86e6f186915ab77c06b87c7107da07705"
        "18e934777832226beb3659450be8264c1cd36d924d22f81329cd9b1f2737a0e3ecaad725dfa9468d48fca908b4"
        "4035a82f4014f053cc0bd8a63de51792e37da09c9484466fe8371e1eccd2a8073736ab1d18d4026c86328847bc"
        "7dfd5569e80aa07ddd5a10e9133debbfe9820d12a05299c7b58cc73677f359d2c52e0992f6c935a0c77326c260"
        "fe8a94beaae4102a8eee1f061c8948a4ae1af0ecd9a5571014c33fa063c479ab438d6ff07a95644478048d0266"
        "8725accabe1b4450aafeb4d3501319a0e41a03dd53a8831ea93f52f719caf26c7de8f4bbb8470f437eda754c6e"
        "bf748aa08c374d9655045bf8a0f62996d5d2ce6683be819fd24b455aa8a9dfaf273eda7780");
    state.put(key, &value, MDBX_put_flags_t::MDBX_NOOVERWRITE);
    key = mdbx::slice("0x63c479ab438d6ff07a95644478048d02668725accabe1b4450aafeb4d3501319");
    value = mdbx::slice(
        "f90211a04c502308723955334eae281a825a0bc4cc9199cf7efe6e88b9a76be41d3729d0a079e239bfa186a7c4"
        "2b68aba4e9e42d6899148a4de0d7a1a7f657ed5137b59fcca0b8865e7852fa2a54d7101bd79570e8e279acc0fc"
        "20f26dfe5a95e6f901a5a00fa0b97b62e0c87a0437c8ddece4ea899dec87ecfe5c249edf76c77551b8d4832c57"
        "a0e69ea70b026ccb3fb2e1424f5614a9cededa1f0c3f1958fa95103244f7a6b828a0bf171c930b70944c33687f"
        "f504f834ef66f231a659803eaf81b58356e59ccf6ea0c3675c426ebc43bbf6c3a84d68de2e65c4783ec381281b"
        "fcae0f9fa522cf864fa0dcf284bc2d647b363b667cf980d9c2656fb412e687dd969598065f7c2313bba2a00215"
        "657e3be6da5dc9a6e6026d9503253c4bdba73b8a4f6247f3cf56c411e7e4a065a7d2249c01cf452721880668ce"
        "97a623ee542a8974cebbb6fe89e74e25e058a04aceaf715cf844abc7bb4bc75776085c05d3ed82c90b38e3f911"
        "a096c61f13d4a074bcfd1493c9369d8ad8bf2658be1d95529f11a33e89f9fdb0f2d81f7d35c6cfa055dee0b167"
        "532a4ce037f35c0d6701d70147b70872305b27e9cb113fb32faf9ca08dfb09899f966a4e02c0273866eb58c7ec"
        "87c0c567f6577f39fb8b5ea3a92677a0b407375cdb6af7783f976238230ad494b6e07b7263c0b1e8176b22d0ad"
        "e5b35aa06aae033c38b9b0e7c8407410fbf99ec2b554e016a6bc182e3a47ee097876a6b080");
    state.put(key, &value, MDBX_put_flags_t::MDBX_NOOVERWRITE);
    key = mdbx::slice("0xb8865e7852fa2a54d7101bd79570e8e279acc0fc20f26dfe5a95e6f901a5a00f");
    value = mdbx::slice(
        "f891808080a0208ffdee9912762dce630a8274550d83ee931de1f5d91945d3f64451e9f57b908080a0044c21be"
        "c02b96201e05b104f6c18a9fff309236d14a217d8675d1375a3ebc88a010973a3632910c50a255e8ff10f827d9"
        "25ffd19c8607859544bc5d597282803580a0aa98138a6a7a7e1b77edf735cf1ae4dc4df7fd86a9d5d8c3b184f8"
        "0ce87f1d2080808080808080");
    state.put(key, &value, MDBX_put_flags_t::MDBX_NOOVERWRITE);
    key = mdbx::slice("0x208ffdee9912762dce630a8274550d83ee931de1f5d91945d3f64451e9f57b90");
    value = mdbx::slice(
        "f8749f2083c03ce0b55c830c915be63e218bc20ad607e774fe830315146a47a6bc75b852f8508201ef8a13884d"
        "700f7cea738f17a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a0c5d24601"
        "86f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470");
    state.put(key, &value, MDBX_put_flags_t::MDBX_NOOVERWRITE);
    key = mdbx::slice("0xdd2fb81d97f565eb14cf0172ca3f4e79711804178adce020c45bd51e44065f77");
    value = mdbx::slice(
        "f90211a08aefa559db8519ef6990d4ec4926f38832f994ecc52c0c8db3647bb10a07f750a06f2eff35c9bdf856"
        "8105a98a969b0b1bf567330d1666cefc4d64aa55203404f0a05b1f3e4b64ae3955e39846c845f21f94a71fa38c"
        "499322d60f527e4a013f473ba0f6b45d3d8804bf0a4ee0fbfd923bf4dd89f20512890131a688c4d6f266913f0c"
        "a0f42bf1ee70b1a0e3e44b323e5e31609808bb6dc54b715105c614d812799fac0aa0458a78d5492c33f453ec9c"
        "b02da81e6458845f1ebc7210181e35877cb1dcbdd8a0d190d7e95124c88f3091bd8474f34ec2021f21cfea1bf9"
        "4175c90dbcc3b2c12ea05ba80f5b36547334c267e6f245e8bfe85a898f2a22373c11f5d16e41cdb269a7a0032c"
        "722143d72392828377868282ee10feaf3ef42cc3501b46037189bbeb98aca054f4e4c3b907bbef0dfd64d80367"
        "423b63cfcaf5db26a25fcf564c2393d0d0bba00af06e652d7da6e3506aa480137e2aa2aa24fe23a7e74bdf20aa"
        "c92889824134a0b40a4d5eb05c157782da79f0e54515a0abc5aafcf6e66ff0f4a9a4641fdb0c21a046bed3783c"
        "5f26e101dde08f4b5f0574ff38f084943d0b33c37ae36595575e50a096bdd03dcabf36f1e930dd0459aa1c056b"
        "c370aa516f24ffad4a6c20c34b26aaa0e04cc02856f81fab0368368c8de5394b7e8e840dc6072009b2a1fcb5be"
        "0f81f6a0fdcfad86e1a0648afb8681166d0706d3fdb808437a596b20f86d4aefc25058e080");
    state.put(key, &value, MDBX_put_flags_t::MDBX_NOOVERWRITE);
    key = mdbx::slice("0x96bdd03dcabf36f1e930dd0459aa1c056bc370aa516f24ffad4a6c20c34b26aa");
    value = mdbx::slice(
        "f90211a05a959eb3c42a1cf362d5b922934439310840ad23f4e0fc42649542e3a9c75c1da0071dc96890977be0"
        "280a52a27f7e746e24d2353df8caf99d36b09029c4704ce4a066d634eda8a94ad69bad9642473ccad235b85c1d"
        "22c9027dd5c892dab8cabd2ba09371107c5b286ca362ac684d52732418cfeb8314b2dddc93e3e1113fdd0ae746"
        "a0ca40306c7d65390c8b06348d1fd612e410e902b8f92836901abd443bd7ffd863a06db5cafe213d8adfbc2cb3"
        "53794a3aaf28f184ca33f925ffc71784ab80b50ddea0e3a9a7cdad7e4d5cdad9e0459aac79e9711e4e5c0edc30"
        "8334b8152cdb1b9639a0945e5128579e932d32f6ede20ba7c5ae9e3ccca3d505d0e24c3b3e77d37b9213a0f3c7"
        "fd50823de55e1942bad8dddeccf092a7ccc23055269615a0206a404efa1da0413e7210ca0015117c66af77a860"
        "5134db957ccc9f25adfabe3651cac775ef08a0c71a0f9e5c18a985f8c7d742f483b52d01199fb2e6586d91d245"
        "78144e40a009a0cf7a48d11ce96c97e257797780db24856b36515cedc7970be7c761791408ff8aa00ecddf951e"
        "4a2165f47817d76815aae2ba12365caaaf5867885e30d79753f463a06305fde0d1d6752103b1329d5ad7de4db8"
        "26f55e058b41769d004965d35278cba0799f350e70e4fd7a0462bb089c3da811d9ec8943716f150edd4b6e09d9"
        "502ae4a0322d430a8ec0223a3971997513554bdc43db0a4b0570f6d257b32ae345cd143380");
    state.put(key, &value, MDBX_put_flags_t::MDBX_NOOVERWRITE);
    key = mdbx::slice("0x5a959eb3c42a1cf362d5b922934439310840ad23f4e0fc42649542e3a9c75c1d");
    value = mdbx::slice(
        "f89180808080808080a0e0d591322c0bf95c92b1ca958bc78dfabb2276d751c2d77a77e9029056e14de180a0c9"
        "379825c422eebbfe47962472482f90997d3a9f9d5d66ed40a07a03368d776ba0391efd69111c636706b144b0fe"
        "be9c4c40e34453cb59f1e85cff0812cd784c0180a060ef5cfed04275d55724081d388b3d36d12e661ea96d3c79"
        "ee92048a4d55b81680808080");
    state.put(key, &value, MDBX_put_flags_t::MDBX_NOOVERWRITE);
    key = mdbx::slice("0xb468e2fc9424bf8bcf64cc3100123f6ddb417c4678736211d9dc315d0fdae53e");
    value = mdbx::slice(
        "f90211a0237cf50605b17be42ad83914204158eee344b6c1fb471a7e3bcd6f4ba6c35aa1a0a0e4a692227359be"
        "f80b39a637439a03d4e302fd210cbd64665e7ee2fcd632b1a053bba9baa0b1713099c96329b18843a3f850374c"
        "01a2554279050f0b80e98294a0f41743f078142d3d39b4dd7016fe93ed8a895e46559299b74d97c6bbc82bb67d"
        "a07f6c9db580d115b3039d64e5b3ec919f98c42a59018d67f779f25d735f2ca421a07470dbc6448b2e825ee9ff"
        "ef93c0e28b94ec5d687ed169398ae88d769121dbcba073298a3d4fd2338ed57ee92cb21d8f6292f356b72a9a27"
        "f2ca1a78028ca6bd8ca0c7edaa29b3411e8872b1b487c4163e49942ef079dd8dd6c81588eac8165cb3f1a079a1"
        "4406ddfad2f19f83c7630592502195373c1832d8ccb536ddebe793adda69a0856473f90f8f6522b36d4685d063"
        "206fecb526773998d71f09b2cd5dc7961faea01748b75912e850a866f0add29f38ffa8c82d71cc96d86c546b79"
        "22737a766d89a0eed544b5ffed24e02338f65270fca36488f2974c742eef8247c6e5d521ea2d78a09eb56b4801"
        "44ab26072a6dc13b959715757a4a8e9f6aa5d24fe421c7d22107c7a00418842e60da4bc61b5b6185660b1fa3a9"
        "fce2261335dabe77c0afb70161dd81a0ed9050ae68ec78afccca4a659d7e5bc994a29369ff4860eb48ee7ee22e"
        "bfdde4a0e0535dc0cb928846619a29b7c727e849931f335bc31db9cee08c3bffc5db9d0080");
    state.put(key, &value, MDBX_put_flags_t::MDBX_NOOVERWRITE);
    key = mdbx::slice("0x9eb56b480144ab26072a6dc13b959715757a4a8e9f6aa5d24fe421c7d22107c7");
    value = mdbx::slice(
        "f90211a0fe58ebbf1b3967e59caaf962aa5149fc6f73de8824005902d385a0a9f10a7e59a0fcd91bb862a83f1d"
        "6f12a4cf71be5f29374e0eaf93d8c806298d798547fe075fa0dc13694e325490027df153d7d359a57fa4d2e20e"
        "f15473409bc7d44576817dcda019bafe99573a79737dde5053ff11e1a57a5ae46268085203284dcb6b1c81fbdd"
        "a0705fcad76441688d3994f33f9a1695e743c95aa4ac139a933b1d55debb287f22a0047be678b8d6e4540dc37e"
        "192ebfadbfbde5e9a3835d1a8e087429d6455de593a0381005da04cbade0425341aec8032405700d5663602893"
        "3b9e5927c82dd057f6a02d4f457f0039139d7eb41d866e67898be82e8dd9cd7a3a0a3618462cbc5c0a03a0f7c8"
        "67457f99d50f8d92fd2d0e6a5152d0f366cea526b904e3d3b2f8fb7f93eca039463c1c1cb4bbc68ce8669268e3"
        "ec65326e3d6e90cb7b90e54adaf9a5bd5426a028599d49b949a6da964a04db5526be221801a351321e639811f0"
        "7fe9a82534f1a0f29167044da496fc43e735389d45bfb5ae8fa2715847cfd0fd4303a21febfd6fa0673e0bbf4c"
        "c29ea263f8969f13828ee43bf2e58f664f498c3ee10a97a4b29976a0c3f8966e796ba2c994b8995c250ea9a76c"
        "3c252b5ca6284111a9bbd69babcfc2a01caafbe33b5dd92147a30523c42ece802664f72db7cc81be1f6b765ed4"
        "75e960a05b6683eeef3fb246da89e3773d4b2e0c4d65ad9ef5484396918d55372271673a80");
    state.put(key, &value, MDBX_put_flags_t::MDBX_NOOVERWRITE);
    key = mdbx::slice("0x047be678b8d6e4540dc37e192ebfadbfbde5e9a3835d1a8e087429d6455de593");
    value = mdbx::slice(
        "f8b1808080a0147106df653127b1633f2192c27b5565c742873a72d8fd4baea5781e06ab949580808080a050bf"
        "8d407a0638955a89b2cb1c0237a0b8e561f9dcab89c638a204a823a23a3f80a01c626bae0d4a3bbc67251b097e"
        "cdc46f099b045d06077ca13c1351feaf21a97480a0569a8ad3fa1f34dcffa2eac7fea3237912372f00a5c9e156"
        "9e0fcafe970a945080a0b4937c33bc00f4b5119a267a02117bafee772f8a08a448b751bf47fae879a4818080");
    state.put(key, &value, MDBX_put_flags_t::MDBX_NOOVERWRITE);
    key = mdbx::slice("0xb4937c33bc00f4b5119a267a02117bafee772f8a08a448b751bf47fae879a481");
    value = mdbx::slice(
        "f85180808080a045f97b36dd902edb23c07c25656410f67bda6a7fd8eadb0de59b5d8027d666ba8080808080a0"
        "040a878d39e577a175308d815d07eedb77e3b75c1e5ca7f22ee3cdf3f5d8ba14808080808080");
    state.put(key, &value, MDBX_put_flags_t::MDBX_NOOVERWRITE);
    key = mdbx::slice("0x45f97b36dd902edb23c07c25656410f67bda6a7fd8eadb0de59b5d8027d666ba");
    value = mdbx::slice(
        "f8729e3236a9a856bf5bed4d5dbd59e1c6194206269ddd1402847d2c38d3721fabb851f84f8240f1898bce2ec2"
        "48cc449cf7a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a0c5d2460186f7"
        "233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470");
    state.put(key, &value, MDBX_put_flags_t::MDBX_NOOVERWRITE);
    insert_on_overlay_db(odb);
}

void insert_on_overlay_db(db::StateCacheDB& odb) {
    auto valueBytes =
        "f90211a02b647cb807a3fe8a21d1e6bd9c079e8c8e27d41bfea397f673ffdaf1793383c2a06be39feb8b104bae"
        "d9da563a0428e846bce11d8c02b81b9f5c5452b083b7e082a0d582d771e4e023d0c11329d1904c755c70d2ade5"
        "bb8b4619db17189931d2b45ea086673bbd6c985ddade2c83277a4ba17d124e7f27bdb8f3b0040a81fddcfa63d1"
        "a032ab0e6427a1aa2b30094d07dcd3be1ffaa58e9a2a7eb77385dcdf2c327a9fc2a0b468e2fc9424bf8bcf64cc"
        "3100123f6ddb417c4678736211d9dc315d0fdae53ea0d8658c63e36d6464c9a4e9b1793e4d07fbadd0ab590362"
        "d95bb9900303b9a1baa0ef0bc769d91a7cd9f173878e7ca5916816742d10f356aa5da3a7aa548f85eef3a05a08"
        "1ca5d6a0f654506122d8cfa404d8bc28e6f8642befffa985e109bc6237ada0146b29cb6aa77689e0e80ed74e85"
        "931fd7987d865422a3fb32da20cf6c1f751da0ab0bbef0fe4b3b9ee60dd05f933a0cae50b9076ca3badb8f01b6"
        "fd59c1ca8ff8a0dd2fb81d97f565eb14cf0172ca3f4e79711804178adce020c45bd51e44065f77a00cd280b835"
        "4ecf96060702eba54ca02112ed8e46abc4968caa27756cda7aba22a0d057926d397ae4ad419fdb3b2f025dffb6"
        "5f8637a7ca4f15b66832bca1f5d130a0976f8be12bf9d64b02da5df38a631bda7052ea05a8e789fcfd1d16f21d"
        "341b35a0d5f52284d31ba2841b854af70c0f589b05177052c0c154cdac57ea687f6632b080";
    odb.insert(h256{"bc4da42c51a54602e97563f49e236495284a4efdd4514d2de45f25b6f1ffd5a8"}, valueBytes);
    valueBytes =
        "f90211a05bb628177c51d51037059156bc38749b184010b19cfb2bccf2984c035c7ca4dda0de7e2901f1149a8b"
        "d665b7c533eebb5bf4b693800f92d5e6e5780cf7ec1103dba0534c416a7c0a6bd3b91cfb29440aa6c4b0075f8b"
        "13ffc55f37e9113bc706e2a0a04e338dbc806c6aeba5e866c3a5b89fe5636314081b79f4c7f780b068774a60f7"
        "a08a9fc963d8f38ac3c4a442202a5cdd37eeee607783ba0cedf269acb2cf317f73a01b31a2e7808720e249a78f"
        "ebfb187946852dd0e6d4203673b6889e3744fec4eaa09e2336fd03941004eecb202833190a973e2fe3c2d838d0"
        "3c8f17db5f1733711fa0db272d84889a98c4e06f7f6ee1d99bb4ab55c86e6f186915ab77c06b87c7107da07705"
        "18e934777832226beb3659450be8264c1cd36d924d22f81329cd9b1f2737a0e3ecaad725dfa9468d48fca908b4"
        "4035a82f4014f053cc0bd8a63de51792e37da09c9484466fe8371e1eccd2a8073736ab1d18d4026c86328847bc"
        "7dfd5569e80aa07ddd5a10e9133debbfe9820d12a05299c7b58cc73677f359d2c52e0992f6c935a0c77326c260"
        "fe8a94beaae4102a8eee1f061c8948a4ae1af0ecd9a5571014c33fa063c479ab438d6ff07a95644478048d0266"
        "8725accabe1b4450aafeb4d3501319a0e41a03dd53a8831ea93f52f719caf26c7de8f4bbb8470f437eda754c6e"
        "bf748aa08c374d9655045bf8a0f62996d5d2ce6683be819fd24b455aa8a9dfaf273eda7780";
    odb.insert(h256{"0cd280b8354ecf96060702eba54ca02112ed8e46abc4968caa27756cda7aba22"}, valueBytes);
    valueBytes =
        "f90211a04c502308723955334eae281a825a0bc4cc9199cf7efe6e88b9a76be41d3729d0a079e239bfa186a7c4"
        "2b68aba4e9e42d6899148a4de0d7a1a7f657ed5137b59fcca0b8865e7852fa2a54d7101bd79570e8e279acc0fc"
        "20f26dfe5a95e6f901a5a00fa0b97b62e0c87a0437c8ddece4ea899dec87ecfe5c249edf76c77551b8d4832c57"
        "a0e69ea70b026ccb3fb2e1424f5614a9cededa1f0c3f1958fa95103244f7a6b828a0bf171c930b70944c33687f"
        "f504f834ef66f231a659803eaf81b58356e59ccf6ea0c3675c426ebc43bbf6c3a84d68de2e65c4783ec381281b"
        "fcae0f9fa522cf864fa0dcf284bc2d647b363b667cf980d9c2656fb412e687dd969598065f7c2313bba2a00215"
        "657e3be6da5dc9a6e6026d9503253c4bdba73b8a4f6247f3cf56c411e7e4a065a7d2249c01cf452721880668ce"
        "97a623ee542a8974cebbb6fe89e74e25e058a04aceaf715cf844abc7bb4bc75776085c05d3ed82c90b38e3f911"
        "a096c61f13d4a074bcfd1493c9369d8ad8bf2658be1d95529f11a33e89f9fdb0f2d81f7d35c6cfa055dee0b167"
        "532a4ce037f35c0d6701d70147b70872305b27e9cb113fb32faf9ca08dfb09899f966a4e02c0273866eb58c7ec"
        "87c0c567f6577f39fb8b5ea3a92677a0b407375cdb6af7783f976238230ad494b6e07b7263c0b1e8176b22d0ad"
        "e5b35aa06aae033c38b9b0e7c8407410fbf99ec2b554e016a6bc182e3a47ee097876a6b080";
    odb.insert(h256{"63c479ab438d6ff07a95644478048d02668725accabe1b4450aafeb4d3501319"}, valueBytes);
    valueBytes =
        "f891808080a0208ffdee9912762dce630a8274550d83ee931de1f5d91945d3f64451e9f57b908080a0044c21be"
        "c02b96201e05b104f6c18a9fff309236d14a217d8675d1375a3ebc88a010973a3632910c50a255e8ff10f827d9"
        "25ffd19c8607859544bc5d597282803580a0aa98138a6a7a7e1b77edf735cf1ae4dc4df7fd86a9d5d8c3b184f8"
        "0ce87f1d2080808080808080";
    odb.insert(h256{"b8865e7852fa2a54d7101bd79570e8e279acc0fc20f26dfe5a95e6f901a5a00f"}, valueBytes);
    valueBytes =
        "f8749f2083c03ce0b55c830c915be63e218bc20ad607e774fe830315146a47a6bc75b852f8508201ef8a13884d"
        "700f7cea738f17a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a0c5d24601"
        "86f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470";
    odb.insert(h256{"208ffdee9912762dce630a8274550d83ee931de1f5d91945d3f64451e9f57b90"}, valueBytes);
    valueBytes =
        "f90211a08aefa559db8519ef6990d4ec4926f38832f994ecc52c0c8db3647bb10a07f750a06f2eff35c9bdf856"
        "8105a98a969b0b1bf567330d1666cefc4d64aa55203404f0a05b1f3e4b64ae3955e39846c845f21f94a71fa38c"
        "499322d60f527e4a013f473ba0f6b45d3d8804bf0a4ee0fbfd923bf4dd89f20512890131a688c4d6f266913f0c"
        "a0f42bf1ee70b1a0e3e44b323e5e31609808bb6dc54b715105c614d812799fac0aa0458a78d5492c33f453ec9c"
        "b02da81e6458845f1ebc7210181e35877cb1dcbdd8a0d190d7e95124c88f3091bd8474f34ec2021f21cfea1bf9"
        "4175c90dbcc3b2c12ea05ba80f5b36547334c267e6f245e8bfe85a898f2a22373c11f5d16e41cdb269a7a0032c"
        "722143d72392828377868282ee10feaf3ef42cc3501b46037189bbeb98aca054f4e4c3b907bbef0dfd64d80367"
        "423b63cfcaf5db26a25fcf564c2393d0d0bba00af06e652d7da6e3506aa480137e2aa2aa24fe23a7e74bdf20aa"
        "c92889824134a0b40a4d5eb05c157782da79f0e54515a0abc5aafcf6e66ff0f4a9a4641fdb0c21a046bed3783c"
        "5f26e101dde08f4b5f0574ff38f084943d0b33c37ae36595575e50a096bdd03dcabf36f1e930dd0459aa1c056b"
        "c370aa516f24ffad4a6c20c34b26aaa0e04cc02856f81fab0368368c8de5394b7e8e840dc6072009b2a1fcb5be"
        "0f81f6a0fdcfad86e1a0648afb8681166d0706d3fdb808437a596b20f86d4aefc25058e080";
    odb.insert(h256{"dd2fb81d97f565eb14cf0172ca3f4e79711804178adce020c45bd51e44065f77"}, valueBytes);
    valueBytes =
        "f90211a05a959eb3c42a1cf362d5b922934439310840ad23f4e0fc42649542e3a9c75c1da0071dc96890977be0"
        "280a52a27f7e746e24d2353df8caf99d36b09029c4704ce4a066d634eda8a94ad69bad9642473ccad235b85c1d"
        "22c9027dd5c892dab8cabd2ba09371107c5b286ca362ac684d52732418cfeb8314b2dddc93e3e1113fdd0ae746"
        "a0ca40306c7d65390c8b06348d1fd612e410e902b8f92836901abd443bd7ffd863a06db5cafe213d8adfbc2cb3"
        "53794a3aaf28f184ca33f925ffc71784ab80b50ddea0e3a9a7cdad7e4d5cdad9e0459aac79e9711e4e5c0edc30"
        "8334b8152cdb1b9639a0945e5128579e932d32f6ede20ba7c5ae9e3ccca3d505d0e24c3b3e77d37b9213a0f3c7"
        "fd50823de55e1942bad8dddeccf092a7ccc23055269615a0206a404efa1da0413e7210ca0015117c66af77a860"
        "5134db957ccc9f25adfabe3651cac775ef08a0c71a0f9e5c18a985f8c7d742f483b52d01199fb2e6586d91d245"
        "78144e40a009a0cf7a48d11ce96c97e257797780db24856b36515cedc7970be7c761791408ff8aa00ecddf951e"
        "4a2165f47817d76815aae2ba12365caaaf5867885e30d79753f463a06305fde0d1d6752103b1329d5ad7de4db8"
        "26f55e058b41769d004965d35278cba0799f350e70e4fd7a0462bb089c3da811d9ec8943716f150edd4b6e09d9"
        "502ae4a0322d430a8ec0223a3971997513554bdc43db0a4b0570f6d257b32ae345cd143380";
    odb.insert(h256{"96bdd03dcabf36f1e930dd0459aa1c056bc370aa516f24ffad4a6c20c34b26aa"}, valueBytes);
    valueBytes =
        "f89180808080808080a0e0d591322c0bf95c92b1ca958bc78dfabb2276d751c2d77a77e9029056e14de180a0c9"
        "379825c422eebbfe47962472482f90997d3a9f9d5d66ed40a07a03368d776ba0391efd69111c636706b144b0fe"
        "be9c4c40e34453cb59f1e85cff0812cd784c0180a060ef5cfed04275d55724081d388b3d36d12e661ea96d3c79"
        "ee92048a4d55b81680808080";
    odb.insert(h256{"5a959eb3c42a1cf362d5b922934439310840ad23f4e0fc42649542e3a9c75c1d"}, valueBytes);
    valueBytes =
        "f90211a0237cf50605b17be42ad83914204158eee344b6c1fb471a7e3bcd6f4ba6c35aa1a0a0e4a692227359be"
        "f80b39a637439a03d4e302fd210cbd64665e7ee2fcd632b1a053bba9baa0b1713099c96329b18843a3f850374c"
        "01a2554279050f0b80e98294a0f41743f078142d3d39b4dd7016fe93ed8a895e46559299b74d97c6bbc82bb67d"
        "a07f6c9db580d115b3039d64e5b3ec919f98c42a59018d67f779f25d735f2ca421a07470dbc6448b2e825ee9ff"
        "ef93c0e28b94ec5d687ed169398ae88d769121dbcba073298a3d4fd2338ed57ee92cb21d8f6292f356b72a9a27"
        "f2ca1a78028ca6bd8ca0c7edaa29b3411e8872b1b487c4163e49942ef079dd8dd6c81588eac8165cb3f1a079a1"
        "4406ddfad2f19f83c7630592502195373c1832d8ccb536ddebe793adda69a0856473f90f8f6522b36d4685d063"
        "206fecb526773998d71f09b2cd5dc7961faea01748b75912e850a866f0add29f38ffa8c82d71cc96d86c546b79"
        "22737a766d89a0eed544b5ffed24e02338f65270fca36488f2974c742eef8247c6e5d521ea2d78a09eb56b4801"
        "44ab26072a6dc13b959715757a4a8e9f6aa5d24fe421c7d22107c7a00418842e60da4bc61b5b6185660b1fa3a9"
        "fce2261335dabe77c0afb70161dd81a0ed9050ae68ec78afccca4a659d7e5bc994a29369ff4860eb48ee7ee22e"
        "bfdde4a0e0535dc0cb928846619a29b7c727e849931f335bc31db9cee08c3bffc5db9d0080";
    odb.insert(h256{"b468e2fc9424bf8bcf64cc3100123f6ddb417c4678736211d9dc315d0fdae53e"}, valueBytes);
    valueBytes =
        "f90211a0fe58ebbf1b3967e59caaf962aa5149fc6f73de8824005902d385a0a9f10a7e59a0fcd91bb862a83f1d"
        "6f12a4cf71be5f29374e0eaf93d8c806298d798547fe075fa0dc13694e325490027df153d7d359a57fa4d2e20e"
        "f15473409bc7d44576817dcda019bafe99573a79737dde5053ff11e1a57a5ae46268085203284dcb6b1c81fbdd"
        "a0705fcad76441688d3994f33f9a1695e743c95aa4ac139a933b1d55debb287f22a0047be678b8d6e4540dc37e"
        "192ebfadbfbde5e9a3835d1a8e087429d6455de593a0381005da04cbade0425341aec8032405700d5663602893"
        "3b9e5927c82dd057f6a02d4f457f0039139d7eb41d866e67898be82e8dd9cd7a3a0a3618462cbc5c0a03a0f7c8"
        "67457f99d50f8d92fd2d0e6a5152d0f366cea526b904e3d3b2f8fb7f93eca039463c1c1cb4bbc68ce8669268e3"
        "ec65326e3d6e90cb7b90e54adaf9a5bd5426a028599d49b949a6da964a04db5526be221801a351321e639811f0"
        "7fe9a82534f1a0f29167044da496fc43e735389d45bfb5ae8fa2715847cfd0fd4303a21febfd6fa0673e0bbf4c"
        "c29ea263f8969f13828ee43bf2e58f664f498c3ee10a97a4b29976a0c3f8966e796ba2c994b8995c250ea9a76c"
        "3c252b5ca6284111a9bbd69babcfc2a01caafbe33b5dd92147a30523c42ece802664f72db7cc81be1f6b765ed4"
        "75e960a05b6683eeef3fb246da89e3773d4b2e0c4d65ad9ef5484396918d55372271673a80";
    odb.insert(h256{"9eb56b480144ab26072a6dc13b959715757a4a8e9f6aa5d24fe421c7d22107c7"}, valueBytes);
    valueBytes =
        "f8b1808080a0147106df653127b1633f2192c27b5565c742873a72d8fd4baea5781e06ab949580808080a050bf"
        "8d407a0638955a89b2cb1c0237a0b8e561f9dcab89c638a204a823a23a3f80a01c626bae0d4a3bbc67251b097e"
        "cdc46f099b045d06077ca13c1351feaf21a97480a0569a8ad3fa1f34dcffa2eac7fea3237912372f00a5c9e156"
        "9e0fcafe970a945080a0b4937c33bc00f4b5119a267a02117bafee772f8a08a448b751bf47fae879a4818080";
    odb.insert(h256{"047be678b8d6e4540dc37e192ebfadbfbde5e9a3835d1a8e087429d6455de593"}, valueBytes);
    valueBytes =
        "f85180808080a045f97b36dd902edb23c07c25656410f67bda6a7fd8eadb0de59b5d8027d666ba8080808080a0"
        "040a878d39e577a175308d815d07eedb77e3b75c1e5ca7f22ee3cdf3f5d8ba14808080808080";
    odb.insert(h256{"b4937c33bc00f4b5119a267a02117bafee772f8a08a448b751bf47fae879a481"}, valueBytes);
    valueBytes =
        "f8729e3236a9a856bf5bed4d5dbd59e1c6194206269ddd1402847d2c38d3721fabb851f84f8240f1898bce2ec2"
        "48cc449cf7a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a0c5d2460186f7"
        "233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470";
    odb.insert(h256{"45f97b36dd902edb23c07c25656410f67bda6a7fd8eadb0de59b5d8027d666ba"}, valueBytes);
}

StageResult insert_blocks(mdbx::txn& txn, const std::vector<std::string>& blocks_rlp) {
    auto state{db::open_cursor(txn, db::table::kHeaders)};

    for (auto& block_rlp : blocks_rlp) {
        Bytes rlp_bytes{*from_hex(block_rlp)};
        ByteView in{rlp_bytes};
        Block block{};
        Bytes rlp{};
        if (rlp::decode(in, block) != rlp::DecodingResult::kOk) return StageResult::kDecodingError;
        block.recover_senders();
        rlp::encode(rlp, block.header);
        auto key{db::block_key(block.header.number, block.header.hash().bytes)};
        auto contains = state.find(mdbx::slice(key), false);
        if (!contains) txn.insert(db::open_map(txn, db::table::kHeaders), mdbx::slice(key), mdbx::slice(rlp));
    }
    return StageResult::kSuccess;
}

StageResult execute_block(mdbx::txn& txn, Block& block, StateCacheDB& odb, h256 root_hash) {
    TransactionManager tm{txn};
    const auto config{db::read_chain_config(*tm)};
    const auto storage_mode{db::read_storage_mode(*tm)};
    (void)odb;
    OverlayState buffer{odb};

    buffer.debug_tree();
    buffer.set_root(root_hash);
    auto block_num{block.header.number};
    AnalysisCache analysis_cache;
    ExecutionStatePool state_pool;

    std::vector<Receipt> receipts;

    auto consensus_engine{consensus::engine_factory(*config)};
    if (!consensus_engine) {
        return StageResult::kUnknownConsensusEngine;
    }

    ExecutionProcessor processor{block, *consensus_engine, buffer, *config};
    processor.evm().advanced_analysis_cache = &analysis_cache;
    processor.evm().state_pool = &state_pool;

    if (const auto res{processor.execute_and_write_block(receipts)}; res != ValidationResult::kOk) {
        SILKWORM_LOG(LogLevel::Error) << "Validation error " << magic_enum::enum_name<ValidationResult>(res)
                                      << " at block " << block_num << std::endl;
        return StageResult::kInvalidBlock;
    }

    if (storage_mode.Receipts && block_num >= 0) {
        buffer.insert_receipts(block_num, receipts);
    }

    SILKWORM_LOG(LogLevel::Debug) << "Blocks <= " << block_num << " executed" << std::endl;
    //    buffer.write_to_db();
    return StageResult::kSuccess;
}

// block_num is input-output
static StageResult execute_batch_of_blocks(mdbx::txn& txn, const ChainConfig& config, const BlockNum max_block,
                                           const db::StorageMode& storage_mode, const size_t batch_size,
                                           BlockNum& block_num, BlockNum prune_from) noexcept {
    try {
        db::Buffer buffer{txn, prune_from};
        AnalysisCache analysis_cache;
        ExecutionStatePool state_pool;
        std::vector<Receipt> receipts;
        auto consensus_engine{consensus::engine_factory(config)};
        if (!consensus_engine) {
            return StageResult::kUnknownConsensusEngine;
        }
        while (true) {
            std::optional<BlockWithHash> bh{db::read_block(txn, block_num, /*read_senders=*/true)};
            if (!bh.has_value()) {
                return StageResult::kBadChainSequence;
            }

            ExecutionProcessor processor{bh->block, *consensus_engine, buffer, config};
            processor.evm().advanced_analysis_cache = &analysis_cache;
            processor.evm().state_pool = &state_pool;

            if (const auto res{processor.execute_and_write_block(receipts)}; res != ValidationResult::kOk) {
                SILKWORM_LOG(LogLevel::Error) << "Validation error " << magic_enum::enum_name<ValidationResult>(res)
                                              << " at block " << block_num << std::endl;
                return StageResult::kInvalidBlock;
            }

            if (storage_mode.Receipts && block_num >= prune_from) {
                buffer.insert_receipts(block_num, receipts);
            }

            if (block_num % 1000 == 0) {
                SILKWORM_LOG(LogLevel::Debug) << "Blocks <= " << block_num << " executed" << std::endl;
            }

            if (buffer.current_batch_size() >= batch_size || block_num >= max_block) {
                buffer.write_to_db();
                return StageResult::kSuccess;
            }

            ++block_num;
        }
    } catch (const mdbx::exception& ex) {
        SILKWORM_LOG(LogLevel::Error) << "Database error " << ex.what() << " at block " << block_num << std::endl;
        return StageResult::kDbError;
    } catch (const db::MissingSenders&) {
        SILKWORM_LOG(LogLevel::Error) << "Missing or incorrect senders at block " << block_num << std::endl;
        return StageResult::kMissingSenders;
    } catch (const rlp::DecodingError& ex) {
        SILKWORM_LOG(LogLevel::Error) << ex.what() << " at block " << block_num << std::endl;
        return StageResult::kDecodingError;
    } catch (const std::exception& ex) {
        SILKWORM_LOG(LogLevel::Error) << "Unexpected error " << ex.what() << " at block " << block_num << std::endl;
        return StageResult::kUnexpectedError;
    } catch (...) {
        SILKWORM_LOG(LogLevel::Error) << "Unknown error at block " << block_num << std::endl;
        return StageResult::kUnknownError;
    }
}

StageResult stage_execution(TransactionManager& txn, const std::filesystem::path&, size_t batch_size,
                            uint64_t prune_from) {
    StageResult res{StageResult::kSuccess};

    try {
        const auto chain_config{db::read_chain_config(*txn)};
        if (!chain_config.has_value()) {
            return StageResult::kUnknownChainId;
        }
        const auto storage_mode{db::read_storage_mode(*txn)};

        const BlockNum max_block{db::stages::read_stage_progress(*txn, db::stages::kBlockBodiesKey)};
        BlockNum block_num{db::stages::read_stage_progress(*txn, db::stages::kExecutionKey) + 1};
        if (block_num > max_block) {
            SILKWORM_LOG(LogLevel::Error) << "Stage progress is " << (block_num - 1)
                                          << " which is <= than requested block_to " << max_block << std::endl;
            return StageResult::kInvalidRange;
        }

        // Execution needs senders hence we need to check whether sender's stage is
        // at least at max_block as set above
        const BlockNum max_block_senders{db::stages::read_stage_progress(*txn, db::stages::kSendersKey)};
        if (max_block > max_block_senders) {
            SILKWORM_LOG(LogLevel::Error) << "Sender's stage progress is " << (max_block_senders)
                                          << " which is <= than requested block_to " << max_block << std::endl;
            return StageResult::kMissingSenders;
        }

        StopWatch sw{};
        (void)sw.start();

        for (; block_num <= max_block; ++block_num) {
            res = execute_batch_of_blocks(*txn, chain_config.value(), max_block, storage_mode, batch_size, block_num,
                                          prune_from);
            if (res != StageResult::kSuccess) {
                return res;
            }

            db::stages::write_stage_progress(*txn, db::stages::kExecutionKey, block_num);

            txn.commit();

            (void)sw.lap();
            SILKWORM_LOG(LogLevel::Info) << (block_num == max_block ? "All blocks" : "Blocks") << " <= " << block_num
                                         << " committed"
                                         << " in " << sw.format(sw.laps().back().second) << std::endl;
        }
    } catch (const mdbx::exception& ex) {
        SILKWORM_LOG(LogLevel::Error) << "Database Error " << ex.what() << " in stage_execution" << std::endl;
        return StageResult::kDbError;
    } catch (const std::exception& ex) {
        SILKWORM_LOG(LogLevel::Error) << "Unexpected error " << ex.what() << " in stage execution " << std::endl;
        return StageResult::kUnexpectedError;
    }

    return res;
}

// Revert State for given address/storage location
static void revert_state(ByteView key, ByteView value, mdbx::cursor& plain_state_table,
                         mdbx::cursor& plain_code_table) {
    if (key.size() == kAddressLength) {
        if (!value.empty()) {
            auto [account, err1]{decode_account_from_storage(value)};
            rlp::err_handler(err1);
            if (account.incarnation > 0 && account.code_hash == kEmptyHash) {
                Bytes code_hash_key(kAddressLength + db::kIncarnationLength, '\0');
                std::memcpy(&code_hash_key[0], &key[0], kAddressLength);
                endian::store_big_u64(&code_hash_key[kAddressLength], account.incarnation);
                auto new_code_hash{plain_code_table.find(db::to_slice(code_hash_key))};
                std::memcpy(&account.code_hash.bytes[0], new_code_hash.value.iov_base, kHashLength);
            }
            // cleaning up contract codes
            auto state_account_encoded{plain_state_table.find(db::to_slice(key), /*throw_notfound=*/false)};
            if (state_account_encoded) {
                auto [state_incarnation, err2]{extract_incarnation(db::from_slice(state_account_encoded.value))};
                rlp::err_handler(err2);
                // cleanup each code incarnation
                for (uint64_t i = state_incarnation; i > account.incarnation && i > 0; --i) {
                    Bytes key_hash(kAddressLength + 8, '\0');
                    std::memcpy(&key_hash[0], key.data(), kAddressLength);
                    endian::store_big_u64(&key_hash[kAddressLength], i);
                    if (plain_code_table.seek(db::to_slice(key_hash))) {
                        plain_code_table.erase();
                    }
                }
            }
            auto new_encoded_account{account.encode_for_storage(false)};
            if (plain_state_table.seek(db::to_slice(key))) {
                plain_state_table.erase(/*whole_multivalue*/ true);
            }
            plain_state_table.upsert(db::to_slice(key), db::to_slice(new_encoded_account));
        } else {
            if (plain_code_table.seek(db::to_slice(key))) {
                plain_code_table.erase();
            }
        }
        return;
    }
    auto location{key.substr(kAddressLength + db::kIncarnationLength)};
    auto key1{key.substr(0, kAddressLength + db::kIncarnationLength)};
    if (db::find_value_suffix(plain_state_table, key1, location) != std::nullopt) {
        plain_state_table.erase();
    }
    if (!value.empty()) {
        Bytes data{location};
        data.append(value);
        plain_state_table.upsert(db::to_slice(key1), db::to_slice(data));
    }
}

// For given changeset cursor/bucket it reverts the changes on states buckets
static void unwind_state_from_changeset(mdbx::cursor& source, mdbx::cursor& plain_state_table,
                                        mdbx::cursor& plain_code_table, BlockNum unwind_to) {
    auto src_data{source.to_last(/*throw_notfound*/ false)};
    while (src_data) {
        Bytes key(db::from_slice(src_data.key));
        Bytes value(db::from_slice(src_data.value));
        const BlockNum block_number = endian::load_big_u64(&key[0]);
        if (block_number == unwind_to) {
            break;
        }
        auto [new_key, new_value]{db::change_set_to_plain_state_format(key, value)};
        revert_state(new_key, new_value, plain_state_table, plain_code_table);
        src_data = source.to_previous(/*throw_notfound*/ false);
    }
}

StageResult unwind_execution(TransactionManager& txn, const std::filesystem::path&, uint64_t unwind_to) {
    BlockNum execution_progress{db::stages::read_stage_progress(*txn, db::stages::kExecutionKey)};
    if (unwind_to >= execution_progress) {
        return StageResult::kSuccess;
    }

    SILKWORM_LOG(LogLevel::Info) << "Unwind Execution from " << execution_progress << " to " << unwind_to << std::endl;

    static const db::MapConfig unwind_tables[7] = {
        db::table::kPlainState,         //
        db::table::kPlainContractCode,  //
        db::table::kAccountChangeSet,   //
        db::table::kStorageChangeSet,   //
        db::table::kBlockReceipts,      //
        db::table::kLogs,               //
        db::table::kCallTraceSet        //
    };

    try {
        if (unwind_to == 0) {
            for (const auto& unwind_table : unwind_tables) {
                auto unwind_map_handle{db::open_map(*txn, unwind_table)};
                txn->clear_map(unwind_map_handle);
            }
        } else {
            {
                auto plain_state_table{db::open_cursor(*txn, db::table::kPlainState)};
                auto plain_code_table{db::open_cursor(*txn, db::table::kPlainContractCode)};
                auto account_changeset_table{db::open_cursor(*txn, db::table::kAccountChangeSet)};
                auto storage_changeset_table{db::open_cursor(*txn, db::table::kStorageChangeSet)};
                unwind_state_from_changeset(account_changeset_table, plain_state_table, plain_code_table, unwind_to);
                unwind_state_from_changeset(storage_changeset_table, plain_state_table, plain_code_table, unwind_to);
            }

            // Delete records which has keys greater than unwind point
            // Note erasing forward the start key is included that's why we increase unwind_to by 1
            Bytes start_key(8, '\0');
            endian::store_big_u64(&start_key[0], unwind_to + 1);
            for (int i = 2; i < 7; ++i) {
                auto unwind_cursor{db::open_cursor(*txn, unwind_tables[i])};
                auto erased{db::cursor_erase(unwind_cursor, start_key, db::CursorMoveDirection::Forward)};
                SILKWORM_LOG(LogLevel::Info)
                    << "Erased " << erased << " records from " << unwind_tables[i].name << std::endl;
                unwind_cursor.close();
            }
        }
        txn.commit();
        return StageResult::kSuccess;
    } catch (const mdbx::exception& ex) {
        SILKWORM_LOG(LogLevel::Error) << "Unexpected db error in " << std::string(__FUNCTION__) << " : " << ex.what()
                                      << std::endl;
        return StageResult::kDbError;
    } catch (...) {
        SILKWORM_LOG(LogLevel::Error) << "Unexpected unknown error in " << std::string(__FUNCTION__) << std::endl;
        return StageResult::kUnexpectedError;
    }
}

StageResult prune_execution(TransactionManager& txn, const std::filesystem::path&, uint64_t prune_from) {
    static const db::MapConfig prune_tables[] = {
        db::table::kAccountChangeSet,  //
        db::table::kStorageChangeSet,  //
        db::table::kBlockReceipts,     //
        db::table::kCallTraceSet,      //
        db::table::kLogs               //
    };

    try {
        const auto prune_point{db::block_key(prune_from)};
        for (const auto& prune_table : prune_tables) {
            auto prune_cursor{db::open_cursor(*txn, prune_table)};
            auto erased{db::cursor_erase(prune_cursor, prune_point, db::CursorMoveDirection::Reverse)};
            SILKWORM_LOG(LogLevel::Info) << "Erased " << erased << " records from " << prune_table.name << std::endl;
            prune_cursor.close();
        }
        txn.commit();  // TODO(Giulio) Should we commit here or at return of stage ?
        return StageResult::kSuccess;
    } catch (const mdbx::exception& ex) {
        SILKWORM_LOG(LogLevel::Error) << "Unexpected db error in " << std::string(__FUNCTION__) << " : " << ex.what()
                                      << std::endl;
        return StageResult::kDbError;
    } catch (...) {
        SILKWORM_LOG(LogLevel::Error) << "Unexpected unknown error in " << std::string(__FUNCTION__) << std::endl;
        return StageResult::kUnexpectedError;
    }
}

}  // namespace silkworm::stagedsync
