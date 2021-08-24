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

#include <iostream>
#include <random>

#include <silkworm/common/stopwatch.hpp>
#include <silkworm/etl/buffer.hpp>

using namespace silkworm;

std::random_device rd;
std::default_random_engine engine{rd()};

Bytes random_bytes() {
    std::uniform_int_distribution<size_t> ud1{0, 254};  // For Bytes selector
    std::uniform_int_distribution<size_t> ud2{5, 512};  // For ByteLen selector
    Bytes ret(ud2(engine), '\0');
    for (size_t i = 0; i < ret.length(); ++i) {
        ret[i] = static_cast<uint8_t>(ud1(engine));
    }
    return ret;
}

int main(int argc, char* argv[]) {
    (void)argc;
    (void)argv;

    StopWatch sw;
    size_t kDataSetSize{1_Gibi};
    etl::Buffer base_buffer(kDataSetSize);

    // Feed base buffer with a random set
    std::cout << "\n Feeding buffer base ..." << std::endl;
    sw.start();
    while (!base_buffer.overflows()) {
        etl::Entry item{random_bytes(), random_bytes()};
        base_buffer.put(item);
    }
    auto feed_time = sw.lap();
    sw.reset();
    std::cout << " Done in " << sw.format(feed_time.second) << std::endl;

    // Push all items in buffer default
    {
        etl::Buffer buffer(kDataSetSize);
        std::cout << "\n [Buffer-Default] First loop ..." << std::endl;
        sw.start();
        for (const auto& item : base_buffer.entries()) {
            buffer.put(item);
        }
        buffer.sort();
        auto loop1_timings{sw.lap()};
        sw.reset();
        std::cout << " [Buffer-Default] Done in " << sw.format(loop1_timings.second);

        buffer.clear();
        std::cout << "\n [Buffer-Default] Second loop ..." << std::endl;
        sw.start();
        for (const auto& item : base_buffer.entries()) {
            buffer.put(item);
        }
        buffer.sort();
        auto loop2_timings{sw.lap()};
        sw.reset();
        std::cout << " [Buffer-Default] Done in " << sw.format(loop2_timings.second) << "\n" << std::endl;
    }

    // Push all items in buffer Gregory
    {
        etl::Buffer2 buffer(kDataSetSize);
        std::cout << "\n [Buffer-Gregory] First loop ..." << std::endl;
        sw.start();
        for (const auto& item : base_buffer.entries()) {
            buffer.put(item);
        }
        buffer.sort();
        auto loop1_timings{sw.lap()};
        sw.reset();
        std::cout << " [Buffer-Gregory] Done in " << sw.format(loop1_timings.second);

        buffer.clear();
        std::cout << "\n [Buffer-Gregory] Second loop ..." << std::endl;
        sw.start();
        for (const auto& item : base_buffer.entries()) {
            buffer.put(item);
        }
        buffer.sort();
        auto loop2_timings{sw.lap()};
        sw.reset();
        std::cout << " [Buffer-Gregory] Done in " << sw.format(loop2_timings.second) << "\n" << std::endl;
    }

}