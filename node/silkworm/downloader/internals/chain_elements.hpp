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

#ifndef SILKWORM_CHAIN_ELEMENTS_HPP
#define SILKWORM_CHAIN_ELEMENTS_HPP

#include <map>
#include <queue>
#include <set>
#include <vector>

#include "db_tx.hpp"
#include "types.hpp"
#include "priority_queue.hpp"

namespace silkworm {

// Auxiliary types needed to implement WorkingChain

// A link corresponds to a block header, links are connected to each other by reverse of parentHash relation
struct Link {
    std::shared_ptr<BlockHeader> header;        // Header to which this link point to
    BlockNum blockHeight = 0;                   // Block height of the header, repeated here for convenience (remove?)
    Hash hash;                                  // Hash of the header
    std::vector<std::shared_ptr<Link>> next;    // Reverse of parentHash / Allows iteration over links in ascending block height order
    bool persisted = false;                     // Whether this link comes from the database record
    bool preverified = false;                   // Ancestor of pre-verified header
    //int idx = 0;                                // Index in the heap for easy of removal (used by Go binary heap impl, remove?)

    Link(BlockHeader h, bool persisted_) {
        blockHeight = h.number;
        hash = h.hash(); // save computation
        header = std::make_shared<BlockHeader>(std::move(h));
        persisted = persisted_;
    }

    void remove_child(std::shared_ptr<Link> child) {
        auto to_remove = std::remove_if(next.begin(), next.end(), [child](auto& link) {return (link->hash == child->hash);});
        next.erase(to_remove, next.end());
    }
};

// An anchor is the bottom of a chain bundle that consists of one anchor and some chain links.
struct Anchor {
    Hash parentHash;                            // Hash of the header this anchor can be connected to (to disappear)
    BlockNum blockHeight;                       // block height of the anchor
    time_point_t timestamp;                     // Zero when anchor has just been created, otherwise timestamps when timeout on this anchor request expires
    int timeouts = 0;                           // Number of timeout that this anchor has experiences - after certain threshold, it gets invalidated
    std::vector<std::shared_ptr<Link>> links;   // Links attached immediately to this anchor
    PeerId peerId;

    Anchor(const BlockHeader& header, PeerId p) {
        parentHash = header.parent_hash;
        blockHeight = header.number;
        //timestamp = 0; automatically set to unix epoch by the constructor
        peerId = std::move(p);
    }

    void remove_child(std::shared_ptr<Link> child) {
        auto to_remove = std::remove_if(links.begin(), links.end(), [child](auto& link) {return (link->hash == child->hash);});
        links.erase(to_remove, links.end());
    }
};

// Binary relations to use in priority queues
struct Link_Older_Than: public std::binary_function<std::shared_ptr<Link>, std::shared_ptr<Link>, bool>
{
    bool operator()(const std::shared_ptr<Link>& x, const std::shared_ptr<Link>& y) const
    { return x->blockHeight < y->blockHeight; }
};

struct Link_Younger_Than: public std::binary_function<std::shared_ptr<Link>, std::shared_ptr<Link>, bool>
{
    bool operator()(const std::shared_ptr<Link>& x, const std::shared_ptr<Link>& y) const
    { return x->blockHeight > y->blockHeight; }
};

struct Anchor_Older_Than: public std::binary_function<std::shared_ptr<Anchor>, std::shared_ptr<Anchor>, bool>
{
    bool operator()(const std::shared_ptr<Anchor>& x, const std::shared_ptr<Anchor>& y) const
    {
        return x->timestamp != y->timestamp ? x->timestamp < y->timestamp :
                                              x->blockHeight < y->blockHeight;  // todo: check! Erigon do the same but in the comment the idea is the opposite
    }
};

// Priority queue types - todo: check boost::multi-index-container to tie queue & map together

using Oldest_First_Link_Queue  = std::priority_queue<std::shared_ptr<Link>,
                                                     std::vector<std::shared_ptr<Link>>,
                                                     Link_Older_Than>;

// For the Youngest_First_Link_Queue, Erigon use an intrusive priority_queue with elements storing index in the queue
// for low removal time. We can:
// 1. use std priority_queue, implementing erase with find (->custom_priority_queue)
// 2. use std set (->Set_Based_Priority_Queue)
// 3. implement an intrusive priority_queue as Erigon
// 4. use a multi-index container
/*
using Youngest_First_Link_Queue = std::priority_queue<std::shared_ptr<Link>,
                                                      std::vector<std::shared_ptr<Link>>,
                                                      Link_Younger_Than>;
*/
using Youngest_First_Link_Queue = Set_Based_Priority_Queue<std::shared_ptr<Link>,Link_Younger_Than>;
// todo: verify if Set_Based_Priority_Queue has comparable performance with Erigon intrusive priority queue

using Oldest_First_Anchor_Queue = custom_priority_queue<std::shared_ptr<Anchor>,
                                                        std::vector<std::shared_ptr<Anchor>>,
                                                        Anchor_Older_Than>;
// todo: anchorQueue and anchorMap should be encapsulated because if one change an anchor anchorQueue must be re-sorted
// todo: find a better alternative of custom_priority_queue (we use the custom one because Oldest_First_Link_Queue need a resort when an anchor change externally)

// Maps
using Link_Map = std::map<Hash,std::shared_ptr<Link>>;     // hash = link hash
using Anchor_Map = std::map<Hash,std::shared_ptr<Anchor>>; // hash = anchor *parent* hash
// todo: Anchor_Map key = anchor *parent* hash, incapsulate this kwnowledge in a class
// so we can write anchor_map.add(anchor) in place of anchor_map[anchor->parent_hash] = anchor

// Lists
using Link_List = std::vector<std::shared_ptr<Link>>;

struct Segment; // forward declaration

// A list of (possibly unrelated) headers
// It arrives from remote peers, it is divided in Segments, and each Segment will be processed alone; each Segment has
// only references to the headers in the list so for safety reason each Segment has also a shared_ptr to the HeaderList
// so one instance of HeaderList remains alive whenever there is at least one Segment that uses it
struct HeaderList: std::enable_shared_from_this<HeaderList> {
public:
    using Header_Ref = std::vector<BlockHeader>::const_iterator;

    static std::shared_ptr<HeaderList> make(const std::vector<BlockHeader>& headers) {
        return std::shared_ptr<HeaderList>(new HeaderList(headers));
    }

    auto split_into_segments() -> std::tuple<std::vector<Segment>, Penalty>; // the core functionality of HeaderList

    std::vector<BlockHeader>& headers() {return headers_;}

private:
    HeaderList(std::vector<BlockHeader> headers): headers_(std::move(headers)) {}   // private, it needs to stay in the heap,
                                                                                    // use make method to get an instance
    std::vector<BlockHeader> headers_;

    std::vector<Header_Ref> to_ref();

    std::tuple<bool,Penalty> static childParentValidity(Header_Ref child, Header_Ref parent);

    std::tuple<bool,Penalty> static childrenParentValidity(const std::vector<Header_Ref>& children, Header_Ref parent);
};


// Segment, a sequence of headers connected to one another (with parent-hash relationship),
// without any branching, ordered from high block number to lower block number
struct Segment:
        public std::vector<HeaderList::Header_Ref> { // pointers/iterators to the headers that belongs to this segment

    Segment(std::shared_ptr<HeaderList> line): line_(line) {}

    void push_back (const HeaderList::Header_Ref& val) {
        assert(empty() || back()->number == val->number + 1); // also back()->parent_hash == val->hash() (expensive test)
        std::vector<HeaderList::Header_Ref>::push_back(val);
    }

    [[nodiscard]] HeaderList::Header_Ref lowest_header() const {return back();}

    using Slice = gsl::span<const HeaderList::Header_Ref>; // a Segment slice

    [[nodiscard]] Slice slice(size_t start, size_t end) const { return Slice(*this).subspan(start, end); } // with c++20 it can be implemented as: return Slice(begin() + start, begin() + end);

protected:
    //std::vector<something> headersRaw; // todo: do we need this?
    std::shared_ptr<HeaderList> line_; // all the headers
};

}

#endif  // SILKWORM_CHAIN_ELEMENTS_HPP