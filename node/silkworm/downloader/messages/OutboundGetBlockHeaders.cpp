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

#include "OutboundGetBlockHeaders.hpp"

#include <sstream>

#include <silkworm/common/log.hpp>
#include <silkworm/downloader/header_downloader.hpp>
#include <silkworm/downloader/packets/RLPEth66PacketCoding.hpp>
#include <silkworm/downloader/rpc/SendMessageByMinBlock.hpp>
#include <silkworm/downloader/rpc/PenalizePeer.hpp>

namespace silkworm {

OutboundGetBlockHeaders::OutboundGetBlockHeaders(WorkingChain& wc, SentryClient& s): working_chain_(wc), sentry_(s) {}

/*
// HeadersForward progresses Headers stage in the forward direction
func HeadersForward(
	s *StageState,
	u Unwinder,
	ctx context.Context,
	tx ethdb.RwTx,
	cfg HeadersCfg,
	initialCycle bool,
	test bool, // Set to true in tests, allows the stage to fail rather than wait indefinitely
) error {
	var headerProgress uint64
	var err error
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	if err = cfg.hd.ReadProgressFromDb(tx); err != nil {
		return err
	}
	cfg.hd.SetFetching(true)
	defer cfg.hd.SetFetching(false)
	headerProgress = cfg.hd.Progress()
	logPrefix := s.LogPrefix()
	// Check if this is called straight after the unwinds, which means we need to create new canonical markings
	hash, err := rawdb.ReadCanonicalHash(tx, headerProgress)
	if err != nil {
		return err
	}
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	if hash == (common.Hash{}) {
		headHash := rawdb.ReadHeadHeaderHash(tx)
		if err = fixCanonicalChain(logPrefix, logEvery, headerProgress, headHash, tx); err != nil {
			return err
		}
		if !useExternalTx {
			if err = tx.Commit(); err != nil {
				return err
			}
		}
		s.Done()
		return nil
	}

	log.Info(fmt.Sprintf("[%s] Waiting for headers...", logPrefix), "from", headerProgress)

	localTd, err := rawdb.ReadTd(tx, hash, headerProgress)
	if err != nil {
		return err
	}
	headerInserter := headerdownload.NewHeaderInserter(logPrefix, localTd, headerProgress)
	cfg.hd.SetHeaderReader(&chainReader{config: &cfg.chainConfig, tx: tx})

	var peer []byte
	stopped := false
	prevProgress := headerProgress
	for !stopped {
		currentTime := uint64(time.Now().Unix())
		req, penalties := cfg.hd.RequestMoreHeaders(currentTime)
		if req != nil {
			peer = cfg.headerReqSend(ctx, req)
			if peer != nil {
				cfg.hd.SentRequest(req, currentTime, 5 ) // 5 = timeout
				log.Debug("Sent request", "height", req.Number)
			}
		}
		cfg.penalize(ctx, penalties)
		maxRequests := 64 // Limit number of requests sent per round to let some headers to be inserted into the database
		for req != nil && peer != nil && maxRequests > 0 {
			req, penalties = cfg.hd.RequestMoreHeaders(currentTime)
			if req != nil {
				peer = cfg.headerReqSend(ctx, req)
				if peer != nil {
					cfg.hd.SentRequest(req, currentTime, 5 ) // 5 = timeout
					log.Debug("Sent request", "height", req.Number)
				}
			}
			cfg.penalize(ctx, penalties)
			maxRequests--
		}

		// Send skeleton request if required
		req = cfg.hd.RequestSkeleton()
		if req != nil {
			peer = cfg.headerReqSend(ctx, req)
			if peer != nil {
				log.Debug("Sent skeleton request", "height", req.Number)
			}
		}
		// Load headers into the database
		var inSync bool
		if inSync, err = cfg.hd.InsertHeaders(headerInserter.FeedHeaderFunc(tx), logPrefix, logEvery.C); err != nil {
			return err
		}
		announces := cfg.hd.GrabAnnounces()
		if len(announces) > 0 {
			cfg.announceNewHashes(ctx, announces)
		}
		if headerInserter.BestHeaderChanged() { // We do not break unless there best header changed
			if !initialCycle {
				// if this is not an initial cycle, we need to react quickly when new headers are coming in
				break
			}
			// if this is initial cycle, we want to make sure we insert all known headers (inSync)
			if inSync {
				break
			}
		}
		if test {
			break
		}
		timer := time.NewTimer(1 * time.Second)
		select {
		case <-ctx.Done():
			stopped = true
		case <-logEvery.C:
			progress := cfg.hd.Progress()
			logProgressHeaders(logPrefix, prevProgress, progress)
			prevProgress = progress
		case <-timer.C:
			log.Trace("RequestQueueTime (header) ticked")
		case <-cfg.hd.DeliveryNotify:
			log.Debug("headerLoop woken up by the incoming request")
		}
		timer.Stop()
	}
	if headerInserter.Unwind() {
		if err := u.UnwindTo(headerInserter.UnwindPoint(), tx, common.Hash{}); err != nil {
			return fmt.Errorf("%s: failed to unwind to %d: %w", logPrefix, headerInserter.UnwindPoint(), err)
		}
	} else if headerInserter.GetHighest() != 0 {
		if err := fixCanonicalChain(logPrefix, logEvery, headerInserter.GetHighest(), headerInserter.GetHighestHash(), tx); err != nil {
			return fmt.Errorf("%s: failed to fix canonical chain: %w", logPrefix, err)
		}
	}
	s.Done()
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	if stopped {
		return common.ErrStopped
	}
	// We do not print the followin line if the stage was interrupted
	log.Info(fmt.Sprintf("[%s] Processed", logPrefix), "highest inserted", headerInserter.GetHighest(), "age", common.PrettyAge(time.Unix(int64(headerInserter.GetHighestTimestamp()), 0)))
	stageHeadersGauge.Update(int64(cfg.hd.Progress()))
	return nil
}
*/

void OutboundGetBlockHeaders::execute() {
    using namespace std::literals::chrono_literals;

    time_point_t now = std::chrono::system_clock::now();
    seconds_t timeout = 5s;
    int max_requests = 64; // limit number of requests sent per round to let some headers to be inserted into the database

    // anchor extension
    do {
        auto [packet, penalizations] = working_chain_.request_more_headers(now);

        if (packet == std::nullopt)
            break;

        auto send_outcome = send_packet(*packet, timeout);

        SILKWORM_LOG(LogLevel::Info) << "Headers request sent, received by " << send_outcome.peers_size() << " peer(s)\n";

        if (send_outcome.peers_size() == 0)
            break;

        working_chain_.request_ack(*packet, now, timeout);

        for (auto& penalization : penalizations) {
            send_penalization(penalization, 1s);
        }

        max_requests--;
    } while(max_requests > 0); // && packet != std::nullopt && receiving_peers != nullptr

    // anchor collection
    auto packet = working_chain_.request_skeleton();

    if (packet != std::nullopt) {
        auto send_outcome = send_packet(*packet, timeout);

        SILKWORM_LOG(LogLevel::Info) << "Headers skeleton request sent, received by " << send_outcome.peers_size() << " peer(s)\n";
    }

    // todo: complete implementations looking at Erigon HeadersForward
}

sentry::SentPeers OutboundGetBlockHeaders::send_packet(const GetBlockHeadersPacket66& packet_, seconds_t timeout) {
    //packet_ = packet;

    if (std::holds_alternative<Hash>(packet_.request.origin))
        throw std::logic_error("OutboundGetBlockHeaders expects block number not hash");    // todo: check!

    BlockNum min_block = std::get<BlockNum>(packet_.request.origin); // choose target peer
    if (!packet_.request.reverse)
        min_block += packet_.request.amount * packet_.request.skip;

    auto msg_reply = std::make_unique<sentry::OutboundMessageData>(); // create header request

    msg_reply->set_id(sentry::MessageId::GET_BLOCK_HEADERS_66);

    Bytes rlp_encoding;
    rlp::encode(rlp_encoding, packet_);
    msg_reply->set_data(rlp_encoding.data(), rlp_encoding.length()); // copy

    SILKWORM_LOG(LogLevel::Info) << "Requesting " << packet_ << " with send_message_by_min_block\n";
    rpc::SendMessageByMinBlock rpc{min_block, std::move(msg_reply)};

    rpc.timeout(timeout);

    sentry_.exec_remotely(rpc);

    sentry::SentPeers peers = rpc.reply();
    SILKWORM_LOG(LogLevel::Info) << "Received rpc result of " << packet_ << ": " << std::to_string(peers.peers_size()) + " peer(s)\n";

    return peers;
}

void OutboundGetBlockHeaders::send_penalization(const PeerPenalization& penalization, seconds_t timeout) {
    rpc::PenalizePeer rpc{penalization.peerId, penalization.penalty};

    rpc.timeout(timeout);

    sentry_.exec_remotely(rpc);
}


}