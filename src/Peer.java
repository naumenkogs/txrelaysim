package txrelaysim.src;

import txrelaysim.src.helpers.*;

import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Queue;
import java.util.Map;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Random;
import java.util.Collections;
import java.util.ListIterator;

import peersim.cdsim.CDProtocol;
import peersim.config.Configuration;
import peersim.config.FastConfig;
import peersim.core.Network;
import peersim.core.Node;
import peersim.core.CommonState;
import peersim.edsim.*;
import peersim.transport.Transport;


public class Peer implements CDProtocol, EDProtocol
{
	/* System */
	public static int pid = 2;

	/* Constants and delays. Reconciliation only! */
	public double inFloodLimitPercent;
	public double outFloodLimitPercent;
	public int reconciliationInterval;
	public int inRelayDelay;
	public int outRelayDelay;
	public double defaultQ;

	/* State */
	public HashSet<Node> outboundPeers;
	public HashSet<Node> inboundPeers;
	public HashMap<Integer, Long> txArrivalTimes;
	public HashMap<Node, HashSet<Integer>> peerKnowsTxs;
	// txid to scheduled time.
	public ArrayList<AnnouncementData> scheduledAnnouncements;
	public long nextFloodInbound = 0;
	public HashMap<Node, Long> nextFloodOutbound;

	/* Reconciliation state */
	public boolean reconcile = false;
	public Queue<Node> reconciliationQueue;
	public long nextRecon = 0;
	// This variable is used to check if a peer supports reconciliations.
	private HashMap<Node, HashSet<Integer>> reconSets;

	/* Stats */
	public int invsSent;
	public int shortInvsSent;
	public int txSent;

	public int successRecons;
	public int extSuccessRecons;
	public int failedRecons;

	public Peer(String prefix) {
		inboundPeers = new HashSet<Node>();
		outboundPeers = new HashSet<Node>();
		reconciliationQueue = new LinkedList<>();
		reconSets = new HashMap<>();
		peerKnowsTxs = new HashMap<>();
		txArrivalTimes = new HashMap<>();
		scheduledAnnouncements = new ArrayList<>();
		nextFloodOutbound = new HashMap<>();
	}

	class AnnouncementData
	{
		public long executionTime;
		public boolean shouldFanout;
		public Node recepient;
		public int txId;
		public AnnouncementData(int txId, long executionTime, boolean shouldFanout, Node recepient) {
			this.txId = txId;
			this.executionTime = executionTime;
			this.shouldFanout = shouldFanout;
			this.recepient = recepient;
		}
	};



	public Object clone() {
		return new Peer("");
	}

	@Override
	public void nextCycle(Node node, int pid) {
		long curTime = CommonState.getTime();
		if (reconcile && reconciliationQueue.peek() != null) {
			// If reconciliation is enabled on this node, it should periodically request reconciliations
			// with a queue of its reconciling peers.
			if (curTime > nextRecon) {
				Node recepient = reconciliationQueue.poll();

				SimpleMessage request = new SimpleMessage(SimpleEvent.RECON_REQUEST, node);
				((Transport)recepient.getProtocol(FastConfig.getTransport(Peer.pid))).send(node, recepient, request, Peer.pid);

				// Move this node to the end of the queue, schedule the next reconciliation.
				reconciliationQueue.offer(recepient);
				nextRecon = curTime + (reconciliationInterval / reconciliationQueue.size());
			}
		}

		ListIterator<AnnouncementData> iter = scheduledAnnouncements.listIterator();
		while(iter.hasNext()) {
			AnnouncementData entry = iter.next();
			if (entry.executionTime < curTime) {
				int txId = entry.txId;
				Node recepient = entry.recepient;
				if (peerKnowsTxs.get(recepient).contains(txId)) continue;
				peerKnowsTxs.get(recepient).add(txId);

				boolean fanout = entry.shouldFanout;

				// Peer reconciles
				if (reconcile && reconSets.containsKey(recepient)) {
					if (fanout) {
						removeFromReconSet(node, txId, recepient);
					} else {
						reconSets.get(recepient).add(txId);
					}
				}

				if (fanout) {
					IntMessage inv = new IntMessage(SimpleEvent.INV, node, txId);
					((Transport)recepient.getProtocol(FastConfig.getTransport(Peer.pid))).send(node, recepient, inv, Peer.pid);
					++invsSent;
				}
				iter.remove();
			}
		}
	}

	@Override
	public void processEvent(Node node, int pid, Object event) {
		SimpleEvent castedEvent = (SimpleEvent)event;
		switch (castedEvent.getType()) {
		case SimpleEvent.INV:
			// INV received from a peer.
			handleInvMessage(node, pid, (IntMessage)castedEvent);
			break;
		case SimpleEvent.RECON_REQUEST:
			// Reconciliation request from a peer.
			handleReconRequest(node, pid, (SimpleMessage)castedEvent);
			break;
		case SimpleEvent.SKETCH:
			// Sketch from a peer in response to reconciliation request.
			ArrayListMessage<?> ar = (ArrayListMessage<?>) castedEvent;
			ArrayList<Integer> remoteSet = new ArrayList<Integer>();
			for (Object x : ar.getArrayList()) {
				remoteSet.add((Integer) x);
			}
			handleSketchMessage(node, pid, ar.getSender(), remoteSet);
			break;
		case SimpleEvent.RECON_FINALIZATION:
			// We use this to track how many inv/shortinvs messages were sent for statas.
			handleReconFinalization(node, pid, (TupleMessage)castedEvent);
			break;
		// TODO: consider removing this for optimization.
		case SimpleEvent.GETDATA:
			// We use this just for bandwidth accounting, the actual txId (what we need) was already
			// commnunicated so nothing to do here.
			++txSent;
			break;
		}
	}

	// Handle a transaction announcement (INV) from a peer. Remember when the transaction was
	// announced, and set it for further relay to other peers.
	private void handleInvMessage(Node node, int pid, IntMessage message) {
		int txId = message.getInteger();
		Node sender = message.getSender();

		if (sender.getID() != 0) {
			// Came not from source.
			peerKnowsTxs.get(sender).add(txId);
			if (reconcile && reconSets.containsKey(sender)) {
				removeFromReconSet(node, txId, sender);
			}
		}

		if (!txArrivalTimes.keySet().contains(txId)) {
			SimpleMessage getdata = new SimpleMessage(SimpleEvent.GETDATA, node);
			((Transport)sender.getProtocol(FastConfig.getTransport(pid))).send(node, sender, getdata, Peer.pid);
			txArrivalTimes.put(txId, CommonState.getTime());

			prepareAnnouncement(node, pid, txId, sender);
		}
	}

	private void handleReconRequest(Node node, int pid, SimpleMessage message) {
		Node sender = message.getSender();
		HashSet<Integer> reconSet = reconSets.get(sender);
		ArrayListMessage<Integer> sketch = new ArrayListMessage<Integer>(SimpleEvent.SKETCH, node, new ArrayList<Integer>(reconSet));
		((Transport)sender.getProtocol(FastConfig.getTransport(Peer.pid))).send(node, sender, sketch, Peer.pid);
		peerKnowsTxs.get(sender).addAll(reconSet);
		reconSet.clear();
	}

	// Handle a sketch a peer sent us in response to our request. All sketch extension logic and
	// txId exchange is done here implicitly without actually sending messages, because a it can be
	// easily modeled and accounted at this node locally.
	private void handleSketchMessage(Node node, int pid, Node sender, ArrayList<Integer> remoteSet) {
		Set<Integer> localSet = reconSets.get(sender);
		int shared = 0, usMiss = 0, theyMiss = 0;
		// Handle transactions the local (sketch receiving) node doesn't have.
		for (Integer txId : remoteSet) {
			peerKnowsTxs.get(sender).add(txId);
			if (localSet.contains(txId)) {
				++shared;
			} else {
				++usMiss;
				if (!txArrivalTimes.keySet().contains(txId)) {
					SimpleMessage getdata = new SimpleMessage(SimpleEvent.GETDATA, node);
					((Transport)sender.getProtocol(FastConfig.getTransport(Peer.pid))).send(node, sender, getdata, Peer.pid);
					txArrivalTimes.put(txId, CommonState.getTime());
					prepareAnnouncement(node, pid, txId, sender);
				} else {
					// This is an edge case: sometimes a local set doesn't have a transaction
					// although we did receive/record it. It happens when we announce a transaction
					// to the peer and remove from the set while the peer sends us a sketch
					// including the same transaction.
				}
			}
		}

		// Handle transactions which the remote (sketch sending) node doesn't have.
		// TODO: batch-optimize
		for (Integer txId : localSet) {
			if (!remoteSet.contains(txId)) {
				theyMiss++;
				IntMessage inv = new IntMessage(SimpleEvent.INV, node, txId);
				((Transport)sender.getProtocol(FastConfig.getTransport(Peer.pid))).send(node, sender, inv, Peer.pid);
				++invsSent;
			}
		}

		// Compute the cost of this sketch exchange.
		int diff = usMiss + theyMiss;
		// This is a technicality of the simulator: in the finalization message we will notify
		// the node how much INV they supposedly sent us in this reconciliation round.
		int theySentInvs = 0, theySentShortInvs = 0;

		// Although diff estimation should happen at the sketch sender side, we do it here because
		// it works in our simplified model, to save extra messages.
		// To make it more detailed, we could remember the set size at request time here.
		int localSetSize = localSet.size();
		int remoteSetSize = remoteSet.size();
		// TODO: Q could be dynamicly updated after each reconciliation.
		int capacity = Math.abs(localSetSize - remoteSetSize) + (int)(defaultQ * Math.min(localSetSize, remoteSetSize)) + 1;
		if (capacity >= diff) {
			// Reconciliation succeeded right away.
			// TODO: where are their requests?
			// TODO: re-review this accounting.
			successRecons++;
			theySentShortInvs = capacity; // account for sketch
			shortInvsSent += usMiss; // RECONCILDIFF
			theySentInvs += usMiss; // after RECONCILDIFF
		} else if (capacity * 2 >= diff) {
			// Reconciliation succeeded after extension.
			extSuccessRecons++;
			theySentShortInvs = capacity * 2;  // account for sketch and extension
			shortInvsSent += usMiss;
			theySentInvs += usMiss;
		} else {
			// Reconciliation failed.
			failedRecons++;
			theySentShortInvs = capacity * 2;  // account for sketch and extension
			// Above, we already sent them invs they miss.
			// Here, we just account for all the remaining full invs: what we miss, and shared txs.
			// I think ideally the "inefficient" overlap between our set and their set should
			// be sent by us, hence the accounting below.
			invsSent += shared;
			theySentInvs = usMiss;
		}

		TupleMessage finalizationData = new TupleMessage(SimpleEvent.RECON_FINALIZATION, node, theySentInvs, theySentShortInvs);

		((Transport)sender.getProtocol(FastConfig.getTransport(Peer.pid))).send(
			node, sender, finalizationData, Peer.pid);

		localSet.clear();
	}

	private void handleReconFinalization(Node node, int pid, TupleMessage message) {
		invsSent += message.getX();
		shortInvsSent += message.getY();
	}

	private void prepareAnnouncement(Node node, int pid, int txId, Node sender) {
		long delay;
		long curTime = CommonState.getTime();

		Random random = new Random();
		for (Node peer : inboundPeers) {
			if (!reconSets.containsKey(peer)) { // non-reconciling (legacy) inbound peers
				assert(false);
			}

			if (nextFloodInbound < curTime) {
				nextFloodInbound = curTime + generateRandomDelay(this.inRelayDelay);
				delay = 0;
			} else {
				delay = nextFloodInbound - curTime;
			}
			// 10% chance
			boolean fanout = random.nextInt(100) < 10;
			scheduleAnnouncement(node, delay, peer, txId, fanout);
		}

		ArrayList<Node> outboundPeersCopy = new ArrayList<Node>(outboundPeers);
		Collections.shuffle(outboundPeersCopy) ;
		int fanouts = 1;
		for (Node peer : outboundPeersCopy) {
			if (!reconSets.containsKey(peer)) { // check for non-reconciling
				assert(false);
			}

			long nextFloodOutboundTime = nextFloodOutbound.get(peer);
			if (nextFloodOutboundTime < curTime) {
				delay = 0;
				nextFloodOutbound.put(peer, curTime + generateRandomDelay(this.outRelayDelay));
			} else {
				delay = nextFloodOutboundTime - curTime;
			}

			if (fanouts > 0) {
				scheduleAnnouncement(node, delay, peer, txId, true);
				fanouts--;
			} else {
				scheduleAnnouncement(node, delay, peer, txId, false);
			}
		}
	}

	private void removeFromReconSet(Node node, int txId, Node target) {
		if (reconSets.get(target).contains(txId)) {
			reconSets.get(target).remove(txId);
		}
	}

	// We don't announce transactions right away, because usually the delay takes place to make it
	// more private.
	private void scheduleAnnouncement(Node node, long delay, Node recepient, int txId, boolean shouldFanout) {
		assert(recepient.getID() != 0);

		if (peerKnowsTxs.get(recepient).contains(txId)) {
			return;
		}
		scheduledAnnouncements.add(new AnnouncementData(txId, delay, shouldFanout, recepient));
	}

	// A helper for scheduling events which happen after a random delay.
	private long generateRandomDelay(long avgDelay) {
		return CommonState.r.nextLong(avgDelay * 2 + 1);
	}

	// Used for setting up the topology.
	public void addPeer(Node peer, boolean outbound) {
		boolean peerSupportsRecon = ((Peer)peer.getProtocol(Peer.pid)).reconcile;
		boolean added = outbound ? outboundPeers.add(peer) : inboundPeers.add(peer);
		assert(added);
		peerKnowsTxs.put(peer, new HashSet<>());
		if (reconcile && peerSupportsRecon) {
			if (outbound) { reconciliationQueue.offer(peer); }
			reconSets.put(peer, new HashSet<>());
		}
		if (outbound) nextFloodOutbound.put(peer, 0L);
	}
}