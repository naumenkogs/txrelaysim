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

class Stats {
	public int invs;
	public int shortInvs;
	public int sketchItems;

	public int successRecons;
	public int failedRecons;

	public int duplicateAnno;
	public int freshAnno;
}

class FanoutDestinations {
	public double in;
	public int out;

	public FanoutDestinations() {
		// defaults for legacy
		this.in = 1;
		this.out = 10000;
	}

	public FanoutDestinations(double in, int out) {
		this.in = in;
		this.out = out;
	}
}

class DelayedTxData {
	public Node owner;
	public Long requestTime;

	public DelayedTxData(Node owner, Long requestTime) {
		this.owner = owner;
		this.requestTime = requestTime;
	}
}

public class Peer implements CDProtocol, EDProtocol
{
	/* System */
	public static int pid = 2;

	public static int reconciliationInterval; // between touching the queue
	public static double q;
	public static int c;

	public Delays delays;

	/* State */
	public HashSet<Node> outboundPeers;
	public HashSet<Node> inboundPeers;
	public HashMap<Integer, Long> txArrivalTimes;
	// For inbounds, we delay requesting a transaction (GETDATA), so that we're not stuck asking a malicious inbound for it.
	// In this simulation, this works through arrival time accounting.
	// Maps txid to <node, time>.
	public HashMap<Integer, DelayedTxData> txDelayedRequest;

	public HashMap<Node, HashSet<Integer>> peerKnowsTxs;
	// How many times a tx was announced to either to us or by us.
	// Stop fanning out if reached 4.
	public HashMap<Integer, Integer> txAnnouncedTimes;

	public FanoutDestinations fanoutDestinations;

	public ArrayList<AnnouncementData> scheduledAnnouncements;

	public long nextFloodInbound = 0;
	public HashMap<Node, Long> nextFloodOutbound;

	/* Reconciliation state */
	public boolean reconcile = false;
	public Queue<Node> reconciliationQueue;
	public long nextRecon = 0;
	private HashMap<Node, HashSet<Integer>> reconSets;

	// If a peer hits 8 poissons, it's time to initiate reconciliations and reset the count.
	private HashMap<Node, Integer> reconTimes;
	private HashSet<Node> awaitingSketch;
	private boolean respondWithSketches = false;

	private HashMap<Node, Integer> localSetSizeWhenInitiated;

	private HashSet<Integer> txReconciledByInitiator;

	/* Stats */
	public Stats stats;

	public Peer(String prefix) {
		inboundPeers = new HashSet<Node>();
		outboundPeers = new HashSet<Node>();
		reconciliationQueue = new LinkedList<>();
		reconSets = new HashMap<>();
		peerKnowsTxs = new HashMap<>();
		txArrivalTimes = new HashMap<>();
		scheduledAnnouncements = new ArrayList<>();
		nextFloodOutbound = new HashMap<>();
		stats = new Stats();
		fanoutDestinations = new FanoutDestinations();
		txDelayedRequest = new HashMap<>();
		txAnnouncedTimes = new HashMap<>();
		reconTimes = new HashMap<>();
		awaitingSketch = new HashSet<>();
		localSetSizeWhenInitiated = new HashMap<>();
		txReconciledByInitiator = new HashSet<>();
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

		// Consider initiating reconciliations with outbounds only
		for (Map.Entry<Node, Integer> reconCandidate : reconTimes.entrySet()) {
			Node candidate = reconCandidate.getKey();
			assert(!inboundPeers.contains(candidate));
			if (reconCandidate.getValue() >= 8) {
				reconCandidate.setValue(0);
				SimpleMessage request = new SimpleMessage(SimpleEvent.RECON_REQUEST, node);
				((Transport)candidate.getProtocol(FastConfig.getTransport(Peer.pid))).send(node, candidate, request, Peer.pid);
				localSetSizeWhenInitiated.put(candidate, reconSets.get(candidate).size());
			}
		}

		// Consider responding to reconciliations with inbounds only
		if (respondWithSketches) {
			for (Node peer : awaitingSketch) {
				HashSet<Integer> reconSet = reconSets.get(peer);
				ArrayListMessage sketch = new ArrayListMessage(SimpleEvent.SKETCH, node, new ArrayList<Integer>(reconSet));
				((Transport)peer.getProtocol(FastConfig.getTransport(Peer.pid))).send(node, peer, sketch, Peer.pid);
				reconSet.clear();
			}
			awaitingSketch.clear();
			respondWithSketches = false;
		}

		// TODO optimization: Sort this by executionTime not to go through entire list every time.
		// TODO optimziation: since delay is the same for all inbounds, we can store just one entry for them all. All we need is delay.
		ListIterator<AnnouncementData> iter = scheduledAnnouncements.listIterator();
		Random random = new Random();
		while(iter.hasNext()) {
			AnnouncementData entry = iter.next();
			if (entry.executionTime < curTime) {
				iter.remove();
				int txId = entry.txId;
				Node recepient = entry.recepient;
				// We could have received it between scheduling and executing.
				if (peerKnowsTxs.get(recepient).contains(txId)) continue;

				boolean fanout = entry.shouldFanout;

				if (inboundPeers.contains(recepient)) {
					fanout = random.nextInt(100) < (100 * fanoutDestinations.in);
				} else {
					int announcedTimes = txAnnouncedTimes.get(txId);
					if (announcedTimes < fanoutDestinations.out) {
						fanout = true;
						txAnnouncedTimes.put(txId, announcedTimes + 1);
					} else {
						fanout = false;
					}
				}

				if (fanout) {
					announceTx(node, txId, recepient);
					reconSets.get(recepient).remove(txId);
				}
			}
		}

		Iterator<Map.Entry<Integer, DelayedTxData>> delayexTxIt = txDelayedRequest.entrySet().iterator();
		while (delayexTxIt.hasNext()) {
			Map.Entry<Integer, DelayedTxData> delayedTx = (Map.Entry<Integer, DelayedTxData>)delayexTxIt.next();
			if (delayedTx.getValue().requestTime < curTime) {
				receiveTx(node, delayedTx.getKey(), delayedTx.getValue().owner);
				delayexTxIt.remove();
			}
		}
	}

	@Override
	public void processEvent(Node node, int pid, Object event) {
		SimpleEvent castedEvent = (SimpleEvent)event;
		switch (castedEvent.getType()) {
		case SimpleEvent.INV:
			handleInvMessage(node, (IntMessage)castedEvent);
			break;
		case SimpleEvent.RECON_REQUEST:
			handleReconRequest(node, (SimpleMessage)castedEvent);
			break;
		case SimpleEvent.SKETCH:
			// Sketch from a peer in response to reconciliation request.
			ArrayListMessage ar = (ArrayListMessage) castedEvent;
			handleSketchMessage(node, ar.getSender(), ar.getArrayList());
			break;
		}
	}

	// Handle a transaction announcement (INV) from a peer. Remember when the transaction was
	// announced, and set it for further relay to other peers.
	private void handleInvMessage(Node node, IntMessage message) {
		int txId = message.getInteger();
		Node sender = message.getSender();

		if (sender.getID() != 0 && reconcile) {
			removeFromReconSet(node, txId, sender);
		}

		receiveAnnoucement(node, txId, sender);
	}

	private void receiveAnnoucement(Node node, int txId, Node sender) {
		// Consider for requesting only if we hasn't got the tx body yet.
		if (!txArrivalTimes.keySet().contains(txId)) {
			// If came from outbound, store right away.
			// If came from inbound, delay requesting the tx, so that we have a chance to fetch it from outbounds (safer).
			if (inboundPeers.contains(sender)) {
				if (!txDelayedRequest.keySet().contains(txId)) {
					txDelayedRequest.put(txId, new DelayedTxData(sender, CommonState.getTime() + 2000));
				}
			} else {
				receiveTx(node, txId, sender);
				txDelayedRequest.remove(txId);
			}
			++stats.freshAnno;
			txAnnouncedTimes.put(txId, 1);
		} else {
			++stats.duplicateAnno;
			txAnnouncedTimes.put(txId, txAnnouncedTimes.get(txId) + 1);
		}

		++stats.invs;

		if (sender.getID() != 0) {
			peerKnowsTxs.get(sender).add(txId);
		}
	}


	private void receiveTx(Node node, int txId, Node sender) {
		txArrivalTimes.put(txId, CommonState.getTime());
		prepareAnnouncement(node, txId, sender);
	}

	private void handleReconRequest(Node node, SimpleMessage message) {
		Node sender = message.getSender();
		awaitingSketch.add(sender);
	}

	// Handle a sketch a peer sent us in response to our request. All sketch extension logic and
	// txId exchange is done here implicitly without actually sending messages, because a it can be
	// easily modeled and accounted at this node locally.
	private void handleSketchMessage(Node node, Node sender, ArrayList<Integer> remoteSet) {
		Set<Integer> localSet = reconSets.get(sender);
		// Although diff estimation should happen at the sketch sender side, we do it here because
		// it works in our simplified model, to save extra messages.
		int localSetSize = localSetSizeWhenInitiated.get(sender);
		int remoteSetSize = remoteSet.size();
		int capacity = Math.abs(localSetSize - remoteSetSize) + (int)(q * Math.min(localSetSize, remoteSetSize)) + c;

		int shared = 0, usMiss = 0, theyMiss = 0;
		// Handle transactions the local (sketch receiving) node doesn't have.
		for (Integer txId : remoteSet) {
			if (localSet.contains(txId)) {
				++shared;
				peerKnowsTxs.get(sender).add(txId);
			} else {
				++usMiss;
				receiveAnnoucement(node, txId, sender);
				txReconciledByInitiator.add(txId);
			}
		}

		// Handle transactions which the remote (sketch sending) node doesn't have.
		// TODO: batch-optimize
		for (Integer txId : localSet) {
			if (!remoteSet.contains(txId)) {
				theyMiss++;
				assert(!peerKnowsTxs.get(sender).contains(txId));
				// Possibly we heard it from them, but didn't add to the set yet (delay).
				announceTx(node, txId, sender);
			}
		}

		// Compute the cost of this sketch exchange.
		int diff = usMiss + theyMiss;

		stats.sketchItems += capacity;
		// All INVs we'd announce to them (theyMiss) are accounted above (announceTx).
		if (capacity >= diff) {
			stats.successRecons++;
			stats.shortInvs += usMiss;
		} else {
			stats.failedRecons++;
			stats.invs += usMiss;
			stats.invs += shared;
		}

		localSet.clear();
	}

	private void prepareAnnouncement(Node node, int txId, Node sender) {
		long delay;
		long curTime = CommonState.getTime();

		Random random = new Random();
		for (Node peer : inboundPeers) {
			if (peer == sender) continue;
			if (nextFloodInbound < curTime) {
				nextFloodInbound = curTime + generateRandomDelay(this.delays.in);
				respondWithSketches = true;
			}
			boolean fanout = random.nextInt(100) < (100 * fanoutDestinations.in);
			scheduleAnnouncement(node, nextFloodInbound, peer, txId, fanout);
		}

		ArrayList<Node> outboundPeersCopy = new ArrayList<Node>(outboundPeers);
		Collections.shuffle(outboundPeersCopy);
		int fanouts = 1;
		for (Node peer : outboundPeersCopy) {
			long nextFloodOutboundTime = nextFloodOutbound.get(peer);
			if (nextFloodOutboundTime < curTime) {
				nextFloodOutboundTime = curTime + generateRandomDelay(this.delays.out);
				nextFloodOutbound.put(peer, nextFloodOutboundTime);
				reconTimes.put(peer, reconTimes.get(peer) + 1);
			}
			if (peer == sender) continue;
			scheduleAnnouncement(node, nextFloodOutboundTime, peer, txId, fanouts-- > 0);
		}
	}

	private void removeFromReconSet(Node node, int txId, Node target) {
		// todo optimize?
		if (reconSets.containsKey(target) && reconSets.get(target).contains(txId)) {
			reconSets.get(target).remove(txId);
		}
	}

	private void scheduleAnnouncement(Node node, long executionTime, Node recepient, int txId, boolean shouldFanout) {
		assert(recepient.getID() != 0);

		if (peerKnowsTxs.get(recepient).contains(txId)) {
			return;
		}
		scheduledAnnouncements.add(new AnnouncementData(txId, executionTime, shouldFanout, recepient));

		if (reconcile && reconSets.containsKey(recepient)) {
			reconSets.get(recepient).add(txId);
		}
	}

	private void announceTx(Node node, int txId, Node recepient) {
		IntMessage inv = new IntMessage(SimpleEvent.INV, node, txId);
		((Transport)recepient.getProtocol(FastConfig.getTransport(Peer.pid))).send(node, recepient, inv, Peer.pid);
		peerKnowsTxs.get(recepient).add(txId);
		
	}

	// A helper for scheduling events which happen after a random delay.
	private long generateRandomDelay(long avgDelay) {
		return CommonState.r.nextPoisson(avgDelay / 1000) * 1000;
	}

	// Used for setting up the topology.
	public void addPeer(Node peer, boolean outbound) {
		boolean peerSupportsRecon = ((Peer)peer.getProtocol(Peer.pid)).reconcile;
		boolean added = outbound ? outboundPeers.add(peer) : inboundPeers.add(peer);
		assert(added);
		peerKnowsTxs.put(peer, new HashSet<>());
		if (reconcile && peerSupportsRecon) {
			if (outbound) { reconciliationQueue.offer(peer); }
			if (outbound) reconTimes.put(peer, 0);
			reconSets.put(peer, new HashSet<>());
		}
		if (outbound) nextFloodOutbound.put(peer, 0L);
	}
}
