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

	public FanoutDestinations fanoutDestinations;

	public ArrayList<AnnouncementData> scheduledAnnouncements;

	public long nextFloodInbound = 0;
	public HashMap<Node, Long> nextFloodOutbound;

	/* Reconciliation state */
	public boolean reconcile = false;
	public Queue<Node> reconciliationQueue;
	public long nextRecon = 0;
	private HashMap<Node, HashSet<Integer>> reconSets;

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
				nextRecon = curTime + reconciliationInterval;
			}
		}

		// TODO optimization: Sort this by executionTime not to go through entire list every time.
		// TODO optimziation: since delay is the same for all inbounds, we can store just one entry for them all. All we need is delay.
		ListIterator<AnnouncementData> iter = scheduledAnnouncements.listIterator();
		while(iter.hasNext()) {
			AnnouncementData entry = iter.next();
			if (entry.executionTime < curTime) {
				iter.remove();
				int txId = entry.txId;
				Node recepient = entry.recepient;
				// We could have received it between scheduling and executing.
				if (peerKnowsTxs.get(recepient).contains(txId)) continue;

				peerKnowsTxs.get(recepient).add(txId);

				// TODO: should this be decided on scheduling or right-before-announcing?
				boolean fanout = entry.shouldFanout;
				// Peer reconciles
				if (reconcile && reconSets.containsKey(recepient)) {
					if (!fanout) {
						reconSets.get(recepient).add(txId);
					}
				}

				if (fanout) {
					announceTx(node, txId, recepient);
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

		if (sender.getID() != 0) {
			// Came not from source.
			peerKnowsTxs.get(sender).add(txId);
			if (reconcile) {
				removeFromReconSet(node, txId, sender);
			}
		}

		receiveAnnoucement(node, txId, sender);
	}

	private void receiveAnnoucement(Node node, int txId, Node sender) {
		if (txArrivalTimes.keySet().contains(txId)) {
			return;
		}

		// If came from outbound, store right away.
		// If came from inbound, delay requesting the tx, so that we have a chance to fetch it from outbounds (safer).
		if (inboundPeers.contains(sender)) {
			if (txDelayedRequest.keySet().contains(txId)) {
				return;
			}
			txDelayedRequest.put(txId, new DelayedTxData(sender, CommonState.getTime() + 2000));
		} else {
			receiveTx(node, txId, sender);
			txDelayedRequest.remove(txId);
		}
	}


	private void receiveTx(Node node, int txId, Node sender) {
		txArrivalTimes.put(txId, CommonState.getTime());
		prepareAnnouncement(node, txId, sender);
	}

	private void handleReconRequest(Node node, SimpleMessage message) {
		Node sender = message.getSender();
		HashSet<Integer> reconSet = reconSets.get(sender);
		ArrayListMessage sketch = new ArrayListMessage(SimpleEvent.SKETCH, node, new ArrayList<Integer>(reconSet));
		((Transport)sender.getProtocol(FastConfig.getTransport(Peer.pid))).send(node, sender, sketch, Peer.pid);
		peerKnowsTxs.get(sender).addAll(reconSet);
		reconSet.clear();
		assert(reconSets.get(sender).size() == 0);
	}

	// Handle a sketch a peer sent us in response to our request. All sketch extension logic and
	// txId exchange is done here implicitly without actually sending messages, because a it can be
	// easily modeled and accounted at this node locally.
	private void handleSketchMessage(Node node, Node sender, ArrayList<Integer> remoteSet) {
		Set<Integer> localSet = reconSets.get(sender);
		int shared = 0, usMiss = 0, theyMiss = 0;
		// Handle transactions the local (sketch receiving) node doesn't have.
		for (Integer txId : remoteSet) {
			if (localSet.contains(txId)) {
				assert(peerKnowsTxs.get(sender).contains(txId));
				++shared;
			} else {
				++usMiss;
				receiveAnnoucement(node, txId, sender);
			}
		}

		// Handle transactions which the remote (sketch sending) node doesn't have.
		// TODO: batch-optimize
		for (Integer txId : localSet) {
			if (!remoteSet.contains(txId)) {
				theyMiss++;
				announceTx(node, txId, sender);
			}
		}

		// Compute the cost of this sketch exchange.
		int diff = usMiss + theyMiss;

		// Although diff estimation should happen at the sketch sender side, we do it here because
		// it works in our simplified model, to save extra messages.
		int localSetSize = localSet.size();
		int remoteSetSize = remoteSet.size();
		int capacity = Math.abs(localSetSize - remoteSetSize) + (int)(q * Math.min(localSetSize, remoteSetSize)) + c;

		stats.sketchItems += capacity;
		// All INVs we'd announce to them (theyMiss) are accounted above (announceTx).
		if (capacity >= diff) {
			stats.successRecons++;
			stats.shortInvs += usMiss;
			stats.invs += usMiss;
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
			if (nextFloodInbound < curTime) {
				nextFloodInbound = curTime + generateRandomDelay(this.delays.in);
				delay = 0;
			} else {
				delay = nextFloodInbound - curTime;
			}
			boolean fanout = random.nextInt(100) < (100 * fanoutDestinations.in);
			scheduleAnnouncement(node, delay + curTime, peer, txId, fanout);
		}

		ArrayList<Node> outboundPeersCopy = new ArrayList<Node>(outboundPeers);
		Collections.shuffle(outboundPeersCopy) ;
		int fanouts = fanoutDestinations.out;
		for (Node peer : outboundPeersCopy) {
			long nextFloodOutboundTime = nextFloodOutbound.get(peer);
			if (nextFloodOutboundTime < curTime) {
				delay = 0;
				nextFloodOutbound.put(peer, curTime + generateRandomDelay(this.delays.out));
			} else {
				delay = nextFloodOutboundTime - curTime;
			}
			scheduleAnnouncement(node, delay + curTime, peer, txId, fanouts-- > 0);
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
	}

	private void announceTx(Node node, int txId, Node recepient) {
		IntMessage inv = new IntMessage(SimpleEvent.INV, node, txId);
		((Transport)recepient.getProtocol(FastConfig.getTransport(Peer.pid))).send(node, recepient, inv, Peer.pid);
		++stats.invs;
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
			reconSets.put(peer, new HashSet<>());
		}
		if (outbound) nextFloodOutbound.put(peer, 0L);
	}
}