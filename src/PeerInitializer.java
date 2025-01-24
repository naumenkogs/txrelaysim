package txrelaysim.src;

import txrelaysim.src.helpers.*;

import java.util.HashSet;
import java.util.HashMap;

import peersim.config.*;
import peersim.core.*;
import peersim.edsim.EDSimulator;
import peersim.transport.Transport;

class Delays {
	public int in;
	public int out;

	public Delays(int in, int out) {
		this.in = in;
		this.out = out;
	}
}

public class PeerInitializer implements Control
{
	public static Delays legacyDelays;

	// Erlay configurations
	public static Delays erlayDelays;

	// Total number of reachable nodes.
	private int reachableCount;

	// How many of the nodes in the network support reconciliation.
	private int reconcileCount;

	// Reconciliation params
	// The q coefficient value.
	private double defaultQ;
	// The intervals between reconciliations.
	private int reconciliationInterval;

	private FanoutDestinations erlayDestinations;

	public PeerInitializer(String prefix) {
		reachableCount = Configuration.getInt(prefix + "." + "reachable_count");
		assert(reachableCount <= Network.size());

		legacyDelays = new Delays(Configuration.getInt(prefix + "." + "in_delay_legacy"),
										Configuration.getInt(prefix + "." + "out_delay_legacy"));


		reconcileCount = Configuration.getInt(prefix + "." + "reconcile_count");
		assert(reconcileCount <= Network.size());

		if (reconcileCount > 0) {
			Peer.reconciliationInterval = Configuration.getInt(prefix + "." + "reconciliation_interval");
			Peer.q = Configuration.getDouble(prefix + "." + "q");
			Peer.c = Configuration.getInt(prefix + "." + "c");

			erlayDelays = new Delays(Configuration.getInt(prefix + "." + "in_delay_erlay"),
											Configuration.getInt(prefix + "." + "out_delay_erlay"));

			erlayDestinations = new FanoutDestinations(Configuration.getDouble(prefix + "." + "in_fanout_fraction_erlay"),
														Configuration.getInt(prefix + "." + "out_fanout_erlay"));

		}
	}

	@Override
	public boolean execute() {
		// Set a subset of nodes to be reachable by other nodes.
		HashSet<Integer> reachableNodes = new HashSet<Integer>();
		while (reachableNodes.size() < reachableCount) {
			int reachableCandidate = CommonState.r.nextInt(Network.size() - 1) + 1;
			reachableNodes.add(reachableCandidate);
		}

		// A list storing who is already connected to who, so that we don't make duplicate conns.
		HashMap<Integer, HashSet<Integer>> peers = new HashMap<>();
		for (int i = 1; i < Network.size(); i++) {
			peers.put(i, new HashSet<>());

			if (reconcileCount-- > 0) {
				((Peer)Network.get(i).getProtocol(Peer.pid)).reconcile = true;
				((Peer)Network.get(i).getProtocol(Peer.pid)).delays = erlayDelays;
				((Peer)Network.get(i).getProtocol(Peer.pid)).fanoutDestinations = erlayDestinations;
			} else {
				((Peer)Network.get(i).getProtocol(Peer.pid)).reconcile = false;
				((Peer)Network.get(i).getProtocol(Peer.pid)).delays = legacyDelays;
			}
		}

		// Connect all nodes to a limited number of reachable nodes.
		for(int i = 1; i < Network.size(); i++) {
			Node curNode = Network.get(i);
			boolean curNodeSupportsRecon = ((Peer)Network.get(i).getProtocol(Peer.pid)).reconcile;
			int connsTarget = 8;
			while (connsTarget > 0) {
				int randomNodeIndex = CommonState.r.nextInt(Network.size() - 1) + 1;
				if (randomNodeIndex == i) {
					continue;
				}

				if (!reachableNodes.contains(randomNodeIndex)) continue;


				if (peers.get(i).contains(randomNodeIndex)) {
					continue;
				}

				assert(!peers.get(randomNodeIndex).contains(i));

				assert(peers.get(i).add(randomNodeIndex));
				assert(peers.get(randomNodeIndex).add(i));

				// Actual connecting.
				Node randomNode = Network.get(randomNodeIndex);
				((Peer)curNode.getProtocol(Peer.pid)).addPeer(randomNode, true);
				((Peer)randomNode.getProtocol(Peer.pid)).addPeer(curNode, false);
				--connsTarget;
			}
		}

		System.err.println("Initialized peers");
		return true;
	}
}