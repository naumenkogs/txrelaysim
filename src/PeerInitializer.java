package txrelaysim.src;

import txrelaysim.src.helpers.*;

import java.util.HashSet;
import java.util.HashMap;

import peersim.config.*;
import peersim.core.*;
import peersim.edsim.EDSimulator;
import peersim.transport.Transport;

public class PeerInitializer implements Control
{
	// Total number of reachable nodes.
	private int reachableCount;

	// Number of outbound connections.
	private int outPeersLegacy;
	private int outPeersRecon;

	// The tx announcement delays we apply for inbound and outbound peers.
	private int inRelayDelayReconPeer;
	private int outRelayDelayReconPeer;
	private int inRelayDelayLegacyPeer;
	private int outRelayDelayLegacyPeer;

	// How many of the nodes in the network support reconciliation.
	private int reconcilePercent;

	// Reconciliation params

	// How many of the peers are chosen as fanout targets.
	private double outFloodPeersPercent;
	private double inFloodPeersPercent;
	// The q coefficient value.
	private double defaultQ;
	// The intervals between reconciliations.
	private int reconciliationInterval;

	public PeerInitializer(String prefix) {
		reachableCount = Configuration.getInt(prefix + "." + "reachable_count");
		assert(reachableCount < Network.size());

		outPeersLegacy = Configuration.getInt(prefix + "." + "out_peers_legacy");
		outPeersRecon = Configuration.getInt(prefix + "." + "out_peers_recon");

		inRelayDelayReconPeer = Configuration.getInt(prefix + "." + "in_relay_delay_recon_peer");
		outRelayDelayReconPeer = Configuration.getInt(prefix + "." + "out_relay_delay_recon_peer");
		inRelayDelayLegacyPeer = Configuration.getInt(prefix + "." + "in_relay_delay_legacy_peer");
		outRelayDelayLegacyPeer = Configuration.getInt(prefix + "." + "out_relay_delay_legacy_peer");

		// How many of the nodes in the network support reconciliation.
		reconcilePercent = Configuration.getInt(prefix + "." + "reconcile_percent");
		assert(reconcilePercent >= 0);
		assert(reconcilePercent <= 100);
		if (reconcilePercent > 0) {
			reconciliationInterval = Configuration.getInt(prefix + "." + "reconciliation_interval");
			defaultQ = Configuration.getDouble(prefix + "." + "default_q");
			assert(outFloodPeersPercent >= 0);
			assert(outFloodPeersPercent <= 100);
			outFloodPeersPercent = Configuration.getDouble(prefix + "." + "out_flood_peers_percent");
			inFloodPeersPercent = Configuration.getDouble(prefix + "." + "in_flood_peers_percent");
			assert(outFloodPeersPercent >= 0);
			assert(outFloodPeersPercent <= 100);
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

		int reconcilingNodes = Network.size() * reconcilePercent / 100;
		// A list storing who is already connected to who, so that we don't make duplicate conns.
		HashMap<Integer, HashSet<Integer>> peers = new HashMap<>();
		for (int i = 1; i < Network.size(); i++) {
			peers.put(i, new HashSet<>());
			// Initial parameters setting for all nodes.

			if (reconcilingNodes > 0) {
				reconcilingNodes--;
				((Peer)Network.get(i).getProtocol(Peer.pid)).reconcile = true;
				((Peer)Network.get(i).getProtocol(Peer.pid)).reconciliationInterval = reconciliationInterval;
				((Peer)Network.get(i).getProtocol(Peer.pid)).inFloodLimitPercent = inFloodPeersPercent;
				((Peer)Network.get(i).getProtocol(Peer.pid)).outFloodLimitPercent = outFloodPeersPercent;
				((Peer)Network.get(i).getProtocol(Peer.pid)).defaultQ = defaultQ;
				((Peer)Network.get(i).getProtocol(Peer.pid)).inRelayDelay = inRelayDelayReconPeer;
				((Peer)Network.get(i).getProtocol(Peer.pid)).outRelayDelay = outRelayDelayReconPeer;
			} else {
				((Peer)Network.get(i).getProtocol(Peer.pid)).reconcile = false;
				((Peer)Network.get(i).getProtocol(Peer.pid)).inFloodLimitPercent = 100;
				((Peer)Network.get(i).getProtocol(Peer.pid)).outFloodLimitPercent = 100;
				((Peer)Network.get(i).getProtocol(Peer.pid)).inRelayDelay = inRelayDelayLegacyPeer;
				((Peer)Network.get(i).getProtocol(Peer.pid)).outRelayDelay = outRelayDelayLegacyPeer;
			}
		}

		// Connect all nodes to a limited number of reachable nodes.
		for(int i = 1; i < Network.size(); i++) {
			Node curNode = Network.get(i);
			boolean curNodeSupportsRecon = ((Peer)Network.get(i).getProtocol(Peer.pid)).reconcile;
			int connsTarget = curNodeSupportsRecon ? outPeersRecon : outPeersLegacy;
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