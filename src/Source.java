package txrelaysim.src;

import txrelaysim.src.helpers.*;

import java.util.ArrayList;

import peersim.cdsim.CDProtocol;
import peersim.config.FastConfig;
import peersim.config.Configuration;
import peersim.core.Network;
import peersim.core.Node;
import peersim.core.CommonState;
import peersim.edsim.EDProtocol;
import peersim.edsim.EDSimulator;
import peersim.transport.Transport;


public class Source implements CDProtocol, EDProtocol
{
	public static int pid = 1;
	public static int tps;
	private ArrayList<Node> peerList;
	private static int txId = 0;

	public Source(String prefix) {
		this.peerList = new ArrayList<>();
	}

	@Override
	public void nextCycle(Node node, int pid) {
		// TODO: why we call this for more than the first node?
		if (peerList.size() == 0 ) return;

		int randomNumberOfTxs = CommonState.r.nextInt(7 * 2); // anything from 0 to 14.
		while (randomNumberOfTxs > 0) {
			int randomRecipientIndex = CommonState.r.nextInt(peerList.size() - 1) + 1;
			Node recipient = peerList.get(randomRecipientIndex);
			IntMessage inv = new IntMessage(SimpleEvent.INV, node, ++txId);
			((Transport)recipient.getProtocol(FastConfig.getTransport(Peer.pid))).send(node, recipient, inv, Peer.pid);
			--randomNumberOfTxs;
		}
	}

	@Override
	public void processEvent(Node node, int pid, Object event) {
		return;
	}

	public Object clone() {
		return new Source("");
	}

	public void addPeer() {
		int randomNodeIndex = CommonState.r.nextInt(Network.size() - 1) + 1;
		Node node = Network.get(randomNodeIndex);
		peerList.add(node);
	}

}