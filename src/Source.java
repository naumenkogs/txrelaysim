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
	public boolean isSource = false;
	private ArrayList<Node> peerList;

	public Source(String prefix) {
		this.peerList = new ArrayList<>();
	}

	@Override
	public void nextCycle(Node node, int pid) {
		if (isSource == false)
			return;

		// if the experiment is over soon, stop issuing transactions and let existing propagate.
		if (CommonState.getEndTime() < CommonState.getTime() + 40 * 1000) {
			return;
		}

		int randomNumberOfTxs = CommonState.r.nextInt(7 * 2); // anything from 0 to 14.
		for (int txid = 0; txid < randomNumberOfTxs; ++txid) {
			int randomRecipientIndex = CommonState.r.nextInt(peerList.size() - 1) + 1;
			Node recipient = peerList.get(randomRecipientIndex);
			IntMessage inv = new IntMessage(SimpleEvent.INV, node, txid);
			((Transport)recipient.getProtocol(FastConfig.getTransport(Peer.pid))).send(node, recipient, inv, Peer.pid);
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