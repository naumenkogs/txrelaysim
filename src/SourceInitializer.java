package txrelaysim.src;

import peersim.config.*;
import peersim.core.*;

public class SourceInitializer implements Control
{
	public static final int sourceIndex = 0;
	private int tps;

	public SourceInitializer(String prefix) {}

	@Override
	public boolean execute() {
		// Set node 0 as source.
		((Source) Network.get(sourceIndex).getProtocol(Source.pid)).isSource = true;

		//set other nodes as not source.
		for(int i = 1; i < Network.size() - 1; i++)
			((Source) Network.get(i).getProtocol(Source.pid)).isSource = false;

		// Source connects to some nodes.
		Node source = Network.get(0);
		for (int sourceConns = 0; sourceConns < 20; ++sourceConns) {
			((Source)source.getProtocol(Source.pid)).addPeer();
		}

		return true;
	}
}
