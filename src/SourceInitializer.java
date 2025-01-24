package txrelaysim.src;

import peersim.config.*;
import peersim.core.*;

public class SourceInitializer implements Control
{
	public SourceInitializer(String prefix) {}

	@Override
	public boolean execute() {
		// Source connects to some nodes.
		Node source = Network.get(0);
		for (int sourceConns = 0; sourceConns < 20; ++sourceConns) {
			((Source)source.getProtocol(Source.pid)).addPeer();
		}

		return true;
	}
}
