/*
 * Copyright (c) 2003-2005 The BISON Project
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 *
 */

package txrelaysim.src;

import txrelaysim.src.helpers.*;

import peersim.config.*;
import peersim.core.*;
import peersim.util.*;

import java.util.Map;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;
import java.util.*;


public class InvObserver implements Control
{
	/** Protocol identifier */
	private final int pid = 2;

	public InvObserver(String prefix) {
	}

	public boolean execute() {
		HashMap<Integer, ArrayList<Long>> txArrivalTimes = new HashMap<Integer, ArrayList<Long>>();
		Stats stats = new Stats();

		for(int i = 1; i < Network.size(); i++) {
			Peer peer = (Peer) Network.get(i).getProtocol(pid);

			// Store all arrival times (at every node) for all transactions. We will later use this
			// to calculate latency.
			Iterator it = peer.txArrivalTimes.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry pair = (Map.Entry)it.next();
				Integer txId = (Integer)pair.getKey();
				Long arrivalTime = (Long)pair.getValue();
				if (txArrivalTimes.get(txId) == null) {
					txArrivalTimes.put(txId, new ArrayList<>());
				}
				txArrivalTimes.get(txId).add(arrivalTime);
			}

			stats.successRecons += peer.stats.successRecons;
			stats.failedRecons += peer.stats.failedRecons;
			stats.invs += peer.stats.invs;
			stats.shortInvs += peer.stats.shortInvs;
			stats.sketchItems += peer.stats.sketchItems;
		}

		int allTxs = txArrivalTimes.size();

		// Measure the delays it took to reach majority of the nodes (based on receival time).
		ArrayList<Long> avgTxArrivalDelay = new ArrayList<>();
		Iterator it = txArrivalTimes.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry)it.next();
			// A workaround to avoid unchecked cast.
			ArrayList<?> ar = (ArrayList<?>) pair.getValue();
			ArrayList<Long> arrivalTimes = new ArrayList<>();

			if (ar.size() < (Network.size() - 1) * 0.99) continue;

			for (Object x : ar) {
				arrivalTimes.add((Long) x);
			}

	   		Collections.sort(arrivalTimes);
			int percentile95Index = (int)(arrivalTimes.size() * 0.95);
			Long percentile95delay = (arrivalTimes.get(percentile95Index) - arrivalTimes.get(0));
			avgTxArrivalDelay.add(percentile95delay);
		}

		int totalBw = stats.invs * 32 + (stats.shortInvs + stats.sketchItems) * 8;

		System.err.println("");
		System.err.println("-----------RESULTS--------");
		System.err.println("Relayed txs: " + allTxs);
		System.err.println(String.format("Reconciliations. Success: %d, fail: %d.", stats.successRecons, stats.failedRecons));
		System.err.println(String.format("Total bandwidth (in megabytes): %d", totalBw / 1024 / 1024));

		double avgMaxDelay = avgTxArrivalDelay.stream().mapToLong(val -> val).average().orElse(0.0);
		System.out.println("Avg max latency: " + avgMaxDelay);
		return false;
	}
}
