package txrelaysim.src.helpers;

import java.util.ArrayList;

import peersim.core.Node;

public class ArrayListMessage extends SimpleMessage {

	private ArrayList<Integer> arrayList;

	public ArrayListMessage(int type, Node sender, ArrayList<Integer> arrayList) {
		super(type, sender);
		this.arrayList = arrayList;
	}

	public ArrayList<Integer> getArrayList() {
		return this.arrayList;
	}

}
