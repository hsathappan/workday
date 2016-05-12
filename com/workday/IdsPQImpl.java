package com.workday;

import java.util.PriorityQueue;

/**
 * Implementation of the Ids interface using a priority queue
 * @author hsathappan
 *
 */
public class IdsPQImpl implements Ids {

	PriorityQueue<Short> idList;
	
	public IdsPQImpl(PriorityQueue<Short> idList) {
		this.idList = idList;
	}

	@Override
	public short nextId() {
		return !idList.isEmpty() ? idList.poll() : Ids.END_OF_IDS;
	}

}
