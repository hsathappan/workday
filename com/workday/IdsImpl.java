package com.workday;

import java.util.Iterator;
import java.util.List;

public class IdsImpl implements Ids {

	List<Short> idList;
	Iterator<Short> itr;
	
	public IdsImpl(List<Short> idList) {
		this.idList = idList;
		this.itr = this.idList.iterator();
	}

	@Override
	public short nextId() {
		return itr.hasNext() ? itr.next() : Ids.END_OF_IDS;
	}

}
