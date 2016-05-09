package com.workday;

import java.util.LinkedList;
import java.util.List;

public final class Mapper {

	int mapperNumber;
	long[] data;
	
	Mapper(int n, long[] data) {
		this.mapperNumber = n;
		this.data = data;
	}
	
	public List<Short> findIdsInRange(long fromValue, long toValue,
			boolean fromInclusive, boolean toInclusive) {
		List<Short> idList = new LinkedList<>();
		for (short i = 0; i < data.length; i++) {
			if (inRange(data[i], fromValue, toValue, fromInclusive, toInclusive)) {
				short id = (short) (mapperNumber + i);
				idList.add(id);
			}
		}
		
		return idList;
	}
	
	private boolean inRange(long value, long fromValue, long toValue,
			boolean fromInclusive, boolean toInclusive) {
		boolean isAboveLowerRange = fromInclusive ? value >= fromValue : value > fromValue;
		boolean isBelowUpperRange = toInclusive ? value <= toValue : value < toValue;
		return isAboveLowerRange && isBelowUpperRange;
	}

}
