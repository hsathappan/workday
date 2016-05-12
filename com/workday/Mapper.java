package com.workday;

import java.util.LinkedList;
import java.util.List;

/**
 * Mapper for the map-reduce algorithm which has a subset of the input data
 * @author hsathappan
 *
 */
public final class Mapper {

	int mapperNumber;
	long[] data;
	
	Mapper(int n, long[] data) {
		this.mapperNumber = n;
		this.data = data;
	}
	
	/**
	 * Finds the ids in range on the subset of the data using linear search
	 * @param fromValue
	 * @param toValue
	 * @param fromInclusive
	 * @param toInclusive
	 * @return
	 */
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
	
	// determines if the given value is in range
	private boolean inRange(long value, long fromValue, long toValue,
			boolean fromInclusive, boolean toInclusive) {
		boolean isAboveLowerRange = fromInclusive ? value >= fromValue : value > fromValue;
		boolean isBelowUpperRange = toInclusive ? value <= toValue : value < toValue;
		return isAboveLowerRange && isBelowUpperRange;
	}

}
