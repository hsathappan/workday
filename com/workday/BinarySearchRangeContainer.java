package com.workday;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * Does a binary search on the sorted data and returns the indexes in the range
 * @author hsathappan
 *
 */
public class BinarySearchRangeContainer implements RangeContainer {

	// the input data without any dups
	long[] clonedData;
	
	// key value mapping of input data and list of indexes whose value equals the key
	Map<Long, List<Short>> indexMap; 
	
	private BinarySearchRangeContainer(long[] data) {
		if (data == null || data.length > 32000 || data.length == 0) {
			throw new IllegalArgumentException("number of elements of input data should be in the range 1 <= n <= 32k");
		}
		indexMap = new HashMap<>();
		for (int i = 0; i < data.length; i++) {
			List<Short> indexes;
			if (indexMap.containsKey(data[i])) {
				indexes = indexMap.get(data[i]);
			}
			else {
				indexes = new LinkedList<>();
				indexMap.put(data[i], indexes);
			}
			indexes.add((short) i);
		}
		
		clonedData = new long[indexMap.keySet().size()];
		int c = 0;
		for (Long l : indexMap.keySet()) {
			clonedData[c] = l;
			c++;
		}
		Arrays.sort(clonedData);
	}
	
	public static RangeContainer create(long[] data) {
		return new BinarySearchRangeContainer(data);
	}

	@Override
	public Ids findIdsInRange(long fromValue, long toValue,
			boolean fromInclusive, boolean toInclusive) {
		int fromIndex = Arrays.binarySearch(clonedData, fromValue);
		fromIndex = getFromIndex(fromIndex, fromInclusive, fromValue);
		
		if (fromIndex >= clonedData.length) {
			// range not in data
			return new IdsImpl(Collections.emptyList());
		}
		
		PriorityQueue<Short> resultIds = new PriorityQueue<>();
		while (fromIndex < clonedData.length && clonedData[fromIndex] < toValue) {
			List<Short> ids = indexMap.get(clonedData[fromIndex]);
			resultIds.addAll(ids);
			fromIndex++;
		}
		
		if (toInclusive) {
			if (indexMap.containsKey(toValue)) {
				resultIds.addAll(indexMap.get(toValue));				
			}
		}
		
		return new IdsPQImpl(resultIds);
	}
	
	private int getFromIndex(int fromIndex, boolean fromInclusive, long fromValue) {
		// if fromIndex < 0, fromValue is not in the array
		// fromIndex = -(fromIndex) - 1;
		// else
		// if fromInclusive
		//		return fromValue
		// else
		//		move to the right to get the first index whose value is > fromValue
		if (fromIndex < 0) {
			fromIndex = -fromIndex - 1;
		}
		else {
			if (!fromInclusive) {
				fromIndex++;
			}				
		}
		
		return fromIndex;
	}
	
}
