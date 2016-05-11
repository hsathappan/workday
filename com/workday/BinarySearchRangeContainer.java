package com.workday;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class BinarySearchRangeContainer implements RangeContainer {

	long[] clonedData;
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
		
		List<Short> resultIds = new LinkedList<>();
		while (fromIndex < clonedData.length && clonedData[fromIndex] < toValue) {
			List<Short> ids = indexMap.get(clonedData[fromIndex]);
			//resultIds.addAll(ids);
			resultIds = collateSortedLists(resultIds, ids);
			fromIndex++;
		}
		
		if (toInclusive) {
			if (indexMap.containsKey(toValue)) {
				//resultIds.addAll(indexMap.get(toValue));
				resultIds = collateSortedLists(resultIds, indexMap.get(toValue));
			}
		}
		
		Collections.sort(resultIds);
		return new IdsImpl(resultIds);
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
	
	private List<Short> collateSortedLists(List<Short> l1, List<Short> l2) {
        if (l1 == null || l1.size() == 0) {
        	return l2;
        }
        else if (l2 == null || l2.size() == 0) {
        	return l1;
        }
        
        return merge(l1, l2);
    }
	
	private List<Short> merge(List<Short> l1, List<Short> l2) {
		List<Short> mergedList = new LinkedList<>();
		
		Iterator<Short> i1 = l1.listIterator();
		Iterator<Short> i2 = l2.listIterator();
		
		Short item1 = i1.next();
		Short item2 = i2.next();
		
		while (true) {
			if (item1 < item2) {
				mergedList.add(item1);
				if (i1.hasNext()) {
					item1 = i1.next();
				}
				else {
					mergedList.add(item2);
					break;
				}
			}
			else {
				mergedList.add(item2);
				if (i2.hasNext()) {
					item2 = i2.next();
				}
				else {
					mergedList.add(item1);
					break;
				}
			}
		} 
		
		while (i1.hasNext()) {
			mergedList.add(i1.next());
		}
		
		while (i2.hasNext()) {
			mergedList.add(i2.next());
		}
		
		return mergedList;
	}
}
