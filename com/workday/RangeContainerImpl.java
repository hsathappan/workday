/**
 * 
 */
package com.workday;

import java.util.ArrayList;
import java.util.List;

/**
 * @author hsathappan
 *
 */
public class RangeContainerImpl implements RangeContainer {

	private long[] data;
	
	/**
	 * 
	 */
	private RangeContainerImpl(long[] data) {
		this.data = data.clone();
	}
	
	public static RangeContainer create(long[] data) {
		if (data == null || data.length > 32000 || data.length == 0) {
			throw new IllegalArgumentException("number of elements of input data should be in the range 1 <= n <= 32k");
		}
		return new RangeContainerImpl(data);
	}

	@Override
	public Ids findIdsInRange(long fromValue, long toValue,
			boolean fromInclusive, boolean toInclusive) {
		List<Short> idList = new ArrayList<>();
		for (short i = 0; i < data.length; i++) {
			if (inRange(data[i], fromValue, toValue, fromInclusive, toInclusive)) {
				idList.add(i);
			}
		}
		return new IdsImpl(idList);
	}

	private boolean inRange(long value, long fromValue, long toValue,
			boolean fromInclusive, boolean toInclusive) {
		boolean isAboveLowerRange = fromInclusive ? value >= fromValue : value > fromValue;
		boolean isBelowUpperRange = toInclusive ? value <= toValue : value < toValue;
		return isAboveLowerRange && isBelowUpperRange;
	}

}
