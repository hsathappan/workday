package com.workday;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * Invokes the range query on each mapper to return the result of the subset data in the mapper.
 * @author hsathappan
 *
 */
public final class Reducer implements Callable<List<Short>> {

	private Mapper mapper;
	private long fromValue;
	private long toValue;
	private boolean fromInclusive;
	private boolean toInclusive;

	Reducer(Mapper mapper, long fromValue, long toValue,
			boolean fromInclusive, boolean toInclusive) {
		this.mapper = mapper;
		this.fromValue = fromValue;
		this.toValue = toValue;
		this.fromInclusive = fromInclusive;
		this.toInclusive = toInclusive;
	}
	
	@Override
	public List<Short> call() {
		List<Short> ids = mapper.findIdsInRange(fromValue, toValue, fromInclusive, toInclusive);
		return ids;
	}

}
