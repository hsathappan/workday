package com.workday;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class MapReduceRangeContainer implements RangeContainer {

	//private static short MAPPER_DATA_SIZE = 1000;
	private static short MAPPER_DATA_SIZE = 2;
	
	long[] data;
	List<Mapper> mapperList;
	
	private MapReduceRangeContainer(long[] data) {
		if (data.length > 32000) {
			throw new IllegalArgumentException("data > 32k");
		}
		this.data = data;
		mapperList = createMappers(data);
		
	}
	
	public static RangeContainer create(long[] data) {
		return new MapReduceRangeContainer(data);
	}

	private List<Mapper> createMappers(long[] data) {
		int noOfMappers = data.length / MAPPER_DATA_SIZE;
		if (data.length % MAPPER_DATA_SIZE != 0) {
			noOfMappers++;
		}
		List<Mapper> mappers = new LinkedList<>();
		
		for (int i = 0; i < noOfMappers; i++) {
			int startRange = i * MAPPER_DATA_SIZE;
			int endRange = Math.min(startRange + MAPPER_DATA_SIZE, data.length) ;
			Mapper mapper = new Mapper(startRange, Arrays.copyOfRange(data, startRange, endRange));
			mappers.add(mapper);
		}
		
		return mappers;
	}

	@Override
	public Ids findIdsInRange(long fromValue, long toValue,
			boolean fromInclusive, boolean toInclusive) {
		List<Short> idsList = new LinkedList<>();
		for (Mapper mapper : mapperList) {
			List<Short> idList = mapper.findIdsInRange(fromValue, toValue, fromInclusive, toInclusive);
			idsList.addAll(idList);
		}
		return new IdsImpl(idsList);
	}
	
	public static class Mapper {
		
		int mapperNumber;
		long[] data;
		
		private Mapper(int n, long[] data) {
			this.mapperNumber = n;
			this.data = data;
		}
		
		public List<Short> findIdsInRange(long fromValue, long toValue,
				boolean fromInclusive, boolean toInclusive) {
			List<Short> idList = new ArrayList<>();
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

}
