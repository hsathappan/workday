package com.workday;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class MapReduceRangeContainer implements RangeContainer {

	private static short MAPPER_DATA_SIZE = 1000;
	//private static short MAPPER_DATA_SIZE = 2;
	
	List<Mapper> mapperList;
	
	private MapReduceRangeContainer(long[] data) {
		if (data == null || data.length > 32000 || data.length == 0) {
			throw new IllegalArgumentException("number of elements of input data should be in the range 1 <= n <= 32k");
		}
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
	
	private List<Short> reduce(long fromValue, long toValue,
			boolean fromInclusive, boolean toInclusive) {
		ExecutorService executor = Executors.newFixedThreadPool(mapperList.size());
		List<Callable<List<Short>>> reducers = new LinkedList<>();
		for (Mapper mapper : mapperList) {
			Callable<List<Short>> reducer = new Reducer(mapper, fromValue, toValue, fromInclusive, toInclusive);
			reducers.add(reducer);
		}
		
		List<Future<List<Short>>> reducerResults = null;
		
		try {
			reducerResults = executor.invokeAll(reducers);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			executor.shutdown();
		}
		// Wait until all threads are finish
		while (!executor.isTerminated()) {
 
		}
		//System.out.println("\nFinished all threads");
		
		List<Short> resultIds = new LinkedList<>();
		for (Future<List<Short>> result : reducerResults) {
			try {
				resultIds.addAll(result.get());
			} catch (InterruptedException | ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		return resultIds;
	}

	@Override
	public Ids findIdsInRange(long fromValue, long toValue,
			boolean fromInclusive, boolean toInclusive) {
		List<Short> idsList = reduce(fromValue, toValue, fromInclusive, toInclusive);
		return new IdsImpl(idsList);
	}
	
	/*@Override
	public Ids findIdsInRange(long fromValue, long toValue,
			boolean fromInclusive, boolean toInclusive) {
		List<Short> idsList = new LinkedList<>();
		for (Mapper mapper : mapperList) {
			List<Short> idList = mapper.findIdsInRange(fromValue, toValue, fromInclusive, toInclusive);
			idsList.addAll(idList);
		}
		return new IdsImpl(idsList);
	}*/

}
