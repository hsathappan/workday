package com.workday;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Uses a map-reduce algorithm to find the ids in range
 * @author hsathappan
 *
 */
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

	/**
	 * This method does the map part of the algorithm
	 * Creates the mappers list which is based on the data length and the mapper data size
	 * i.e the number of elements that the mapper will perform the range search on.
	 * @param data
	 * @return
	 */
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
	
	/**
	 * This method does the reduce part of the algorithm.
	 * It creates as many threads as mappers and then executes the range query 
	 * and merges the results from each thread to return the final result.
	 * @param fromValue
	 * @param toValue
	 * @param fromInclusive
	 * @param toInclusive
	 * @return
	 */
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
		
		// the results are in the same order in which they were added to the executor service.
		List<Short> resultIds = new LinkedList<>();
		for (Future<List<Short>> result : reducerResults) {
			try {
				resultIds.addAll(result.get());
			} catch (InterruptedException | ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		// the results are in the order expected and hence no need to sort.
		return resultIds;
	}

	/**
	 * Calls the reduce method which creates as many threads as mappers and invokes the range 
	 * query on each thread before merging the results.
	 */
	@Override
	public Ids findIdsInRange(long fromValue, long toValue,
			boolean fromInclusive, boolean toInclusive) {
		List<Short> idsList = reduce(fromValue, toValue, fromInclusive, toInclusive);
		return new IdsImpl(idsList);
	}
	
}
