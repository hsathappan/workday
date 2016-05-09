package com.workday;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class RangeQueryBasicTest {

	private RangeContainer rc;
	private long[] data32k;
	
	@Before
	public void setUp() {
		RangeContainerFactory rf = new RangeContainerFactoryImpl();
		rc = rf.createContainer(new long[]{10,12,17,21,2,15,16});
		
		data32k = new long[32000];
		for (int i = 0; i < data32k.length; i++) {
			data32k[i] = 1;
		}
	}

	@Test
	public void runARangeQuery() {
		Ids ids = rc.findIdsInRange(14, 17, true, true);
		assertEquals(2, ids.nextId());
		assertEquals(5, ids.nextId());
		assertEquals(6, ids.nextId());
		assertEquals(Ids.END_OF_IDS, ids.nextId());
		
		ids = rc.findIdsInRange(14, 17, true, false);
		assertEquals(5, ids.nextId());
		assertEquals(6, ids.nextId());
		assertEquals(Ids.END_OF_IDS, ids.nextId());
		
		ids = rc.findIdsInRange(20, Long.MAX_VALUE, false, true);
		assertEquals(3, ids.nextId());
		assertEquals(Ids.END_OF_IDS, ids.nextId());
		
	}
	
	@Test
	public void runMaxCapacityRangeQuery() {
		int noOfRuns = 5;
		data32k[10] = 7;
		RangeContainerFactory rf = new RangeContainerFactoryImpl();
		RangeContainer rcLarge = rf.createContainer(data32k);
		
		long time = 0;
		for (int i = 0; i < noOfRuns; i++) {
			long startTime = System.nanoTime();
			Ids ids = rcLarge.findIdsInRange(6, 17, true, true);
			long estimatedTime = System.nanoTime() - startTime;
			assertEquals(10, ids.nextId());
			assertEquals(Ids.END_OF_IDS, ids.nextId());
			
			System.out.println(estimatedTime);
			time += estimatedTime;
		}
		
		System.out.println("Avg time is: " + time/noOfRuns);
	}

}