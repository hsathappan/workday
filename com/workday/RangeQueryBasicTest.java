package com.workday;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class RangeQueryBasicTest {

	private RangeContainer rc;
	private long[] data32k;
	private long[] data32kDifferentNumbers;
	
	@Rule
    public ExpectedException thrown= ExpectedException.none();
	
	@Before
	public void setUp() {
		RangeContainerFactory rf = new RangeContainerFactoryImpl();
		rc = rf.createContainer(new long[]{10,12,17,21,2,15,16});
		
		data32k = new long[32000];
		for (int i = 0; i < data32k.length; i++) {
			data32k[i] = 1;
		}
		
		data32kDifferentNumbers = new long[32000];
		for (int i = 0; i < data32kDifferentNumbers.length; i++) {
			data32kDifferentNumbers[i] = i;
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
	public void runARangeQueryWithFromValueInData() {
		Ids ids = rc.findIdsInRange(15, 17, true, true);
		assertEquals(2, ids.nextId());
		assertEquals(5, ids.nextId());
		assertEquals(6, ids.nextId());
		assertEquals(Ids.END_OF_IDS, ids.nextId());
		
		ids = rc.findIdsInRange(15, 17, true, false);
		assertEquals(5, ids.nextId());
		assertEquals(6, ids.nextId());
		assertEquals(Ids.END_OF_IDS, ids.nextId());
		
		ids = rc.findIdsInRange(21, Long.MAX_VALUE, false, true);
		assertEquals(Ids.END_OF_IDS, ids.nextId());
		
		ids = rc.findIdsInRange(16, 21, false, false);
		assertEquals(2, ids.nextId());
		assertEquals(Ids.END_OF_IDS, ids.nextId());
		
	}
	
	@Test
	public void runRangeQueryWithDups() {
		long[] data = {1,3,4,1,4,7};
		RangeContainerFactory rf = new RangeContainerFactoryImpl();
		RangeContainer rcDups = rf.createContainer(data);
		Ids ids = rcDups.findIdsInRange(4, 4, true, true);
		assertEquals(2, ids.nextId());
		assertEquals(4, ids.nextId());
		
		ids = rcDups.findIdsInRange(1, 3, true, false);
		assertEquals(0, ids.nextId());
		assertEquals(3, ids.nextId());
		assertEquals(Ids.END_OF_IDS, ids.nextId());
		
		ids = rcDups.findIdsInRange(1, 4, false, true);
		assertEquals(1, ids.nextId());
		assertEquals(2, ids.nextId());
		assertEquals(4, ids.nextId());
		assertEquals(Ids.END_OF_IDS, ids.nextId());
		
		ids = rcDups.findIdsInRange(16, 21, false, false);
		assertEquals(Ids.END_OF_IDS, ids.nextId());		
	}
	
	@Test
	public void runRangeQueryWithRangeOutsideInput() {
		long[] data = {2,3,4,2,4,7};
		RangeContainerFactory rf = new RangeContainerFactoryImpl();
		RangeContainer rcOutOfRange = rf.createContainer(data);
		
		Ids ids = rcOutOfRange.findIdsInRange(-1, 1, true, true);
		assertEquals(Ids.END_OF_IDS, ids.nextId());
		
		ids = rcOutOfRange.findIdsInRange(8, 10, true, true);
		assertEquals(Ids.END_OF_IDS, ids.nextId());
		
		ids = rcOutOfRange.findIdsInRange(1, 2, false, false);
		assertEquals(Ids.END_OF_IDS, ids.nextId());
		
		ids = rcOutOfRange.findIdsInRange(7, 9, false, false);
		assertEquals(Ids.END_OF_IDS, ids.nextId());
	}
	
	@Test
	public void runRangeQueryWithInvalidInput() {
		thrown.expect(IllegalArgumentException.class);
		RangeContainerFactory rf = new RangeContainerFactoryImpl();
		rf.createContainer(new long[0]);
	}
	
	@Test
	public void benchMarkTestWithSameNumber() {
		System.out.println("Bench mark test with same nos");
		int noOfRuns = 5;
		data32k[10] = 7;
		RangeContainerFactory rf = new RangeContainerFactoryImpl();
		RangeContainer rcLarge = rf.createContainer(data32k);
		
		Ids ids = benchMark(rcLarge, noOfRuns, 6, 17, true, true);
		assertEquals(10, ids.nextId());
		assertEquals(Ids.END_OF_IDS, ids.nextId());
		
		ids = benchMark(rcLarge, noOfRuns, 1, 17, true, true);
		for (int i = 0; i < 32000; i++) {
			assertEquals(i, ids.nextId());
		}
		assertEquals(Ids.END_OF_IDS, ids.nextId());
	}
	
	@Test
	public void benchMarkTestDifferentNumbers() {
		System.out.println("Bench mark test with diff. nos");
		int noOfRuns = 5;
		RangeContainerFactory rf = new RangeContainerFactoryImpl();
		RangeContainer rcLarge = rf.createContainer(data32kDifferentNumbers);
		
		Ids ids = benchMark(rcLarge, noOfRuns, 1, 17, true, true);
		for (int i = 1; i <= 17; i++) {
			assertEquals(i, ids.nextId());
		}
		assertEquals(Ids.END_OF_IDS, ids.nextId());
	}
	
	private Ids benchMark(RangeContainer rc, int noOfRuns, long fromValue, long toValue, boolean fromInclusive, boolean toInclusive) {
		Ids ids = null;
		double time = 0;
		for (int i = 0; i < noOfRuns; i++) {
			long startTime = System.nanoTime();
			ids = rc.findIdsInRange(fromValue, toValue, fromInclusive, toInclusive);
			long estimatedTime = System.nanoTime() - startTime;
			
			System.out.println(estimatedTime);
			time += estimatedTime;
		}
		
		System.out.println("Avg time in ns is: " + time/noOfRuns);
		
		return ids;
	}

}