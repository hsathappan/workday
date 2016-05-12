/**
 * 
 */
package com.workday;

/**
 * RangeContainerFactory implementation which creates the immutable RangeContainer
 * @author hsathappan
 *
 */
public class RangeContainerFactoryImpl implements RangeContainerFactory {

	public RangeContainerFactoryImpl() {
		
	}

	@Override
	public RangeContainer createContainer(long[] data) {
		//return RangeContainerImpl.create(data);
		//return MapReduceRangeContainer.create(data);
		return BinarySearchRangeContainer.create(data);
	}

}
