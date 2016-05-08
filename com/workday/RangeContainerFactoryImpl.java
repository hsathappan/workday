/**
 * 
 */
package com.workday;

/**
 * @author hsathappan
 *
 */
public class RangeContainerFactoryImpl implements RangeContainerFactory {

	/**
	 * 
	 */
	public RangeContainerFactoryImpl() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public RangeContainer createContainer(long[] data) {
		//return RangeContainerImpl.create(data);
		return MapReduceRangeContainer.create(data);
	}

}
