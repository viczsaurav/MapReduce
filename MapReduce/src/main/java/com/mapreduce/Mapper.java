package com.mapreduce;

import java.io.Serializable;

public interface Mapper<Key extends Comparable<Key>, Value> extends Serializable {
	
	/**
	 * Method required for one-time setup before map call for the context
	 * Added in as replacement to abstract MapperClass
	 * @param cntxt
	 * @throws Exception
	 */
	public void setup(JobContext<Key,Value> cntxt) throws Exception;
	
	/**
	 * Main map method required to read input line and map them
	 * @param key
	 * @param val
	 * @param cntxt
	 * @throws Exception
	 */
	public void map(Key key, Value val, JobContext<Key,Value> cntxt) throws Exception;

}
