package com.mapreduce;

import java.io.Serializable;

public interface Mapper<Key extends Comparable<Key>, Value> extends Serializable {
	
	/**
	 * Main map method required to read input line and map them
	 * @param key
	 * @param val
	 * @param cntxt
	 * @throws Exception
	 */
	public void map(Key key, Value val, JobContext<Key,Value> cntxt) throws Exception;

}
