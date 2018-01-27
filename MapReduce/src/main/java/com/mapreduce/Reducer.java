package com.mapreduce;

import java.io.Serializable;

public interface Reducer<Key1 extends Comparable<Key1>, Value1, Key2 extends Comparable<Key2>, Value2> extends Serializable{
	
	/**
	 * Method required for one-time setup before reduce call for the context
	 * Added in replacement to abstract ReducerClass
	 * @param cntxt
	 * @throws Exception
	 */
	public void setup(JobContext<Key2,Value2> cntxt) throws Exception;
	
	/**
	 * Main reduce method required to read mapped values and combine to provide final output
	 * @param key
	 * @param values
	 * @param cntxt
	 */
	public void reduce(Key1 key, Iterable<Value1> values,JobContext<Key2,Value2> cntxt);
}
