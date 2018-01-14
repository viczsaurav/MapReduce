package com.mapreduce;

import java.io.Serializable;

public interface Mapper<Key extends Comparable<Key>, Value> extends Serializable {
	
	public void map(Key key, Value val, JobContext<Key,Value> cntxt) throws Exception;

}
