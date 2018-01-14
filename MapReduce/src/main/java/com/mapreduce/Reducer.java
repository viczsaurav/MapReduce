package com.mapreduce;

import java.io.Serializable;
import java.util.List;

public interface Reducer extends Serializable{
	
	public KeyValPair reducer(String key, List<String> values, JobContext cntxt);
}
