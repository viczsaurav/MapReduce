package com.mapreduce;

import java.io.Serializable;

public interface Mapper extends Serializable {
	
	public Iterable<KeyValPair> map(String line, JobContext cntxt) throws Exception;

}
