package com.mapreduce;

import java.io.Serializable;

public interface Reducer<Key1 extends Comparable<Key1>, Value1, Key2 extends Comparable<Key2>, Value2> extends Serializable{
	
	public KeyValPair<Key2,Value2> reducer(Key1 key, Iterable<Value1> values,JobContext<Key2,Value2> cntxt);
}
