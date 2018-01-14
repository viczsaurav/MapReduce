package com.wordcount;

import com.mapreduce.JobContext;
import com.mapreduce.KeyValPair;
import com.mapreduce.Reducer;

public class WordCountReducer implements Reducer<String, Integer, String, Integer> {

	private static final long serialVersionUID = -3904724828336643593L;

	public KeyValPair<String,Integer> reducer(String word, Iterable<Integer> values, JobContext<String,Integer> cntxt) {
		
		int totalVal=0;
		
		for (Integer i :values) {
			totalVal+=i;
		}
		
		cntxt.write(word, new Integer(totalVal));
		return null;
	}

}
