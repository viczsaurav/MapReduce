package com.wordcount;

import com.mapreduce.JobContext;
import com.mapreduce.Reducer;

public class WordCountReducer implements Reducer<String, Integer, String, Integer> {

	private static final long serialVersionUID = -3904724828336643593L;


	@Override
	public void setup(JobContext<String, Integer> cntxt) throws Exception {
		// TODO Auto-generated method stub
		
	}
	
	public void reduce(String word, Iterable<Integer> values, JobContext<String,Integer> cntxt) {
		int totalVal=0;
		
		for (Integer i :values) {
			totalVal+=i.intValue();
		}		
		cntxt.reduce(word, new Integer(totalVal));
	}
}
