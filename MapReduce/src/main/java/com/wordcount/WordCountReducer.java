package com.wordcount;

import java.util.List;

import com.mapreduce.JobContext;
import com.mapreduce.KeyValPair;
import com.mapreduce.Reducer;

public class WordCountReducer implements Reducer {

	public KeyValPair reducer(String key, List<String> values, JobContext cntxt) {
		// TODO Auto-generated method stub
		return null;
	}

}
