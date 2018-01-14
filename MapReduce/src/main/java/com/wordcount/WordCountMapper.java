package com.wordcount;

import com.mapreduce.JobContext;
import com.mapreduce.KeyValPair;
import com.mapreduce.Mapper;

public class WordCountMapper implements Mapper<String, Integer>{

	private static final long serialVersionUID = 3775249923368179674L;

	public Iterable<KeyValPair<String,Integer>> map(String line,Integer val, JobContext<String,Integer> cntxt) throws Exception {
		
		String[] words = line.split(" ");
		for (String word:words) {
			cntxt.write(word, 1);
		}
		return null;
	}

}
