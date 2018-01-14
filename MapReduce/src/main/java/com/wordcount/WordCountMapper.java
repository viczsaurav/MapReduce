package com.wordcount;

import com.mapreduce.JobContext;
import com.mapreduce.Mapper;

public class WordCountMapper implements Mapper<String, Integer>{

	private static final long serialVersionUID = 3775249923368179674L;

	public void map(String line,Integer val, JobContext<String,Integer> cntxt) throws Exception {
		
		System.out.println("Calling User map method..");
		String[] words = line.split(" ");
		for (String word:words) {
			cntxt.map(word, 1);
		}
	}

}
