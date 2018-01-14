package com.wordcount;

import com.mapreduce.JobContext;

public class WordCountMain {

	public static void main(String[] args) throws Exception {
		
		if (args.length<2) {
			throw new Exception("Arguments Required : <inFile(full path)> <outputpath>");
		}

		JobContext<String, Integer> cntxt= null;
		String inpath=null;
		String outPath=null;
		
		try {
			if (args.length==2) {
				inpath = args[0];
				outPath = args[1];				
			} else {
				
			}

			cntxt = new JobContext<>(inpath,outPath);
			cntxt.setMapper(new WordCountMapper());
			cntxt.setReducer(new WordCountReducer());
			cntxt.submit();
			
		}
		
		catch (Exception e) {
			System.out.println("Error in Word Count Main "+ e.getMessage());
			e.printStackTrace();
		}
			
	}

}
