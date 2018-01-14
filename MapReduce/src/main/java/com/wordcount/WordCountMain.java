package com.wordcount;

import com.mapreduce.JobContext;

public class WordCountMain {

	public static void main(String[] args) throws Exception {
		
		if (args.length<2) {
			throw new Exception("Arguments Required : <inFile(full path)> <outputpath> <number Of Mapper/Reducer (default 1)>>");
		}

		JobContext<String, Integer> cntxt= null;
		String inpath=null;
		String outPath=null;
		
		try {
			inpath = args[0];
			outPath = args[1];
			String numMapReduce = args[1];
			if (null!=numMapReduce)
				cntxt = new JobContext<>(inpath, outPath, Integer.parseInt(numMapReduce), Integer.parseInt(numMapReduce));
			else
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
