package com.wordcount;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

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
			
			// Setting Mapper
			WordCountMapper wcMap = new WordCountMapper();
			try (BufferedReader br = new BufferedReader(new FileReader(inpath))) {
	            String line;
	            while ((line = br.readLine()) != null) {
	            	wcMap.map(line, 1, cntxt);
	            }
	        } catch (IOException e) {
	            e.printStackTrace();
	        }

			// Setting Reducer
			WordCountReducer wcReducer = new WordCountReducer();
//			wcReducer.reducer(word, values, cntxt);
			
		}
		
		catch (Exception e) {
			System.out.println("Error in Word Count Main "+ e.getMessage());
			e.printStackTrace();
		}
			
	}

}
