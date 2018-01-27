package com.ratings;

import com.mapreduce.JobContext;

public class RatingsMain {

	public static void main(String[] args) throws Exception{
		
		if (args.length<3) {
			throw new Exception("Arguments Required : <RatingsFile(full path)> <MovieFile(fullPath)> <outputpath>");
		}

		JobContext<Integer,Float> rateCntxt= null;
		
		String ratingsPath=null;
		String moviePath=null;
		String outPath=null;
		
		try {
			ratingsPath = args[0];
			moviePath = args[1];
			outPath = args[2];				
			
			rateCntxt = new JobContext<>(ratingsPath,outPath);
			rateCntxt.setMapper(new RatingsMapper());
			rateCntxt.setReducer(new RatingsReducer(moviePath));
			rateCntxt.submit();
			
		}
		
		catch (Exception e) {
			System.out.println("Error in Word Count Main "+ e.getMessage());
			e.printStackTrace();
		}

	}

}
