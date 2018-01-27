package com.ratings;

import com.mapreduce.JobContext;

public class RatingsMain {

	public static void main(String[] args) throws Exception{
		
		if (args.length<3) {
			throw new Exception("Arguments Required : <RatingsFile(full path)> <MovieFile(fullPath)> <outputpath>");
		}

		JobContext<Integer,Float> rateCntxt= null;
		JobContext<Integer,String> movieCntxt= null;
		JobContext<String,Float> joinedCntxt= null;
		
		String ratingsPath=null;
		String moviePath=null;
		String outPath=null;
		
		try {
			ratingsPath = args[0];
			moviePath = args[1];
			outPath = args[2];				

			// Step 1:  Processing Ratings file
			rateCntxt = new JobContext<>(ratingsPath,outPath);
			rateCntxt.setMapper(new RatingsMapper());
			rateCntxt.setReducer(new RatingsReducer());
			rateCntxt.submit();
			
			// Step 2: Processing Movie
			joinedCntxt = new JobContext<>(moviePath, outPath);
			joinedCntxt.setMapper(new MovieMapper());
			joinedCntxt.setReducer(new MovieReducer());
			
		}
		
		catch (Exception e) {
			System.out.println("Error in Word Count Main "+ e.getMessage());
			e.printStackTrace();
		}

	}

}
