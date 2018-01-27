package com.ratings;

import com.mapreduce.JobContext;
import com.mapreduce.Reducer;

public class RatingsReducer implements Reducer<String, Float, Integer, Float>{

	
	private static final long serialVersionUID = -7911259339855195995L;

	@Override
	public void setup(JobContext<Integer, Float> cntxt) throws Exception {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void reduce(String movieId, Iterable<Float> values, JobContext<Integer, Float> cntxt) {
		
		float totalRate=0f;
		int rateCnt =0;
		System.out.println("ID : "+ movieId);
		for (Float i :values) {
			
			totalRate += i.floatValue();
			rateCnt++;
		}
		float finalRate = totalRate / rateCnt ;
		System.out.println("Id: " + movieId + ", Rating: "+ finalRate);
		cntxt.reduce(new Integer(movieId), new Float(finalRate));
	}

}
