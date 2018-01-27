package com.ratings;

import com.google.gson.Gson;
import com.mapreduce.JobContext;
import com.mapreduce.Mapper;

public class RatingsMapper implements Mapper<String, Float>{

	private static final long serialVersionUID = 164574148732313037L;

	@Override
	public void map(String jsonLine, Float val, JobContext<String, Float> rateCntxt) throws Exception {
		
		System.out.println("Calling Ratings map method..");
		
		Gson g = new Gson();
		RatingModel rateObj = g.fromJson(jsonLine, RatingModel.class);
		System.out.println(rateObj.getMovieId() + ", " + rateObj.getRating());
		rateCntxt.map(String.valueOf(rateObj.getMovieId()), new Float(rateObj.getRating()));	
		
	}

	@Override
	public void setup(JobContext<String, Float> cntxt) throws Exception {
		// TODO Auto-generated method stub
		
	}

}
