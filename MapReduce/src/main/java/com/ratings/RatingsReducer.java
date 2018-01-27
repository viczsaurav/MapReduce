package com.ratings;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;
import com.mapreduce.JobContext;
import com.mapreduce.Reducer;

public class RatingsReducer implements Reducer<String, Float, String, Float>{

	
	private static final long serialVersionUID = -7911259339855195995L;

	private static Map<String, String> movieMap = new HashMap<String, String>();
	
	/**
	 * Contructor to cache 
	 * @param moviePath
	 * @throws Exception
	 */
	public RatingsReducer(String moviePath) throws Exception {
		System.out.println("Caching file: " + moviePath);
		Gson g = new Gson();
		MovieModel mObj=null;
		try (BufferedReader br = new BufferedReader(new FileReader(moviePath))) {
			String line;
			while ((line = br.readLine()) != null) {
				mObj = g.fromJson(line, MovieModel.class);
				movieMap.put(String.valueOf(mObj.getId()), mObj.getName());
			}
		} catch (Exception e) {
			System.out.println("Error while parsing the file: " + e.getMessage());
			e.printStackTrace();
			throw e;
		}		
	}
	@Override
	public void reduce(String movieId, Iterable<Float> values, JobContext<String, Float> cntxt) {
		
		float totalRate=0f;
		int rateCnt =0;
		for (Float i :values) {
			
			totalRate += i.floatValue();
			rateCnt++;
		}
		float finalRate = totalRate / rateCnt ;		
		cntxt.reduce(movieMap.get(movieId), new Float(finalRate));
	}

}
