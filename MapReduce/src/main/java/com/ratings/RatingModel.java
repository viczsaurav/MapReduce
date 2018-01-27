package com.ratings;

import java.io.Serializable;

public class RatingModel implements Serializable{
	
	private static final long serialVersionUID = 7100980906839613936L;
	
	private int user_id;
	private int movie_id;
	private float rating;
	
	public RatingModel(int userId, int movieId, float rating) {
		this.user_id=userId;
		this.movie_id = movieId;
		this.rating = rating;
	}

	public int getUserId() {
		return this.user_id;
	}

	public void setUserId(int userId) {
		this.user_id = userId;
	}

	public int getMovieId() {
		return this.movie_id;
	}

	public void setMovieId(int movieId) {
		this.movie_id = movieId;
	}

	public float getRating() {
		return rating;
	}

	public void setRating(float rating) {
		this.rating = rating;
	}
	

}
