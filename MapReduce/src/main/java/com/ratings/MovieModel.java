package com.ratings;

import java.io.Serializable;

public class MovieModel implements Serializable{

	private static final long serialVersionUID = 7344421477674996912L;
	
	private int id;
	private String name;
	
	public MovieModel(int id,String name) {
		this.id=id;
		this.name=name;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	public boolean equals(MovieModel m1, MovieModel m2) {
		return m1.getId()==m2.getId();
	}

}
