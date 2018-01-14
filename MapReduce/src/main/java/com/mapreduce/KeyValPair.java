package com.mapreduce;

import java.io.Serializable;

public class KeyValPair implements Serializable{


	private static final long serialVersionUID = -8876016438486040415L;
	
	private String key;
	private float val=0.0f;
	
	public KeyValPair (String key, float val) {
		this.key=key;
		this.val=val;
	}
	
	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public float getVal() {
		return val;
	}

	public void setVal(float val) {
		this.val = val;
	}

	public int compareTo(KeyValPair other) {
		return this.key.compareToIgnoreCase(other.key);
	}
	
	
}
