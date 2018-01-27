package com.mapreduce;

import java.io.Serializable;

public class KeyValPair<Key extends Comparable<Key>, Value > implements Serializable, Comparable<KeyValPair<Key, Value >>{


	private static final long serialVersionUID = -8876016438486040415L;
	
	private Key key;
	private Value val;
	
	public KeyValPair (Key key, Value val) {
		this.key=key;
		this.val=val;
	}
	
	public Key getKey() {
		return key;
	}

	public void setKey(Key key) {
		this.key = key;
	}

	public Value getVal() {
		return val;
	}

	public void setVal(Value val) {
		this.val = val;
	}

	public int compareTo(KeyValPair<Key,Value> othr) {
		return this.key.compareTo(othr.key);
	}
	
	
}
