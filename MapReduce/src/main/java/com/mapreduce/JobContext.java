package com.mapreduce;

import java.util.ArrayList;
import java.util.List;

public class JobContext{
	int numOfMap;
	int numOfReduce;
	String outPath;
	
	List<KeyValPair> kvList = new ArrayList<KeyValPair>();
	
	public JobContext () {
		this.outPath=null;
	}
	// For Map 
	public JobContext(String outPath, String splits) {
		System.out.println("Inside Map..");
		this.outPath=outPath;
	}
	
	public JobContext(String outPath) {
		System.out.println("Inside Reduce..");
		this.outPath=outPath;
	}
	public int getNumOfMap() {
		return numOfMap;
	}
	public void setNumOfMap(int numOfMap) {
		this.numOfMap = numOfMap;
	}
	public int getNumOfReduce() {
		return numOfReduce;
	}
	public void setNumOfReduce(int numOfReduce) {
		this.numOfReduce = numOfReduce;
	}
	public String getOutPath() {
		return outPath;
	}
	public void setOutPath(String outPath) {
		this.outPath = outPath;
	}
	
//	Making write threadsafe for multiple thread access 
	public synchronized void write(String key, float val) {
		this.kvList.add(new KeyValPair(key, val));
	}
	
	
}
