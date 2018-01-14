package com.mapreduce;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class JobContext<Key extends Comparable<Key>, Value> {
	
	final static int DEFAULT_NUM_MAPPER=1;
	final static int DEFAULT_NUM_REDUCR=1;
	
	int numOfMap=0;
	int numOfReduce=0;
	String outPath;
	String inFile;
	
	Class mapperClass =null;
	Class reducerClass=null;
	
	List<KeyValPair<Key,Value>> mapList = new ArrayList<KeyValPair<Key,Value>>();
	List<KeyValPair<Key,Value>> redList = new ArrayList<KeyValPair<Key,Value>>();
	
	public JobContext () {
		this.outPath=null;
	}
	
	public JobContext(String infile, String outPath) {
		System.out.println("Setting Context..");
		this.inFile=infile;
		this.outPath=outPath;
		this.numOfMap 	 = DEFAULT_NUM_MAPPER;		// Default Mapper
		this.numOfReduce = DEFAULT_NUM_REDUCR;		// Default Reducer
	}
	
	public JobContext(String infile, String outPath, int numOfMap, int numOfReduce) {
		System.out.println("Setting Context..");
		this.inFile=infile;
		this.outPath=outPath;
		this.numOfMap 	 = (DEFAULT_NUM_MAPPER > numOfMap) 		? DEFAULT_NUM_MAPPER:numOfMap;		// Default have 1 Mapper
		this.numOfReduce = (DEFAULT_NUM_REDUCR > numOfReduce) 	? DEFAULT_NUM_REDUCR:numOfReduce;		// Default have 1 Reducer
	}

	public void setMapper(Class mapper) {
		this.mapperClass = mapper;
	}
	
	public void setReducer(Class reducer) {
		this.reducerClass = reducer;
	}
	public void parseInFile() {
		// Parse File
		// Call user defined Map Method
	}
	
	public void write(Key key, Value val) {
		if (this instanceof Mapper) {
			this.mapList.add(new KeyValPair<Key,Value>(key, val));			
		}
		if (this instanceof Reducer) {
			this.redList.add(new KeyValPair<Key, Value>(key, val));
		}
	}
	
	// Writing to File (Overloaded Method)
	public void writeToFile() throws Exception {
		if (null==this.outPath) {
			System.out.println("Empty Output Path");
			return;
		}

		else if (this instanceof Reducer) {
			outPath = outPath + File.pathSeparator+"reduce";
		}
		File file = new File(this.outPath);
		if (!file.exists())
			file.getParentFile().mkdirs();
		this.writeToFile(file);
	}
	
	public void writeToFile(File file) throws Exception {
		try {
			if (this.numOfMap==DEFAULT_NUM_MAPPER) {
				PrintWriter writer = new PrintWriter(file);
				for (KeyValPair<Key,Value> p:redList) {
					writer.println(p.getKey() + " " + p.getVal());
				}
				writer.close();
			}
			else {
				// Implement multi-threaded write
				System.out.println("Multiple threads writing");
				Collections.sort(redList);
//				ExecutorService exSrvc = Executors.newFixedThreadPool(this.numOfMap);
				
//				exSrvc.execute(command);
			}
			
		}
		catch (Exception e) {
			System.out.println("Error while writing to file "+ e.getMessage());
			e.printStackTrace();
		}
	}
}
