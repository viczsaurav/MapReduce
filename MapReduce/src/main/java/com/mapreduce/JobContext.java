package com.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class JobContext<Key extends Comparable<Key>, Value> {
	
	final static int DEFAULT_NUM_MAPPER=1;
	final static int DEFAULT_NUM_REDUCR=1;
	
	private int numOfMap=0;
	private int numOfReduce=0;
	private String outPath;
	private String inFile;
	
	private Mapper mapperClass =null;
	private Reducer reducerClass=null;
	
	// Using Concurrent List and Concurrent Map to provide thread safety in case of multiple thread access 
	private CopyOnWriteArrayList<KeyValPair<Key,Value>> mapList = new CopyOnWriteArrayList<KeyValPair<Key,Value>>();
	private ConcurrentSkipListMap<Key, Value> redList = new ConcurrentSkipListMap<>();
	
	public JobContext(String infile, String outPath) {
		System.out.println("Setting Context..");
		this.inFile=infile;
		this.outPath=outPath;
		this.numOfMap 	 = DEFAULT_NUM_MAPPER;		// Default Mapper
		this.numOfReduce = DEFAULT_NUM_REDUCR;		// Default Reducer
	}

	/**
	 * Setting the Mapper class for JobContext
	 * @param mapper
	 */
	@SuppressWarnings("rawtypes")
	public void setMapper(Mapper mapper) {
		System.out.println("Setting Mapper..");
		this.mapperClass = mapper;
	}
	
	/**
	 * Setting the Reducer class for JobContext
	 * @param reducer
	 */
	@SuppressWarnings("rawtypes")
	public void setReducer(Reducer reducer) {
		System.out.println("Setting Reducer..");
		this.reducerClass = reducer;
	}
	/**
	 * Assuming, for our implementation, Map & Reduce are required Classes
	 * @throws Exception
	 */
	public void submit() throws Exception {
		if (null==this.mapperClass || null==this.reducerClass) {
			throw new Exception("No Mapper/Reducer Class found.. Implement and set default Mapper/Reducer..");
		}
		
		// Implement multi-threaded write
		System.out.println(this.numOfMap +" threads Spawned.. " );	
//		ExecutorService exSrvc = Executors.newFixedThreadPool(this.numOfMap);
				
		this.parseAndMap();					//Parse File and Map Values			
		this.reduceAndWrite();				//Reduce Mapped values and Write to outfile	
	}
	
	/**
	 * Read the input file, Call user map method to parse and set the values
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	private void parseAndMap() throws Exception {
		// Parse File
		
		// Step 1: Call Mapper Setup method 
		this.mapperClass.setup(this);
		
		// Step 2: Call Mapper map method for each read line
		System.out.println("Parsing input file: "+ this.inFile);
		try (BufferedReader br = new BufferedReader(new FileReader(this.inFile))) {
            String line;
            while ((line = br.readLine()) != null) {
            	this.mapperClass.map(line, null, this);
            }
        } catch (Exception e) {
        	System.out.println("Error while parsing the file: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
	}
	
	/*
	 * 
	 */
	@SuppressWarnings("unchecked")
	private void reduceAndWrite() throws Exception {
		
		Collections.sort(mapList);		// Sort and Shuffle Phase before reducing
		
		// Step 1: Call Reducer Setup method
		this.reducerClass.setup(this);
		
		// Step 2: Call Reducer reduce method
		List<Value> valList=null;
		Key lastKey=null;
		
		for (KeyValPair<Key, Value> kv : mapList) {
			if (null==lastKey) {
				valList = new ArrayList<>();
				valList.add(kv.getVal());
				lastKey=kv.getKey();
				continue;
			}
			if (kv.getKey().compareTo(lastKey)==0) {
				valList.add(kv.getVal());
			}
			else {
				this.reducerClass.reduce(lastKey, valList, this);					
				valList = new ArrayList<>();
				valList.add(kv.getVal());
				lastKey=kv.getKey();
			}
		}
		this.reducerClass.reduce(lastKey, valList, this);		// Write reduce for last element
		this.writeToFile();
	}
	
	public void map(Key key, Value val) {
		this.mapList.add(new KeyValPair<Key,Value>(key, val));
	}
	
	public void reduce(Key key, Value val) {
		this.redList.put(key,val);
	}
	
	/**
	 *  Writing to File (Overloaded Method)
	 * @throws Exception
	 */
	public void writeToFile() throws Exception {
		System.out.println("Writing to file..");
		try {
			if (null==this.outPath) {
				throw new Exception("Empty Output Path. Please enter valid output path");
			}
			this.outPath = this.outPath + File.separator+this.reducerClass.getClass().getSimpleName().toLowerCase();
			
			File file = new File(this.outPath);
			if (!file.exists())
				file.getParentFile().mkdirs();
			this.writeToFile(file);			
		} catch (Exception e) {
			System.out.println("Error while writing to file: " + e.getMessage());
			throw e;
		}
	}
	
	/**
	 * Writing the reduced values to file, sorted by Key
	 * @param file
	 * @throws Exception
	 */
	public void writeToFile(File file) throws Exception {
		try {
			System.out.println("Writing to file : "+ file.getAbsolutePath());
			PrintWriter writer = new PrintWriter(file);
			for (Map.Entry<Key, Value> p:redList.entrySet()) {
				writer.println(p.getKey() + " " + p.getValue());
			}
			writer.close();			
		}
		catch (Exception e) {
			System.out.println("Error while writing to file "+ e.getMessage());
			e.printStackTrace();
			throw e;
		}
	}
}
