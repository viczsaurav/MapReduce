package com.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class JobContext<Key extends Comparable<Key>, Value> {
	
	final static int DEFAULT_NUM_MAPPER=1;
	final static int DEFAULT_NUM_REDUCR=1;
	
	int numOfMap=0;
	int numOfReduce=0;
	String outPath;
	String inFile;
	
	Mapper mapperClass =null;
	Reducer reducerClass=null;
	
	List<KeyValPair<Key,Value>> mapList = new ArrayList<KeyValPair<Key,Value>>();
	Map<Key, Value> redList = new HashMap<>();
	
	public JobContext(String infile, String outPath) {
		System.out.println("Setting Context..");
		this.inFile=infile;
		this.outPath=outPath;
		this.numOfMap 	 = DEFAULT_NUM_MAPPER;		// Default Mapper
		this.numOfReduce = DEFAULT_NUM_REDUCR;		// Default Reducer
	}

	public void setMapper(Mapper mapper) {
		System.out.println("Setting Mappeer..");
		this.mapperClass = mapper;
	}
	
	public void setReducer(Reducer reducer) {
		System.out.println("Setting Reducer..");
		this.reducerClass = reducer;
	}
	
	public void submit() throws Exception {
		this.parseAndMap();					//Parse File and Map Values
		this.reduceAndWrite();				//Reduce Mapped values and Write to outfile
	}
	public void parseAndMap() throws Exception {
		// Parse File
		System.out.println("Parsing input file: "+ this.inFile);
		try (BufferedReader br = new BufferedReader(new FileReader(this.inFile))) {
            String line;
            while ((line = br.readLine()) != null) {
            	this.mapperClass.map(line, 1, this);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
	
	public void reduceAndWrite() throws Exception {
		
		Collections.sort(mapList);
		
		List<Value> valList=null;
		Key lastKey=null;
		
		for (KeyValPair<Key, Value> kv : mapList) {
			if (lastKey.compareTo(kv.getKey())==0) {
				valList.add(kv.getVal());
			}
			else {
				if (null!=lastKey) {
					this.reducerClass.reduce(kv.getKey(), Arrays.asList(valList), this);					
				}
				valList = new ArrayList<>();
				valList.add(kv.getVal());
				lastKey=kv.getKey();
			}
		}
		this.writeToFile();
	}
	
	public void write(Key key, Value val) {
		if (this instanceof Mapper) {
			this.mapList.add(new KeyValPair<Key,Value>(key, val));
			System.out.println("Mapping Values..");
		}
		if (this instanceof Reducer) {
			this.redList.put(key,val);
			System.out.println("Reducing Values..");
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
				for (Map.Entry<Key, Value> p:redList.entrySet()) {
					writer.println(p.getKey() + " " + p.getValue());
				}
				writer.close();
			}
			else {
				// Implement multi-threaded write
				System.out.println("Multiple threads writing");
				
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
