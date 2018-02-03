package com.qiguo.tv.movie.featuresCollection;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class FeatureFileReader {
	String path = "";
	public FeatureFileReader(String path){
		this.path = path;
	}
	
	public String readFeatureFile() throws FileNotFoundException,
	IOException{
		String fs = "";
		FileReader fr = new FileReader(path);
		BufferedReader bs = new BufferedReader(fr);
		String line = null;
		while((line = bs.readLine())!= null){
			fs += line.trim() + " " ;
		}
		bs.close();
		return fs;
	}
	
	//for test
	public static void main(String[] args){
		try {
			String str = new FeatureFileReader("/Users/qiguo/Documents/fearureData/type.txt").readFeatureFile();
			System.out.println(str);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		};
	}
}
