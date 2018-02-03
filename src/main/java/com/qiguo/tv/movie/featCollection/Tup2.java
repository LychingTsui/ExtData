package com.qiguo.tv.movie.featCollection;

import java.util.ArrayList;

public class Tup2 {
	int idx = 0;
	Double val = 0.0;
	public Tup2(int id, Double v){
		this.idx = id;
		this.val = v;
	}
	public Tup2(String str){
		String[] sts = str.split(":");
		this.idx = Integer.valueOf(sts[0]);
		this.val = Double.valueOf(sts[1]);
	}
	public int getIdx(){
		return idx;
	}
	public double getVal(){
		return val;
	}
	public String toString(){
		return idx + ":"+ val; 
	}
	
	public static void main(String[] args){
		
		ArrayList<Tup2> test = new ArrayList<Tup2>();
		test.add(new Tup2(3, 1.0));
		test.add(new Tup2(4, 1.0));
		//System.out.println(test.toString());
		String te = "B8:FC:9A:0C:39:EB.C8:0E:77:E3:F4:EE.deviceId.serialNumber.93371642 0:[14:1.0, 13:1.0, 8974:1.0, 15667:1.0, 17498:1.0] [32629:0.477, 32980:0.451, 36401:0.301, 33240:0.301, 34884:0.285, 30614:0.494, 28238:0.449, 26440:0.337, 20221:0.283, 30535:0.224, 24205:0.172, 28266:0.165, 27053:0.139, 29503:0.102, 25512:0.097, 21599:0.097, 30378:0.078, 20303:0.077, 28053:0.074, 23438:0.063, 20497:0.063, 30759:0.033, 23026:0.031, 24242:0.027]";
		String[] strs = te.split(" ",-1);
		System.out.println(strs[1].length());
		for(String s: strs){
			System.out.println(s);
		}
		
	}
	
}
