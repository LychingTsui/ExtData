package com.qiguo.tv.movie.featuresCollection;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.TreeSet;

public class Pair2 {
	private int idx;
	private double score;
	
	public Pair2(int idx, double score){
		this.idx = idx;
		this.score = score;
	}
	public Pair2(String str){
		String[] pStr = str.split(":");
		this.idx = Integer.parseInt(pStr[0]);
		this.score = Double.parseDouble(pStr[1]);
	}
	public String toString(){
		return idx + ":" + score;
	}
	
	public int getIdx(){
		return idx;
	} 
	public double getScore(){
		return score;
	}
	
	// for test
	public static void main(String[] args){
		TreeSet<Pair2> p = new TreeSet<Pair2>(new Comparator<Pair2>() {
			public int compare(Pair2 p1, Pair2 p2){	
				int res = p1.getIdx() > p2.getIdx()? -1: 1;
				return res;
			}
		});
		p.add(new Pair2(1, 0.2));
		p.add(new Pair2(3, 0.4));
		p.add(new Pair2(4, 0.4));
		for(Pair2 pair2 : p){
			System.out.println(pair2.toString());
		}
		String s = "0:[12:1.0, 1051:1.0, 1552:1.0, 1160:1.0, 13245:1.0, 17498:1.0] [34557:0.458, 23061:0.528, 28602:0.216, 24491:0.156, 30571:0.081]";
		int idx = s.indexOf(" ");
		StringTokenizer sTokenizer = new StringTokenizer(s,",");
		while(sTokenizer.hasMoreTokens()){
			System.out.println(sTokenizer.nextToken());
		}
		
		
		
	}	
}
