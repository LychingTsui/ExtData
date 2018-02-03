package com.qiguo.tv.movie.featuresCollection;

public class OneHot {
	private String[] featSets = null;
	
	public OneHot(String[] featureSet){
		this.featSets = featureSet;
	}
	public OneHot(String str){
		if(str.contains(",")){    // 分隔符为 ，
			String[] strs = str.split(",");
			this.featSets = strs;
		}
		else if(str.contains(" ")){   //  分隔符为 空格
			String[] strs = str.split(" ");
			this.featSets = strs;	
		}	
	}
	
	public int[] oneHotCode(String feature){
		int setLen = featSets.length;
		//System.out.println(setLen);
		int[] code = new int[setLen];		
		String[] fs = feature.split(",");		
		for(String s : fs){
			for(int i = 0; i < setLen; i++){
				if(s.trim().equals(featSets[i].trim())){
					code[i] += 1;
				}
			}
		}
		return code;
		
		/*
		byte[] res = new byte[setLen];
		for(String s: fs){
			for(int i = 0; i<setLen; i++){
				if(s.trim().equals(featSets[i].trim())){
					res[i] = 1;
				}
			}
		}	
		return res;	
     */
	}
	
}
