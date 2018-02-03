package com.qiguo.tv.movie.card;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.StringTokenizer;

public class ExtCardTagsFeaturesDesc {
	public static HashMap<String, Integer> topTagkey2IdMap(HashSet<String> set){
		int id = 1;
		HashMap<String, Integer>key2IdMp = new HashMap<String, Integer>();
		for(String key : set){
			key2IdMp.put(key, id++);
		}
		return key2IdMp;
	}
	/**
	 * 对排序后的cardId－key－freq 文件取前thres个
	 **/
	public static HashSet<String> extCardTagsTopKeys(String path)throws IOException{
		BufferedReader bfr = new BufferedReader(new FileReader(path));
		String line = "";
		String cardid = "";
		HashSet<String> cardTagSet = new HashSet<String>();
		int thres = 10;
		int cnt = 0;
		while((line = bfr.readLine()) != null){
			StringTokenizer stk = new StringTokenizer(line);
			String tmpid = stk.nextToken();
			String key = stk.nextToken();
			if(!tmpid.equals(cardid)){
				cardid = tmpid;
				cnt = 1;
				cardTagSet.add(key);
			}else {
				if(cnt <= thres){
					cnt++;
					cardTagSet.add(key);
				}
			}
		}
		bfr.close();
		return cardTagSet;
	}
	
	public static void extCardTagsFeatures(String inputpath, String outputpath, HashMap<String, Integer>key2IdMp)throws IOException{
		BufferedReader bfr = new BufferedReader(new FileReader(inputpath));
		String line = "";
		String cardid = "";
		String out = "";
		BufferedWriter bfw = new BufferedWriter(new FileWriter(outputpath));
		int thres = 10;
		int cnt = 0;
		while((line = bfr.readLine()) != null){
			StringTokenizer stk = new StringTokenizer(line);
			String tmpid = stk.nextToken();
			String key = stk.nextToken();
			if(!tmpid.equals(cardid)){
				writeFile(bfw, out);
				cardid = tmpid;
				out = "";
				out += cardid + "\t";
				if(key2IdMp.containsKey(key)){
					out += key2IdMp.get(key)+":1.0 ";
				}
				cnt = 1;
			}else {
				if(cnt <= thres){
					cnt++;
					if (key2IdMp.containsKey(key)) {
						double res = 1.0 - ((cnt-1)*0.05);
						out += key2IdMp.get(key) + ":" + res + " ";
					}
				}
			}
		}
		bfr.close();
		bfw.close();
	}
	public static void writeFile(BufferedWriter bfw, String str)throws IOException{
		bfw.write(str);
		bfw.flush();
		bfw.newLine();
	}
	public static void writekey(HashMap<String, Integer>mp, String outPath)throws IOException{
		BufferedWriter bfw = new BufferedWriter(new FileWriter(outPath));
		for(Map.Entry<String, Integer> entry : mp.entrySet()){
			bfw.write(entry.getKey() + "\t" + entry.getValue());
			bfw.flush();
			bfw.newLine();
		}
		bfw.close();
	}
	public static void main(String[] args)throws IOException{
		String input = args[0]; //排好序cardid－key－频数 文件
		HashSet<String> cardTagsSet = extCardTagsTopKeys(input);
		HashMap<String, Integer>topTagskey2IdMp = topTagkey2IdMap(cardTagsSet);
		
		String output = args[1]; //输出 cardid：features
		String outpath2 = args[2]; // 输出key－id文件，便于查询对应的key和对应id
		extCardTagsFeatures(input, output, topTagskey2IdMp); //
		writekey(topTagskey2IdMp, outpath2);
	}
}
