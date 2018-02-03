package com.youku.tv.showperson;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map.Entry;

public class GetDataFromLogs {
	public static void VVMap(String input,String output)throws IOException,FileNotFoundException{
		BufferedReader reader=new BufferedReader(new FileReader(new File(input)));
		BufferedWriter writer= new BufferedWriter(new FileWriter(new File(output)));
		String line="";
		while ((line=reader.readLine())!=null) {
			String info[]=line.split("\t");
			String uid=info[0];
			for (int i = 1; i < info.length; i++) {
				if (info[i].length()<20) {
					continue;
				}
				String []data=info[i].split(",");
				String []temp=data[3].split(":");
				String vidlen=data[1],ts=data[2],date=temp[0],times=temp[1];
				if (vidlen.length()>15) {
					continue;
				}
				String[]episode=data[0].split(":");
				String showid=episode[0];
				StringBuffer sBuffer=new StringBuffer();
				for (int j = 1; j < episode.length; j++) {
					sBuffer.append("\2").append(episode[j].split("_")[1]);
				}
				if (sBuffer.length()==0) {
					sBuffer.append("\2").append("1");
				}
				writer.write(uid+"\t"+"vv" + showid + "," + sBuffer.substring(1) + "," + vidlen + "," + ts
						+ "," + date+"\n");
				writer.flush();
			}

		}
	}
	public static void clickMap(String input,String output)throws IOException,FileNotFoundException{
		BufferedReader reader=new BufferedReader(new FileReader(new File(input)));
		BufferedWriter writer= new BufferedWriter(new FileWriter(new File(output),true));
		String line="";
		while ((line=reader.readLine())!=null) {
			String info[]=line.split("\t");
			String uid=info[0];
			for (int i = 1; i < info.length; i++) {
				String[]data=info[i].split(",");
				String temp[]=data[1].split(":");
				String showid=data[0],date=temp[0];
				try {
					Integer.valueOf(date);
					writer.write(uid+"\t"+"click" + showid + "," + date+"\n");
					writer.flush();
				} catch (Exception e) {
					// TODO: handle exception
				}
			}			
		}
	}
	public static String MergeEpisode(String epi1, String epi2) {
		String[] episode;
		ArrayList<Integer> list = new ArrayList<Integer>();
		int n=0;
		if (epi1.length() > 0) {
			episode =epi1.split("\2");
			for (int j = 0; j < episode.length; j++) {
				n=Integer.valueOf(episode[j]);
				if (!list.contains(n)) {
					list.add(n);
				}					
			}
		}

		if (epi2.length() > 0) {
			episode = epi2.split("\2");
			for (int j = 0; j < episode.length; j++) {
				n=Integer.valueOf(episode[j]);
				if (!list.contains(n)) {
					list.add(n);
				}					
			}
		}

		Collections.sort(list);
		StringBuffer sBuffer = new StringBuffer();
		for (int j = 0; j < list.size(); j++) {
			sBuffer.append("\2").append(list.get(j));
		}
		return sBuffer.substring(1);
	}
	public static void reduce(String input,String output)throws IOException, FileNotFoundException{
		StringBuffer clickBuffer = new StringBuffer();
		StringBuffer vvBuffer = new StringBuffer();
		HashMap<String, String> vvshow = new HashMap<String,String>();
		HashMap<String, Integer> clickshow = new HashMap<String,Integer>();
		HashMap<String, ArrayList<String>>map=new HashMap<String,ArrayList<String>>();
		BufferedReader reader=new BufferedReader(new FileReader(new File(input)));
		BufferedWriter writer=new BufferedWriter(new FileWriter(new File(output)));
		String line="";
		while ((line=reader.readLine())!=null) {
			String str[]=line.split("\t");
			if (map.containsKey(str[0])) {
				ArrayList<String>list=map.get(str[0]);
				list.add(str[1]);
				map.put(str[0], list);
			}
			else {
				ArrayList<String>list=new ArrayList<String>();
				list.add(str[1]);
				map.put(str[0], list);
			}
		}
		for (Entry<String, ArrayList<String>>maps:map.entrySet()) {
			String key=maps.getKey();
			ArrayList<String>list=maps.getValue();
			for (String info : list) {
				if (info.startsWith("vv")) {
					String[] data =info.substring(2).split(","); 
							//StringUtils.split(info.substring(2), ',');
					String showid = data[0];
					String epi = data[1];
					long vidlen = Long.valueOf(data[2]);
					long ts = Long.valueOf(data[3]);
					int date = Integer.valueOf(data[4]);
					if (vvshow.containsKey(showid)) {
						String[] temp =vvshow.get(showid).split(":");
								//StringUtils.split(vvshow.get(showid), ':');
						epi = epi + "\2" + temp[0];
						vidlen += Long.valueOf(temp[1]);
						ts += Long.valueOf(temp[2]);
						if (date < Integer.valueOf(temp[3])) {
							date = Integer.valueOf(temp[3]);
						}
					}
					epi = MergeEpisode(epi, "");
					vvshow.put(showid, epi + ":" + vidlen + ":" + ts + ":" + date);
					System.out.println(showid+":::"+ epi + ":" + vidlen + ":" + ts + ":" + date);

				} else {
					String[] data = info.substring(5).split(",");
							//StringUtils.split(info.substring(5), ',');
					String showid = data[0];
					int date = Integer.valueOf(data[1]);
					if (clickshow.containsKey(showid)) {
						if (date < clickshow.get(showid)) {
							date = clickshow.get(showid);
						}
					}
					clickshow.put(showid, date);
				}
			}

			StringBuffer sBuffer = new StringBuffer();
			Object[] objs = vvshow.keySet().toArray();
			for (int i = 0; i < objs.length; i++) {
				sBuffer.append(",");
				sBuffer.append(objs[i].toString()).append(":")
						.append(vvshow.get(objs[i].toString()));
			}

			StringBuffer sBufferA = new StringBuffer();
			objs = clickshow.keySet().toArray();
			for (int i = 0; i < objs.length; i++) {
				sBufferA.append(",");
				sBufferA.append(objs[i].toString()).append(":")
						.append(clickshow.get(objs[i].toString()));
			}

			String val = "vv:";
			if (sBuffer.length() > 1) {
				val = val + sBuffer.substring(1);
			}
			val = val + "\tclick:";
			if (sBufferA.length() > 1) {
				val = val + sBufferA.substring(1);
			}
			writer.write(key+"\t"+val+"\n");
			writer.flush();
		}
	}
	public static void main(String[] args) throws FileNotFoundException, IOException {
		GetDataFromLogs.VVMap("/Users/Metro/Documents/temp/vv_xq.txt", "/Users/Metro/Documents/temp/xq_temp.txt");
		//GetDataFromLogs.clickMap("/Users/Metro/Documents/temp/click_xq.txt", "/Users/Metro/Documents/temp/xq_temp.txt");
		GetDataFromLogs.reduce("/Users/Metro/Documents/temp/xq_temp.txt", "/Users/Metro/Documents/temp/xq_log.txt");
	}
}


