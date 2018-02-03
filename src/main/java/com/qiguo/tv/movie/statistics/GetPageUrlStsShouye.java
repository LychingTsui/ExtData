package com.qiguo.tv.movie.statistics;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.youku.tv.json.JSONObject;

import PersonalRecommend.T2;

public class GetPageUrlStsShouye {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{
		private static IntWritable one = new IntWritable(1);
		protected void map(LongWritable key, Text val, Context context)throws IOException,
		InterruptedException{
			String ver = "";
			
			String[] infoStr = val.toString().split("\t", -1);
			if(infoStr.length < 27){
				return;
			}
			if(infoStr[27].startsWith("{")){
				if(infoStr[25].startsWith("/首页/浏览")){
					try {
						JSONObject json = new JSONObject(infoStr[27]);
						String guid = infoStr[15];
						if(guid.isEmpty()){
							guid = json.getString("guid");
						}
						if(json.has("info")){
							String jsinfo = json.getString("info");
						
							if(jsinfo.startsWith("{")){
								JSONObject subjs = new JSONObject(jsinfo);
								if(subjs.has("actionItem")){
									JSONObject  js2 = new JSONObject(subjs.get("actionItem").toString());
									if(js2.has("webUrl") && !js2.isNull("webUrl")){
										String url = js2.getString("webUrl");
										//System.out.println(url);
										ver = getVer(infoStr);
										if(ver.startsWith("5") 
												&& !url.contains("tvall.cn")
												&& !url.contains("tvapk.net")
												&& !url.startsWith("file:///")
												&& !url.contains("qiguo")){ 
										
											String urlstr = url +":"+ one;
											context.write(new Text(guid), new Text(urlstr));
										}
									}
								}
							}
						}
					
					} catch (Exception e) {
						// TODO: handle exception
						e.printStackTrace();
					}
				}
			}
		}
		public String getVer(String[] strs){
			String vsr ="";
			for(int i = 0; i< strs.length; i++){
				if(strs[i].startsWith("电视家") || strs[i].startsWith("電視家")){
					for(int j = i+1; j<strs.length; j++){
						if(!strs[j].trim().isEmpty() ){
							vsr = strs[j];
							break;
						}
					}
				}
			}
			return vsr;
		}
	}
	
	public static class ReduceClass extends Reducer<Text, Text, Text, Text>{
		protected void reduce(Text key, Iterable<Text>val, Context context)throws IOException,
		InterruptedException{
			
			HashMap<String, Integer> urlSet = new HashMap<String, Integer>();
			for(Text v : val){
				int idx = v.toString().lastIndexOf(":");
				String url = v.toString().substring(0,idx);
				int cum = Integer.parseInt(v.toString().substring(idx+1));
				if(urlSet.containsKey(url)){
					cum = cum + urlSet.get(url);
					urlSet.put(url, cum);
				}else {
					urlSet.put(url, cum);
				}
			}
			TreeSet<T2> set = new TreeSet<T2>(new Comparator<T2>() {
				public int compare(T2 p1, T2 p2){
	                int res = p1.getVal() > p2.getVal()? -1: 1;     ///> ：降序  < :  升序
	                return res;
	            }
			});
			String out = "";
			for(Map.Entry<String, Integer>entry : urlSet.entrySet()){
				set.add(new T2(entry.getKey(), entry.getValue()));
			}
			for(T2 t2 : set){
				out += t2.getKey()+":"+ t2.getVal().toString() +"\t";
				
			}
			context.write(key, new Text(out)); 
		}
	}
	
	public static void main(String[] args)throws IOException,
	InterruptedException,ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		Job job = Job.getInstance(conf, "url");
		job.setJarByClass(GetPageUrlStsShouye.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, 
				MapClass.class);
		job.setNumReduceTasks(1);
		job.setReducerClass(ReduceClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}
