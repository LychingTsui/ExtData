package com.qiguo.tv.movie.statistics;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.youku.tv.json.JSONException;
import com.youku.tv.json.JSONObject;

public class MovieRelatedClickSts {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{
		HashMap<String, HashSet<String>> relatedmvSets = new HashMap<String, HashSet<String>>(); 
		protected void map(LongWritable key, Text val, Context context)throws IOException,
		InterruptedException{
			String info[] = val.toString().split("\t", -1);
			if(info.length < 27)
				return;
			if(info[25].endsWith("listClick")){
				if(info[26].equals("relatedRec")){
					if(isJson(info[27])){
						JSONObject js = null;
						String mvid = "";
						String clickMvId = "";
						try {
							js = new JSONObject(info[27]);
							if(js.has("videoInfo")){								
								String videoInfo = js.get("videoInfo").toString();														
								if(videoInfo != null ){									
									JSONObject subJs = new JSONObject(videoInfo);							
									if(subJs.has("id") ){
										mvid = subJs.get("id").toString();																		
									}
								}															
							}
							if(js.has("refVideoInfo")){
								String refVideo = js.get("refVideoInfo").toString();
								if(refVideo != null){
									JSONObject subJs1 = new JSONObject(refVideo);
									if(subJs1.has("id")){
										clickMvId = subJs1.get("id").toString();
										int[] res = getClickIdx(mvid, clickMvId, relatedmvSets);
										context.write(new Text(mvid), new Text(int2String(res)));
									}
								}
							}
							
						} catch (JSONException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}	
					}
				}		
			}
	
		}
		public boolean isJson(String str){
			
			if(str.startsWith("{")){
				try {
					JSONObject jsonObject = new JSONObject(str);
					if(jsonObject.has("ctg")){
						String ctg = jsonObject.get("ctg").toString();
						if(ctg.startsWith("movie")){
							return true;
						}
					}
					else if(jsonObject.has("category")){
						String ctg = jsonObject.get("category").toString();
						if(ctg.startsWith("movie")){
							return true;
						}
					}
				} catch (com.youku.tv.json.JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}				
			}
			else {
				String strTmp = "{" + str + "}";
				try {
					JSONObject jsonObject = new JSONObject(strTmp);
					if(jsonObject.has("ctg")){
						String ctg = jsonObject.get("ctg").toString();
						if(ctg.startsWith("movie")){
							return true;
						}
					}
					else if(jsonObject.has("category")){
						String ctg = jsonObject.get("category").toString();
						if(ctg.startsWith("movie")){
							return true;
						}
					}
				} catch (com.youku.tv.json.JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}	
			}
			return false;
		}
		public int[] getClickIdx(String mvid, String clickId, HashMap<String, HashSet<String>> relatedMp){
			
			int size;
			int[] res;
			if(relatedMp.containsKey(mvid)){	
				size = relatedMp.get(mvid).size();
				res = new int[size];
				HashSet<String>set = relatedMp.get(mvid);
				for(String it : set){
					int splitidx = it.indexOf(" ");
					if(it.substring(0,32).equals(clickId)){
						int idx = Integer.parseInt(it.substring(splitidx + 1).trim());
						res[idx - 1] += 1;
					}
				}				
			}else {
				size = 1;
				res = new int[size];
				res[0] = 0;
			}
			return res;
		}
		
		
		public void setup(Context context)throws IOException,InterruptedException{
			Path[] filePaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for(Path path : filePaths){
				loadData(path.toString(), context);
			}		
			super.setup(context);
		}
		public void loadData(String path, Context context)throws IOException,
		InterruptedException{
			BufferedReader bfr = new BufferedReader(new FileReader(path));
			String line = null;
			HashSet<String> set = new HashSet<String>();
			while ((line = bfr.readLine()) != null) {
				StringTokenizer stok = new StringTokenizer(line, ", \t");
				String keyMovie = stok.nextToken();
				int cnt = 1;
				set.clear();
				while (stok.hasMoreTokens()) {
					String relatedMvStr = stok.nextToken().trim();
					relatedMvStr = relatedMvStr.substring(0,32) +" "+ cnt++;
					set.add(relatedMvStr);
				}
				relatedmvSets.put(keyMovie, set);				
			}
			bfr.close();
		}
	}
	public static class ReduceClass extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> val, Context context)throws IOException,
		InterruptedException{
			int[] res = new int[30];
			for(Text v : val){
				String vStr = v.toString();
				StringTokenizer stk = new StringTokenizer(vStr, " ");
				int cnt = 0;
				while (stk.hasMoreTokens()) {
					res[cnt] += Integer.parseInt(stk.nextToken());
					cnt++;
				}
			}
			String resStr = int2String(res);
			context.write(key, new Text(resStr));
		}
		
	}
	public static String int2String(int[] res){
		String str = "";
		for(int i : res){
			str += i+" ";
		}
		return str;
	}

	public static void main(String[] args)throws IOException,InterruptedException,
	ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		Job job = Job.getInstance(conf, "movieRelated:");
		job.setJarByClass(MovieRelatedClickSts.class);
		Path cachePath = new Path(otherArgs[2]);
		 FileSystem fs = FileSystem.get(conf);
		 FileStatus[] arr = fs.listStatus(cachePath);
		 for (FileStatus fstatus : arr) {
			 Path p = fstatus.getPath();
			 if (fs.isFile(p)) {
				 job.addCacheFile(p.toUri());
			 }
		 }
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, MapClass.class);
		job.setNumReduceTasks(1);
	
		job.setReducerClass(ReduceClass.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
