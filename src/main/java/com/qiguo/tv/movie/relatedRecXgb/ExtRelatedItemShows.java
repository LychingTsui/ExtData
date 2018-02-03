package com.qiguo.tv.movie.relatedRecXgb;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;

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
import com.youku.tv.json.JSONArray;
import com.youku.tv.json.JSONException;
import com.youku.tv.json.JSONObject;

public class ExtRelatedItemShows {
	public static class MapClass extends Mapper<LongWritable, Text, Text, IntWritable>{
		//HashSet<String> mvPairSet = new HashSet<String>();
		protected void map(LongWritable key, Text val, Context context)throws IOException,
		InterruptedException {
			
			String info[] = val.toString().split("\t", -1);
			if(info.length < 27)
				return;
			if(info[25].endsWith("listDisplay")){
				if(info[26].equals("relatedRec")){
					if(info[27].startsWith("{")){
						JSONObject js = null;
						try {
							js = new JSONObject(info[27]);
							boolean flag = false;
							if(js.has("category") && js.getString("category").startsWith("movie")){
								flag = true;
							}else if (js.has("ctg") && js.getString("ctg").startsWith("movie")) {
								flag = true;
							}
							if(flag){
								if(js.has("algGroup") && !js.get("algGroup").toString().isEmpty()){
									if(js.has("list")){
										JSONObject secjs = new JSONObject(js.getString("refVideoInfo"));
										String mvid = secjs.getString("id");
										JSONArray jsar = new JSONArray(js.get("list").toString());
										for(int i = 0; i < jsar.length(); i++){
											JSONObject subJs = jsar.getJSONObject(i);
											String disMvId = subJs.getString("id");
											context.write(new Text(disMvId + ":" + mvid), new IntWritable(1));
										}
									}
								}
							}
						} catch (JSONException e) {
							e.printStackTrace();
						}	
					}
				}		
			}
		}
		
	}
	public static class ReduceClass extends Reducer<Text, IntWritable, Text, IntWritable>{
		protected void reduce(Text key, Iterable<IntWritable>vals, Context context)throws IOException, 
		InterruptedException {
			int sum = 0;
			for(IntWritable v : vals){
				sum += v.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	
	public static void main(String[] args)throws IOException, InterruptedException,
	ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		Job job = Job.getInstance(conf, "collectStsFeats");
		
		String[] argstrs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		FileSystem dirfs = FileSystem.get(URI.create(argstrs[0]), conf);
		FileStatus fstas = dirfs.getFileStatus(new Path(argstrs[0]));
		//String format = "yyyyMMdd";  //  格式：20170731 
		int startDate = Integer.parseInt(argstrs[2]); 
		int endDate = Integer.parseInt(argstrs[3]);
		
        if(fstas.isDirectory()){
				for(FileStatus subfstus : dirfs.listStatus(new Path(argstrs[0]))){
					String subdir = subfstus.getPath().toString();
					int idx = subdir.lastIndexOf("/");
					String lastStr = subdir.substring(idx+1);
					//long someDayTime = sdf.parse(lastStr).getTime(); 
					int someday = Integer.parseInt(lastStr);
					boolean flag1 = someday - startDate >= 0 ? true : false;
					boolean flag2 = someday - endDate <= 0 ? true : false;
					if(flag1 && flag2){
						FileSystem f = FileSystem.get(URI.create(subdir), conf);
						boolean fb = f.getFileStatus(new Path(subdir)).isDirectory();
						if(fb){
							MultipleInputs.addInputPath(job, new Path(subdir+"/"), TextInputFormat.class, MapClass.class);				
						}
					}
				}		
			}
        
		job.setJarByClass(ExtRelatedItemShows.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setReducerClass(ReduceClass.class);
		
		FileOutputFormat.setOutputPath(job, new Path(argstrs[1]));
		System.exit(job.waitForCompletion(true) ? 0:1 );
	}
}
