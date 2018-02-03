package com.qiguo.tv.movie.statistics;

import java.io.IOException;

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

import com.youku.tv.json.JSONArray;
import com.youku.tv.json.JSONException;
import com.youku.tv.json.JSONObject;

public class RelatedRecClkTest {
	public static class MapClass extends Mapper<LongWritable, Text, Text, IntWritable>{
		private IntWritable one = new IntWritable(1);
		protected void map(LongWritable key, Text val, Context context)throws IOException,
		InterruptedException{
			String info[] = val.toString().split("\t", -1);
			if(info.length < 27)
				return;
			if(info[25].endsWith("listClick")){
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
								if(js.has("algGroup") ){
									String group = js.get("algGroup").toString();														
									String guid = js.getString("guid");
									if(guid.isEmpty()){guid = info[15];}
									String outStr = group + " " + guid;
									context.write(new Text(outStr), one);
								}
							}
						} catch (JSONException e) {
							e.printStackTrace();
						}	
					}
				}		
			}
		}
		/*
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
		}*/
	}
	
	public static class ReduceClass extends Reducer<Text, IntWritable, Text, IntWritable>{
		protected void reduce(Text key, Iterable<IntWritable>vals, Context context) throws IOException,
		InterruptedException{
			int sum = 0;
			for(IntWritable v : vals){
				sum += v.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	public static void main(String[] args)throws IOException,
	InterruptedException, ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "relatedRecClk:uv");
		job.setJarByClass(RelatedRecClkTest.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), 
				TextInputFormat.class, MapClass.class);
		job.setNumReduceTasks(1);
		job.setReducerClass(ReduceClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
	
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
