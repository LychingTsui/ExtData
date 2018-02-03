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

import com.qiguo.tv.movie.statistics.ComicRelatedClickUvSts.MapClass;
import com.qiguo.tv.movie.statistics.ComicRelatedClickUvSts.ReduceClass;
import com.youku.tv.json.JSONException;
import com.youku.tv.json.JSONObject;

public class JilupianRelatedClickUvSts {
	public static class MapClass extends Mapper<LongWritable, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		public void map(LongWritable key, Text val, Context context)throws IOException,
		InterruptedException{
			String info[] = val.toString().split("\t", -1);
			if(info.length < 27)
				return;
			if(info[25].endsWith("listClick")){
				if(info[26].equals("relatedRec")){
					if(isJson(info[27])){
						JSONObject js = null;
						try {
							js = new JSONObject(info[27]);
							if(js.has("videoInfo")){
								String videoInfo = js.get("videoInfo").toString();
								String guid = info[15];
								JSONObject subJs = new JSONObject(videoInfo);
								if(subJs.has("id")){
									String mvid = subJs.getString("id").toString();									
									String text = guid + " "+ mvid;
									context.write(new Text(text), one);
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
						if(ctg.startsWith("jilupian")){
							return true;
						}
					}
					else if(jsonObject.has("category")){
						String ctg = jsonObject.get("category").toString();
						if(ctg.startsWith("jilupian")){
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
						if(ctg.startsWith("jilupian")){
							return true;
						}
					}
					else if(jsonObject.has("category")){
						String ctg = jsonObject.get("category").toString();
						if(ctg.startsWith("jilupian")){
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
		
	}
	public static class ReduceClass extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		public void reduce(Text key, Iterable<IntWritable> val, Context context)throws IOException,
		InterruptedException{
			int sum = 0;
			for(IntWritable i : val){
				sum += i.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	
	public static void main(String[] args)throws IOException,
	InterruptedException,ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		Job job = Job.getInstance(conf, "jilupClickRelated:uv");
		job.setJarByClass(JilupianRelatedClickUvSts.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, MapClass.class);
		job.setNumReduceTasks(1);
	
		job.setReducerClass(ReduceClass.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
	
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
