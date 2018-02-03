package com.qiguo.tv.movie.statistics;

import java.io.IOException;
//import java.util.Iterator;

import javax.sound.midi.MidiDevice.Info;

import org.apache.avro.data.Json;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.jasper.JasperException;
import org.codehaus.jettison.json.JSONException;

import com.cloudera.io.netty.util.internal.chmv8.ConcurrentHashMapV8.Fun;
import com.youku.tv.json.JSONObject;

public class GetDIspalyStatistics {
	public static class Mymap extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		private final static IntWritable one = new IntWritable(1);
		@Override
		protected void map(LongWritable key, Text val, Mapper<LongWritable, Text,Text,IntWritable>.Context context)throws IOException,
		InterruptedException{
			//String[] info = StringUtils.splitPreserveAllTokens(val.toString(), "\t");
			String[] info = val.toString().split("\t",-1);
			if(info.length < 27)
				return;
			if(info[25].endsWith("listDisplay")){
				if(isJson(info[27])){
					context.write(new Text(info[15]), one);
				}
			}
			
		}
		
		public boolean isJson(String str){
			
			if(str.startsWith("{")){
				try {
					JSONObject jsonObject = new JSONObject(str);
					if(jsonObject.has("ctg")){
						if(jsonObject.get("ctg").toString().startsWith("movie")){
							return true;
						}
						
					}
					else if(jsonObject.has("category")){
						if(jsonObject.get("category").toString().startsWith("movie")){
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
						if(jsonObject.get("ctg").toString().startsWith("movie")){
							return true;
						}
					}
					else if(jsonObject.has("category")){
						if(jsonObject.get("category").toString().startsWith("movie")){
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
		
	public static class myReduce extends Reducer<Text, IntWritable,Text,IntWritable>{
		@Override
		protected void reduce(Text key,Iterable<IntWritable> val,Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException,InterruptedException{
			int sum = 0;
			for(IntWritable i : val){
				sum += i.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	
	
	public static void main(String[] args) throws IOException,InterruptedException,ClassNotFoundException{
		
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		Job job = Job.getInstance(conf, "tv movie:GetDisplayEventStatistics");
		job.setJarByClass(GetDIspalyStatistics.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, Mymap.class);
		
		job.setNumReduceTasks(1);
		
		job.setReducerClass(myReduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}

