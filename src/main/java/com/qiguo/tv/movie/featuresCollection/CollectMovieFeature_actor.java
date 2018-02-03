package com.qiguo.tv.movie.featuresCollection;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
/**
 *@info：收集电影信息的前三位actor作为key
 **/
public class CollectMovieFeature_actor {
	public static class MapClass extends Mapper<LongWritable, Text, Text, NullWritable>{
		HashSet<String> expActSet = new HashSet<String>();
		protected void map(LongWritable key, Text value, Context context) throws IOException,
		InterruptedException{
			
			String[] valString = value.toString().split("\t", -1);
			for(String s : valString){
				if(s.startsWith("actor")){
					if(s.length() > 6){
						String actor = s.substring(6);
						StringTokenizer stk = new StringTokenizer(actor,",");
						int cutOff = 3;
						int cnt = 0;
						while(stk.hasMoreTokens() && cnt < cutOff){
							String actStr = stk.nextToken().trim();
							if(!expActSet.contains(actStr)){
								context.write(new Text(actStr), NullWritable.get());
								cnt++;
							}
						}											
					}
				}
			}
		}
	}
	
	public static class ReduceClass extends Reducer<Text, NullWritable, Text, NullWritable>{
		protected void reduce(Text key, Iterable<NullWritable>val,Context context)throws IOException,
		InterruptedException {
			String actStr = "t1" + key.toString();
			context.write(new Text(actStr), NullWritable.get());
		}
	}
	public static void main(String[] args)throws IOException,InterruptedException,
	ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "30");
		
		Job job = Job.getInstance(conf,"actor");
		String[] argsStr = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		MultipleInputs.addInputPath(job, new Path(argsStr[0]), TextInputFormat.class,
				MapClass.class);
		
		job.setJarByClass(CollectMovieFeature_actor.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setReducerClass(ReduceClass.class);
		FileOutputFormat.setOutputPath(job, new Path(argsStr[1]));
		System.exit(job.waitForCompletion(true)? 0 : 1);
	}

	
}
