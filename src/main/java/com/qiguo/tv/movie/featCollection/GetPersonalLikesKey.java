package com.qiguo.tv.movie.featCollection;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 修改时间08／28
 * @info: 获得用户喜好的特征key  /user/tvapk/peichao/personas/userlabel/*
 * 		  setp1 :likesFeatures 提取 
 **/

public class GetPersonalLikesKey {
	public static class MapClass extends Mapper<LongWritable, Text, Text, NullWritable>{
		static int cutOffNum = 25;
		protected void map(LongWritable key, Text val, Context context)throws IOException,
		InterruptedException{
			StringTokenizer sTok = new StringTokenizer(val.toString(),"\t");
			sTok.nextToken();  //uid
			while (sTok.hasMoreTokens()) {
				String likes = sTok.nextToken().trim();				
				if(likes.startsWith("label")){
					if(likes.length() > 5){
						String subStr = likes.substring(5);
						StringTokenizer stk = new StringTokenizer(subStr,",");
						int cutOff = cutOffNum;
						int cnt = 0;
						while (stk.hasMoreTokens() && cnt < cutOff) {
							String p = stk.nextToken().trim();
							int idx = p.lastIndexOf(":");
							context.write(new Text(p.substring(0,idx)), NullWritable.get());
							cnt++;
						}
					}
				}
			}
			
		}
		public void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException,
		InterruptedException{
			cutOffNum = Integer.parseInt(context.getConfiguration().get("cutOffNum"));
			super.setup(context);		
		}
	}
	
	public static class ReduceClass extends Reducer<Text, NullWritable, Text, NullWritable>{
		protected void reduce(Text key,Iterable<NullWritable> val, Context context) throws IOException, 
		InterruptedException {
			context.write(key, NullWritable.get());
		}
	}
	
	public static void main(String[] args)throws IOException,
	InterruptedException,ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("cutOffNum", otherArgs[2]);
		
		Job job = Job.getInstance(conf,"features");
		job.setJarByClass(GetPersonalLikesKey.class);
		
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
				MapClass.class);
		job.setReducerClass(ReduceClass.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true)? 0:1);
	}
}

