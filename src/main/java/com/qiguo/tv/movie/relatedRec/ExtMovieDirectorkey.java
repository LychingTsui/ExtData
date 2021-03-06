package com.qiguo.tv.movie.relatedRec;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ExtMovieDirectorkey {
	public static class MapClass extends Mapper<LongWritable, Text, Text, NullWritable>{
		final private static IntWritable One = new IntWritable(1);
		protected void map(LongWritable key,Text value,Context context) throws IOException,
		InterruptedException{
			String[] valString = value.toString().split("\t",-1);
			for(String s : valString){
				if(s.startsWith("diretor")){
					if(s.length() > 8){
						String director = s.substring(8);
						StringTokenizer stk = new StringTokenizer(director, ",");
						String direct = stk.nextToken().trim();					
						context.write(new Text(direct), NullWritable.get());						
					}
				}
			}
		}
	}
	
	public static class ReduceClass extends Reducer<Text, NullWritable, Text, NullWritable>{
		protected void reduce(Text key,Iterable<NullWritable>val,Context context)throws IOException,
		InterruptedException {
			String out = "t2" + key.toString();
			context.write(new Text(out), NullWritable.get());
		}
	}
	public static void main(String[] args)throws IOException,InterruptedException,
	ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "30");
		
		Job job = Job.getInstance(conf,"diretor");
		String[] argsStr = new GenericOptionsParser(conf, args).getRemainingArgs();
		MultipleInputs.addInputPath(job, new Path(argsStr[0]), TextInputFormat.class,
				MapClass.class);
		
		job.setJarByClass(ExtMovieDirectorkey.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setReducerClass(ReduceClass.class);
		FileOutputFormat.setOutputPath(job, new Path(argsStr[1]));
		System.exit(job.waitForCompletion(true)? 0 : 1);
	}
}
