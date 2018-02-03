package com.qiguo.tv.movie.relatedRecXgModel;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.qiguo.tv.movie.relatedRecXgModel.ExtRelatedMovieClickPair.ReduceClass;


public class ExtRelatedMovieShowPair {
	public static class MapClass extends Mapper<LongWritable, Text, Text,IntWritable>{
		private IntWritable one = new IntWritable(1);
		protected void map(LongWritable key, Text val, Context context)throws IOException,
		InterruptedException {
			StringTokenizer stk = new StringTokenizer(val.toString(), ",\t");
			String firstMv = stk.nextToken();
			while (stk.hasMoreTokens()) {
				String mvid = stk.nextToken();
				int idx = mvid.indexOf(":");
				mvid = mvid.substring(0,idx);
				String outPair = firstMv + ":" + mvid; 
				context.write(new Text(outPair), one);
			}
		}
	}
	
	public static class ReduceClass extends Reducer<Text, IntWritable, Text, IntWritable>{
		protected void reduce(Text key, Iterable<IntWritable>vals, Context context)throws IOException,
		InterruptedException{
			int sum = 0;
			for(IntWritable v : vals){
				sum += v.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	public static void main(String[] args) throws IOException,InterruptedException,
	ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		
		String[] othArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "showPair");
		job.setJarByClass(ExtRelatedMovieShowPair.class);
		job.setNumReduceTasks(1);
		job.setReducerClass(ReduceClass.class);
		MultipleInputs.addInputPath(job, new Path(othArgs[0]), TextInputFormat.class,
				MapClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileOutputFormat.setOutputPath(job, new Path(othArgs[1]));
		System.exit(job.waitForCompletion(true)? 0 : 1);
	}
}
