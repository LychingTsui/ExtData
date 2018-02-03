package com.qiguo.tv.movie.relatedRecXgb;

import java.io.IOException;

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


public class CollectRelatedItems {
	public static class MapClass extends Mapper<LongWritable, Text, Text, IntWritable>{
		protected void map(LongWritable key, Text val, Context context)throws IOException,
		InterruptedException{
			String[] arr = val.toString().split("\t");
			context.write(new Text(arr[0]), new IntWritable(Integer.parseInt(arr[1])));
		}
	}
	public static class ReduceClass extends Reducer<Text, IntWritable, Text, IntWritable>{
		protected void reduce(Text key, Iterable<IntWritable> vals, Context context)throws IOException,
		InterruptedException{
			int tot = 0;
			for(IntWritable v : vals){
				tot += v.get();
			}
			context.write(key, new IntWritable(tot));
		}
	}
	
	public static void main(String[] args)throws IOException, InterruptedException,
	ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		
		String[] othArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "rate");
		job.setJarByClass(CollectRelatedItems.class);
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
