package com.qiguo.tv.movie.featuresCollection;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class CollectMovieFeature_area {
	public static class MapClass extends Mapper<LongWritable, Text, Text, NullWritable>{
		private int one =  1;
		public void map(LongWritable key,Text val,Context context) throws IOException,
		InterruptedException{
			String[] valString = val.toString().split("\t",-1);
			for(String str : valString){
				if(str.startsWith("area")){
					if(str.length() > 5){
						String areaSubStr = str.substring(5); 
						String areaStr = areaSubStr.split(",")[1];
						context.write(new Text(areaStr), NullWritable.get());
				
					}
				}
			}
		}
	}
	public static class ReduceClass extends Reducer<Text, NullWritable, Text, NullWritable>{
		protected void reduce(Text key, Iterable<IntWritable> val, Context context) throws IOException,
		InterruptedException{
			
			context.write(key, NullWritable.get());
		}
	}
	public static void main(String[] args)throws IOException,InterruptedException,
	ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		
		String[] argsStr = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf,"feature_area");
		job.setJarByClass(CollectMovieFeature_area.class);
		
		MultipleInputs.addInputPath(job,new Path(argsStr[0]), TextInputFormat.class, 
				MapClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setReducerClass(ReduceClass.class);
		
		FileOutputFormat.setOutputPath(job, new Path(argsStr[1]));
		System.exit(job.waitForCompletion(true) ? 0: 1);
	}
	
	
	
}
