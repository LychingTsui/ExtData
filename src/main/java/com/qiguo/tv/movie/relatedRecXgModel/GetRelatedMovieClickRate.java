package com.qiguo.tv.movie.relatedRecXgModel;

import java.io.IOException;
import java.math.BigDecimal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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


public class GetRelatedMovieClickRate {
	public static class MapClass extends Mapper<LongWritable, Text, Text, IntWritable>{
		protected void map(LongWritable key, Text val, Context context) throws IOException,
		InterruptedException{
			String[] strArr = val.toString().split("\t");
			context.write(new Text(strArr[0]), new IntWritable(Integer.parseInt(strArr[1])));
		}
	}
	
	public static class ReduceClass extends Reducer<Text, IntWritable, Text, Text>{
		protected void reduce(Text key, Iterable<IntWritable>vals, Context context)throws IOException,
		InterruptedException{
			double res = 0.0;
			int[] tmp = {0, 0};
			int i = 0;
			
			for(IntWritable v: vals){
				tmp[i++] = v.get();
			}
			boolean f1 = tmp[0] != 0 ? true : false;
			boolean f2 = tmp[1] != 0 ? true : false;
			if(f2 && f1){
				double restmp = tmp[0] / (tmp[1]*1.0);
				if(restmp > 1.0){
					BigDecimal bgd = new BigDecimal(1.0/restmp);
					res = Double.parseDouble(bgd.setScale(4, BigDecimal.ROUND_HALF_UP).toString()); 
				}else {
					BigDecimal bgd = new BigDecimal(restmp);
					res = Double.parseDouble(bgd.setScale(4, BigDecimal.ROUND_HALF_UP).toString()); 
				}
			}
			
			context.write(new Text(res+""), key);
		}
	}
	public static void main(String[] args)throws IOException,InterruptedException,
	ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		
		String[] othArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "rate");
		job.setJarByClass(GetRelatedMovieClickRate.class);
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
