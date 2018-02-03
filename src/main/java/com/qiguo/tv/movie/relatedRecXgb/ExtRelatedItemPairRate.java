package com.qiguo.tv.movie.relatedRecXgb;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;

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


public class ExtRelatedItemPairRate {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{
		protected void map(LongWritable key, Text val, Context context)throws IOException,
		InterruptedException{
			String[] infoArr = val.toString().split("\t", -1);
			if(infoArr[0].contains(":")){
				String refMv = infoArr[0].split(":")[0];
				context.write(new Text(refMv), val);
			}else {
				context.write(new Text(infoArr[0]), new Text(infoArr[1]));
			}
		}
	}
	public static class ReduceClass extends Reducer<Text, Text, Text, Text>{
		protected void reduce(Text key, Iterable<IntWritable> vals, Context context)throws IOException, 
		InterruptedException{
			//String outStr ="";
			int[] clkshw = {0, 0};
			int cnt = 0;
			for(IntWritable v : vals){
				clkshw[cnt++] = v.get();
			}
			if(clkshw[0] > clkshw[1]){
				int tmp = clkshw[0];
				clkshw[0] = clkshw[1];
				clkshw[1] = tmp;
			}
			float res = clkshw[0] / (1.0f*clkshw[1]);
			BigDecimal bgd = new BigDecimal(res);
			res = Float.parseFloat(bgd.setScale(3, BigDecimal.ROUND_HALF_UP).toString());
			int iter = clkshw[0] == 0 ? clkshw[1] : clkshw[0];
			for(int i = 0; i < iter; i++){
				context.write(new Text(res+""), new Text(key.toString() +" "+ clkshw[0]+" "+clkshw[1]));
			}
		}
	}
	
	public static void mian(String[] args)throws IOException, InterruptedException,
	ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		
		String[] othArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "rate");
		job.setJarByClass(ExtRelatedItemPairRate.class);
		job.setNumReduceTasks(1);
		job.setReducerClass(ReduceClass.class);
		MultipleInputs.addInputPath(job, new Path(othArgs[0]), TextInputFormat.class,
				MapClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job, new Path(othArgs[1]));
		System.exit(job.waitForCompletion(true)? 0 : 1);
	}
}
