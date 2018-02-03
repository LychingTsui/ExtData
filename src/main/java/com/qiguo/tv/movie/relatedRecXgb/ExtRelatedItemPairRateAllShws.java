package com.qiguo.tv.movie.relatedRecXgb;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

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


public class ExtRelatedItemPairRateAllShws {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{
		protected void map(LongWritable key, Text val, Context context)throws IOException,
		InterruptedException{
			String[] infoArr = val.toString().split("\t", -1);
			if(infoArr[0].contains(":")){
				context.write(new Text(infoArr[0].split(":")[0]), val);
			}else {
				context.write(new Text(infoArr[0]), new Text(infoArr[1]));
			}
		}
	}
	public static class ReduceClass extends Reducer<Text, Text, Text, Text>{
		protected void reduce(Text key, Iterable<Text> vals, Context context)throws IOException, 
		InterruptedException{
			HashMap<String, Integer> mvClkMp = new HashMap<String, Integer>();
			HashMap<String, Integer> mvDisMp = new HashMap<String, Integer>();
			HashSet<String> pairSts = new HashSet<String>();
			int totshw = 0;
			
			for(Text v : vals){
				if(v.toString().contains(":")){
					int clktmp = Integer.parseInt(v.toString().split("\t")[1]);
					String pair = v.toString().split("\t")[0];
					if(mvClkMp.containsKey(pair)){
						pairSts.add(pair);
						if(mvClkMp.get(pair) > clktmp){
							mvDisMp.put(pair, mvClkMp.get(pair));
							mvClkMp.put(pair, clktmp);
						}else {
							mvDisMp.put(pair, clktmp);
						}
					}else {
						mvClkMp.put(pair, clktmp);
						mvDisMp.put(pair, clktmp);
					}
				}else {
					totshw = Integer.parseInt(v.toString().trim()); //show
				}
			}
			for(Map.Entry<String, Integer> entry : mvClkMp.entrySet()){
				float res = 0.0f;
				if(totshw > 0){
					if(pairSts.contains(entry.getKey())){
						res = entry.getValue() / (1.0f * totshw + 10); //＋10为了平滑点击率，消除点击次数展示次数均小时点击率很高的情况
						BigDecimal bgd = new BigDecimal(res);
						res = Float.parseFloat(bgd.setScale(3, BigDecimal.ROUND_HALF_UP).toString());
					}
					//int iter = pairSts.contains(entry.getKey()) ? entry.getValue() : mvDisMp.get(entry.getKey());
					context.write(new Text(res+""), new Text(entry.getKey() +"\t"+ entry.getValue() +"\t"+ totshw));
				}
			}
		}
	}
	
	public static void main(String[] args)throws IOException, InterruptedException,
	ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		
		String[] othArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "rate");
		job.setJarByClass(ExtRelatedItemPairRateAllShws.class);
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
