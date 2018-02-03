package com.qiguo.tv.movie.statistics;

import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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


public class PsnAnalysis {
	public static class MapClass extends Mapper<LongWritable, Text, Text, NullWritable>{
		protected void map(LongWritable key, Text val, Context context)throws IOException,
		InterruptedException{
			String vvInfo = val.toString();
			String id = "00:00:00:00:00:00";
			StringTokenizer stk = new StringTokenizer(vvInfo, "\t");
			String guid = stk.nextToken();
			if(!guid.equals(id)){    
				context.write(new Text(guid), NullWritable.get());
			}
		}
	}
	public static class ReduceClass extends Reducer<Text, NullWritable, Text, NullWritable>{
		protected void reduce(Text key, Iterable<NullWritable>vals, Context context)throws IOException,
		InterruptedException{
			context.write(key, NullWritable.get());
		}
	}
	public static void main(String[] args)throws IOException,InterruptedException,
	ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "30");
		Job job = Job.getInstance(conf, "collectVvitems");
		String[] argstrs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		FileSystem dirfs = FileSystem.get(URI.create(argstrs[0]), conf);
		FileStatus fstas = dirfs.getFileStatus(new Path(argstrs[0]));
		String sdate = argstrs[2];  // 可以设置收集起始日期（正月计算，未实现截止到日） 格式：201707 
		String endday = argstrs[3];
		int start_y = Integer.parseInt(sdate.substring(0, 4));
		int start_m = Integer.parseInt(sdate.substring(4,6));
		int start_d = Integer.parseInt(sdate.substring(6));
		
		int end_y = Integer.parseInt(endday.substring(0, 4));
		int end_m = Integer.parseInt(endday.substring(4,6));
		int end_d = Integer.parseInt(endday.substring(6));
		if(fstas.isDirectory()){
			for(FileStatus subfstus : dirfs.listStatus(new Path(argstrs[0]))){
				String subdir = subfstus.getPath().toString();
				int idx = subdir.lastIndexOf("/");
				String lastStr = subdir.substring(idx+1);
				int monthArv = Integer.parseInt(lastStr.substring(4,6));
				int yearArv = Integer.parseInt(lastStr.substring(0,4));
				int dayArv = Integer.parseInt(lastStr.substring(6));
				if(yearArv == start_y){
					/* 在此可以设置某几个月份内的数据，定制几个月内的vvHistory  */					
					if(monthArv > start_m && monthArv < end_m){
						FileSystem f = FileSystem.get(URI.create(subdir), conf);
						for(FileStatus fstu : f.listStatus(new Path(subdir))){
							MultipleInputs.addInputPath(job, fstu.getPath(), TextInputFormat.class, MapClass.class);				
						}
					}else if (monthArv == start_m  && dayArv > start_d) {
						FileSystem f = FileSystem.get(URI.create(subdir), conf);
						for(FileStatus fstu : f.listStatus(new Path(subdir))){
							MultipleInputs.addInputPath(job, fstu.getPath(), TextInputFormat.class, MapClass.class);				
						}
					}else if (monthArv == end_m && dayArv < end_d) {
						FileSystem f = FileSystem.get(URI.create(subdir), conf);
						for(FileStatus fstu : f.listStatus(new Path(subdir))){
							MultipleInputs.addInputPath(job, fstu.getPath(), TextInputFormat.class, MapClass.class);				
						}
					}
				}
				if (yearArv > start_y ) {
					if(yearArv < end_y){
						FileSystem f = FileSystem.get(URI.create(subdir), conf);
						for(FileStatus fstu : f.listStatus(new Path(subdir))){
							MultipleInputs.addInputPath(job, fstu.getPath(), TextInputFormat.class, MapClass.class);				
						}
					}else if(yearArv == end_y && monthArv < end_m){
						FileSystem f = FileSystem.get(URI.create(subdir), conf);
						for(FileStatus fstu : f.listStatus(new Path(subdir))){
							MultipleInputs.addInputPath(job, fstu.getPath(), TextInputFormat.class, MapClass.class);				
						}
					}else if (yearArv == end_y && monthArv == end_m && dayArv < end_d) {
						FileSystem f = FileSystem.get(URI.create(subdir), conf);
						for(FileStatus fstu : f.listStatus(new Path(subdir))){
							MultipleInputs.addInputPath(job, fstu.getPath(), TextInputFormat.class, MapClass.class);				
						}
					}
				}
			}		
		} 
		
		//MultipleInputs.addInputPath(job, new Path(argstrs[0]), TextInputFormat.class, MapClass.class);
		job.setJarByClass(PsnAnalysis.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setReducerClass(ReduceClass.class);
		
		FileOutputFormat.setOutputPath(job, new Path(argstrs[1]));
		System.exit(job.waitForCompletion(true)? 0:1 );
	}
	
}
