package com.qiguo.tv.movie.model;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/***
 * @author Tsui
 * info: 在整合几天show的数据，对某guid，保留其曝光次数大于1的电影（作为可能的负样本）。
 */

public class CollectPersonalShow {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{
		protected void map(LongWritable key, Text val, Context context) throws IOException,
		InterruptedException{
			StringTokenizer stk = new StringTokenizer(val.toString(), "\t");
			String guid = stk.nextToken();
			StringBuffer strbuf = new StringBuffer();
			if(!guid.equals("00:00:00:00:00:00")){
				while(stk.hasMoreTokens()){
					String mv = stk.nextToken();
					String dateStr = stk.nextToken();
					int idx = dateStr.indexOf(":");
					String showcnt = dateStr.substring(idx+1).trim();
					if(mv.length() == 32){
						strbuf.append(mv + ":" + showcnt+" ");
					}
				}
				String outStr = strbuf.toString();
				context.write(new Text(guid), new Text(outStr));
			}
		}
	}
	public static class ReduceClass extends Reducer<Text, Text, Text, Text>{
		protected void reduce(Text key, Iterable<Text>vals, Context context)throws IOException,
		InterruptedException{
			HashMap<String, Integer>mvCntMp = new HashMap<String, Integer>();
			for(Text v : vals){
				String[] strArr = v.toString().split(" ");
				for(String str : strArr){
					String mv = str.substring(0,32);
					int cnt = Integer.parseInt(str.substring(33).trim());
					if(mvCntMp.containsKey(mv)){
						int newCnt = mvCntMp.get(mv) + cnt;
						mvCntMp.put(mv, newCnt);
					}else {
						mvCntMp.put(mv, cnt);
					}
				}
			}
			StringBuffer strbuf = new StringBuffer();
			for(Map.Entry<String, Integer> entry : mvCntMp.entrySet()){
				if(entry.getValue() > 1){
					strbuf.append("#" + entry.getKey() + " "); //#做标记用
				}
			}
			String out = strbuf.toString();
			context.write(key, new Text(out));
		}
	}
	public static void main(String[] args)throws IOException, InterruptedException,
	ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies","25");
		String[] othArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		Job job = Job.getInstance(conf, "guid");
		job.setJarByClass(CollectPersonalShow.class);
		 //Path cachePath = new Path(othArgs[2]);
		 //FileSystem fs = FileSystem.get(conf);
		 //FileStatus[] arr = fs.listStatus(cachePath);
		/* for (FileStatus fstatus : arr) {
			 Path p = fstatus.getPath();
			 if (fs.isFile(p)) {
				 job.addCacheFile(p.toUri());
			 }
		 }	*/
		MultipleInputs.addInputPath(job, new Path(othArgs[0]), TextInputFormat.class,
				MapClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setNumReduceTasks(1);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(othArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
