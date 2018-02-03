package com.qiguo.tv.movie.model;

import java.io.IOException;
import java.util.HashSet;
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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class ExtNegsfromPersonalShow {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{
		protected void map(LongWritable key, Text val, Context context)throws IOException,
		InterruptedException{
			StringTokenizer stk = new StringTokenizer(val.toString(), "\t");
			if(stk.countTokens() > 1){
				String guid = stk.nextToken();
				String out = "";
				while(stk.hasMoreTokens()){
					out += stk.nextToken()+" ";
				}
				context.write(new Text(guid), new Text(out));
			}
		}
	}
	public static class ReduceClass extends Reducer<Text, Text, Text, Text>{
		protected void reduce(Text key, Iterable<Text> vals, Context context)throws IOException,
		InterruptedException {
			HashSet<String> vvset = new HashSet<String>();
			HashSet<String>showSet = new HashSet<String>();
			for(Text v : vals){
				StringTokenizer stk = new StringTokenizer(v.toString(), " ");
				while (stk.hasMoreTokens()) {
					String tmp = stk.nextToken();
					if(!tmp.startsWith("#")){
						vvset.add(tmp);
					}else {
						showSet.add(tmp.substring(1).trim()); // 切除＃标记
					}
				}
			}
			for(String show : showSet){
				if(!vvset.contains(show)){
					context.write(key, new Text("0:" + show));
				}
			}
		}
	}
	public static void main(String[] args)throws IOException,InterruptedException,
	ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies","25");
		String[] othArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		Job job = Job.getInstance(conf, "neg");
		job.setJarByClass(ExtNegsfromPersonalShow.class);

		MultipleInputs.addInputPath(job, new Path(othArgs[0]), TextInputFormat.class,
				MapClass.class);
		job.setNumReduceTasks(1);
		job.setReducerClass(ReduceClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(othArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
