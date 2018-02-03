package com.qiguo.tv.movie.featCollection;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
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
/**
 *@info: 获取用户的偏好key， 截取率按最大label对应的值的1/10截断
 **/
public class GetPersonalLikesKeyCutOff {
	public static class MapClass extends Mapper<LongWritable, Text, Text, NullWritable>{
		private double cutRate = 0.1;
		protected void map(LongWritable key, Text val, Context context)throws IOException,
		InterruptedException{
			String[] likeArr = val.toString().split("\t", -1);
			StringTokenizer stk = new StringTokenizer(likeArr[2].substring(5), ",");
			if(stk.countTokens() > 0){
				String firStr = stk.nextToken().trim();
				int idx = firStr.lastIndexOf(":");
				String firkey = firStr.substring(0,idx);
				double maxval = Double.parseDouble(firStr.substring(idx+1).trim());
				int cutoff = (int) (maxval * cutRate);
				double tmp = maxval;
				context.write(new Text(firkey), NullWritable.get());
				while(stk.hasMoreTokens() && tmp > cutoff){
					String p2 = stk.nextToken().trim();
					int splitidx = p2.lastIndexOf(":");
					tmp = Double.parseDouble(p2.substring(splitidx+1).trim());
					String featkey = p2.substring(0, splitidx);
					context.write(new Text(featkey), NullWritable.get());
				}
			}
		}
		public void setup(Context context)throws IOException, 
		InterruptedException{
			cutRate = Double.parseDouble(context.getConfiguration().get("cutRate"));
		}
	}
	public static class ReduceClass extends Reducer<Text, NullWritable, Text, NullWritable>{
		protected void reduce(Text key,Iterable<NullWritable> val, Context context)throws IOException,
		InterruptedException{
			context.write(key, NullWritable.get());
		}
	}
	public static void main(String[] args)throws IOException,
	InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "30");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("cutRate", otherArgs[2]);
		Job job = Job.getInstance(conf, "personalLikesKey");
		job.setJarByClass(GetPersonalLikesKeyCutOff.class);
		
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, 
				MapClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
