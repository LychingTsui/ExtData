package com.qiguo.tv.movie.relatedRecXgb;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.lang.ObjectUtils.Null;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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


public class ExtNakedMovId {
	public static class MapClass extends Mapper<LongWritable, Text, Text, NullWritable>{
		protected void map(LongWritable key, Text val, Context context)throws IOException,
		InterruptedException{
			StringTokenizer stk = new StringTokenizer(val.toString(), "\t");
			String mvId = stk.nextToken();
			while (stk.hasMoreTokens()) {
				String tagStr = stk.nextToken();
				if(tagStr.startsWith("tags")){
					StringTokenizer substk = new StringTokenizer(tagStr.substring(5), ",");
					while (substk.hasMoreTokens()) {
						if(substk.nextToken().equals("情色")){
							context.write(new Text(mvId), NullWritable.get());
						}
					}
				}
			}
		}
	}
	public static void main(String[] args)throws IOException, 
	InterruptedException, ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		
		String[] othArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "ext");
		job.setJarByClass(ExtNakedMovId.class);
		job.setNumReduceTasks(1);
		//job.setReducerClass(ReduceClass.class);
		MultipleInputs.addInputPath(job, new Path(othArgs[0]), TextInputFormat.class,
				MapClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileOutputFormat.setOutputPath(job, new Path(othArgs[1]));
		System.exit(job.waitForCompletion(true)? 0 : 1);
	}
}
