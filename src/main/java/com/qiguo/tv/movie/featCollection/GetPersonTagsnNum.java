package com.qiguo.tv.movie.featCollection;

import java.io.IOException;
import java.util.StringTokenizer;

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

public class GetPersonTagsnNum {
	public static class MapClass extends Mapper<LongWritable, Text, Text, LongWritable>{
		protected void map(LongWritable key, Text val, Context context) throws IOException,
		InterruptedException{
			StringTokenizer stk = new StringTokenizer(val.toString(), "\t");
			String uid = stk.nextToken();
			long len = 0;
			while (stk.hasMoreTokens()) {
				String likesStr = stk.nextToken();
				if(likesStr.startsWith("label")){
					if(likesStr.length() > 5){						
						String tmp = likesStr.substring(5);
						StringTokenizer subStk = new StringTokenizer(tmp,",");
						len = subStk.countTokens();
						context.write(new Text(uid), new LongWritable(len));
					}
				}
			}
		}
	}
	public static class ReduceClass extends Reducer<Text, LongWritable, LongWritable, NullWritable>{
		protected void reduce(Text key, Iterable<LongWritable> val, Context context) throws IOException,
		InterruptedException{
			for(LongWritable v : val){
				context.write(v, NullWritable.get());
			}			
		}
	}
	
	public static void main(String[] args)throws IOException,InterruptedException,
	ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "30");
		
		Job job = Job.getInstance(conf,"num");
		String[] argsStr = new GenericOptionsParser(conf, args).getRemainingArgs();
		MultipleInputs.addInputPath(job, new Path(argsStr[0]), TextInputFormat.class,
				MapClass.class);

		job.setJarByClass(GetPersonTagsnNum.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setReducerClass(ReduceClass.class);
		FileOutputFormat.setOutputPath(job, new Path(argsStr[1]));
		System.exit(job.waitForCompletion(true)? 0 : 1);
	}
	
}
