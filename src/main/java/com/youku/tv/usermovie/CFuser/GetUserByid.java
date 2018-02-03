package com.youku.tv.usermovie.CFuser;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class GetUserByid {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		
		private Text outkey = new Text();
		private Text outvalue = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			StringBuffer buffer = new StringBuffer();
				Counter counter = context.getCounter("counter", "counter");
				counter.increment(1);
               String info[] = value.toString().split("\t");
               for (int i = 1; i < info.length; i++) {
				buffer.append("\t").append(info[i]);
			}
              outkey.set(info[0]);
              outvalue.set(buffer.substring(1));
              context.write(outkey, outvalue);
               
		}
   }
	public static class ReduceClass extends Reducer<Text, Text, Text, Text> {
		private Text outvalue = new Text();
		static int index = 0;
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException {
			Counter counter =context.getCounter("count", "count");
			Set<String> set = new HashSet<String>();
			StringBuffer buffer = new StringBuffer();
			index++;
			for (Text text : values) {
				buffer.append("\t").append(text.toString());
				String []tb = text.toString().split("\t");
				if (tb[0].substring(3).length()>0) {
					String []te = tb[0].substring(3).split(",");
					for (String string : te) {
						set.add(string.split(":")[0]);
					}
				}
				if (tb[1].substring(6).length()>0) {
					String []te = tb[1].substring(6).split(",");
					for (String string : te) {
						if (string.split(":")[0].length()==32) {
							set.add(string.split(":")[0]);
						}
					}
				}
				
			}
			int a = set.size();
			counter.increment(a);
			outvalue.set(buffer.substring(1)+"\t"+index);
			context.write(key, outvalue);
		}
	}
public static void main(String[] args) throws IOException, InterruptedException,
	ClassNotFoundException {
Configuration conf = new Configuration();
conf.set("mapred.reduce.parallel.copies", "25");
String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
Job job = Job.getInstance(conf, "tv movie:GetTagsTimes");
job.setJarByClass(GetUserByid.class);

MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
		MapClass.class);
job.setNumReduceTasks(1);
job.setReducerClass(ReduceClass.class);
job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(Text.class);

FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
System.exit(job.waitForCompletion(true) ? 0 : 1);
 }
}
