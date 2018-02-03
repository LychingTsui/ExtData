package com.youku.tv.movieperson.reclist20160420;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class GetDataFromUser {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		static Text outkey = new Text(), outvalue = new Text();

		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String[] info = value.toString().split("\t");
			String[] data = info[1].split(",");
			StringBuffer sBuffer = new StringBuffer();
			for (int i = 0; i < data.length; i++) {
				String[] temp = data[i].split(":");
				sBuffer.append(",");
				sBuffer.append(temp[0]).append(":");
				sBuffer.append("10").append(":");
				sBuffer.append(temp[1]).append(":");
				sBuffer.append("20160329");
			}
			sBuffer.append("\t").append("click:").append("\t").append("show:");
			outkey.set(info[0]);
			outvalue.set("vv:" + sBuffer.substring(1));
			context.write(outkey, outvalue);
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException {
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		Job job = Job.getInstance(conf, "tv movie person:GetDataFromUser");
		job.setJarByClass(GetDataFromUser.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
				MapClass.class);

		job.setNumReduceTasks(1);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
