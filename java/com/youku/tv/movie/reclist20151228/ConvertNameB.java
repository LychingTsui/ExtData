package com.youku.tv.movie.reclist20151228;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

import com.youku.tv.movie.reclist20151228.ConvertName2IdB.RecListMapClass;
import com.youku.tv.movie.reclist20151228.ConvertName2IdB.ReducerClass;

public class ConvertNameB {
	public static class RecListMapClass extends Mapper<LongWritable, Text, Text, Text> {
		static Text outkey = new Text(), outvalue = new Text();

		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String[] info = value.toString().split("\t");
			String[] data = info[2].split(":");
			outkey.set(info[0] + "\t" + info[1]);
			if (info[2].startsWith("key")) {
				outvalue.set(data[0] + data[1]);
			} else if (info[2].startsWith("value")) {
				outvalue.set(data[0] + data[2] + ":" + data[1] + ":A:101");
			}
			context.write(outkey, outvalue);
		}
	}

	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
		static Text outkey = new Text(), outvalue = new Text();

		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException {
			StringBuffer sBuffer = new StringBuffer();
			ArrayList<String>list=new ArrayList<String>();
			for (Text text : values) {
				String info = text.toString();
				if (info.startsWith("key")) {
					outkey.set(info.substring(3));
				} else if (info.startsWith("value")) {
					String data=info.substring(5).split(":")[0];
					if (!list.contains(data)) {
						sBuffer.append(",").append(info.substring(5));
						list.add(data);
					}					
				}
			}
			if (sBuffer.length() > 1) {
				outvalue.set(Utils.sortRecListBB(sBuffer.substring(1), 0, 1, ",", ":"));
				context.write(outkey, outvalue);
			}
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException {
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "tv movie:ConvertNameB");
		job.setJarByClass(ConvertNameB.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
				RecListMapClass.class);

		job.setNumReduceTasks(1);
		job.setReducerClass(ReducerClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
