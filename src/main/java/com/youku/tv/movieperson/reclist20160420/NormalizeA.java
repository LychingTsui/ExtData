package com.youku.tv.movieperson.reclist20160420;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
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

public class NormalizeA {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		static Text outkey = new Text();
		static Text outvalue = new Text();

		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String[] info = value.toString().split("\t");
			String[] data = info[1].split(",");

			String pre = "";
			if (info[0].startsWith("\2")) {
				pre = "\2";
			}

			outkey.set(pre + "max");
			outvalue.set(data[0].split(":")[1]);
			context.write(outkey, outvalue);

			outkey.set(pre + "min");
			outvalue.set(data[data.length - 1].split(":")[1]);
			context.write(outkey, outvalue);
		}
	}

	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException {
			double m = 0;
			if (key.toString().contains("max")) {
				m = Double.MIN_VALUE;
			} else {
				m = Double.MAX_VALUE;
			}
			for (Text text : values) {
				double d = Double.valueOf(text.toString());
				if (key.toString().contains("max")) {
					if (m < d) {//找最大值
						m = d;
					}
				} else {//找最小值
					if (m > d) {
						m = d;
					}
				}
			}
			context.write(key, new Text(m + ""));
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException {
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		Job job = Job.getInstance(conf, "tv movie:NormalizeA");
		job.setJarByClass(NormalizeA.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
				MapClass.class);
		job.setReducerClass(ReducerClass.class);

		job.setNumReduceTasks(1);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
