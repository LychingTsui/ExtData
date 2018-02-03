package com.youku.tv.movieperson.reclist20160420;

import java.io.IOException;
import java.text.ParseException;
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

public class GetDataFromLogOne {
	public static class VVMapClass extends Mapper<LongWritable, Text, Text, Text> {
		static Text outkey = new Text();
		static Text outvalue = new Text();

		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String[] info = value.toString().split("\t");
			for (int j = 1; j < info.length; j++) {
				long times = 0, date = 0;
				String[] data = info[j].split(",");
				for (int i = 2; i < data.length; i++) {
					String[] temp = data[i].split(":");
					if (date < Long.valueOf(temp[0])) {
						date = Long.valueOf(temp[0]);
					}
					times += Long.valueOf(temp[1]);
				}
				outkey.set(info[0]);
				outvalue.set("vv" + data[0] + ":" + data[1] + ":" + times + ":" + date);
				context.write(outkey, outvalue);
			}
		}
	}

	public static class ClickMapClass extends Mapper<LongWritable, Text, Text, Text> {
		static Text outkey = new Text();
		static Text outvalue = new Text();

		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String[] info = value.toString().split("\t");
			for (int j = 1; j < info.length; j++) {
				int times = 0, date = 0;
				String[] data = info[j].split(",");
				for (int i = 1; i < data.length; i++) {
					String[] temp = data[i].split(":");
					if (date < Integer.valueOf(temp[0])) {
						date = Integer.valueOf(temp[0]);
					}
					times += Integer.valueOf(temp[1]);
				}
				outkey.set(info[0]);
				outvalue.set("click" + data[0] + ":" + times + ":" + date);
				context.write(outkey, outvalue);
			}
		}
	}

	public static class ShowMapClass extends Mapper<LongWritable, Text, Text, Text> {
		static Text outkey = new Text();
		static Text outvalue = new Text();

		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String[] info = value.toString().split("\t");
			for (int j = 1; j < info.length; j++) {
				int times = 0, date = 0;
				String[] data = info[j].split(",");
				for (int i = 1; i < data.length; i++) {
					String[] temp = data[i].split(":");
					if (date < Integer.valueOf(temp[0])) {
						date = Integer.valueOf(temp[0]);
					}
					times += Integer.valueOf(temp[1]);
				}
				outkey.set(info[0]);
				outvalue.set("show" + data[0] + ":" + times + ":" + date);
				context.write(outkey, outvalue);
			}
		}
	}

	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
		static Text outvalue = new Text();

		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException {
			StringBuffer showBuffer = new StringBuffer();
			StringBuffer clickBuffer = new StringBuffer();
			StringBuffer vvBuffer = new StringBuffer();
			for (Text text : values) {
				String info = text.toString();
				if (info.startsWith("vv")) {
					vvBuffer.append(",").append(info.substring(2));
				} else if (info.startsWith("click")) {
					clickBuffer.append(",").append(info.substring(5));
				} else if (info.startsWith("show")) {
					showBuffer.append(",").append(info.substring(4));
				}
			}

			String vv = "vv:";
			if (vvBuffer.length() > 1) {
				vv += Utils.sortRecList(vvBuffer.substring(1), 0, 3, ",", ":");
			}

			String click = "click:";
			if (clickBuffer.length() > 1) {
				click += Utils.sortRecList(clickBuffer.substring(1), 0, 2, ",", ":");
			}

			String show = "show:";
			if (showBuffer.length() > 1) {
				show += Utils.sortRecList(showBuffer.substring(1), 0, 2, ",", ":");
			}

			outvalue.set(vv + "\t" + click + "\t" + show);
			context.write(key, outvalue);
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException, ParseException {
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String vvpath = otherArgs[0];
		String clickpath = otherArgs[1];
		Job job = Job.getInstance(conf, "tv movie person:GetDataFromLog");
		job.setJarByClass(GetDataFromLog.class);

			MultipleInputs.addInputPath(job, new Path(vvpath),
					TextInputFormat.class, VVMapClass.class);
			MultipleInputs.addInputPath(job, new Path(clickpath),
					TextInputFormat.class, ClickMapClass.class);

			

		job.setNumReduceTasks(10);
		job.setReducerClass(ReducerClass.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
