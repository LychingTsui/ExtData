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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class ConvertName2IdA {
	public static class RecListMapClass extends Mapper<LongWritable, Text, Text, Text> {
		static Text outkey = new Text(), outvalue = new Text();

		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String[] info = value.toString().split("\t");
			String[] data = info[1].split(",");
			outkey.set(info[0]);
			outvalue.set("list" + value.toString() + "\2" + "key");
			context.write(outkey, outvalue);
			for (int i = 0; i < data.length; i++) {
				String[] temp = data[i].split(":");
				if (temp.length>2) {
					temp[0]=temp[0]+":"+temp[1];
					temp[1]=temp[2];
				}
				outkey.set(temp[0]);
				outvalue.set("list" + value.toString() + "\2" + "value" + ":" + temp[1]);
				context.write(outkey, outvalue);
			}
		}
	}

	public static class MovieInfoMapClass extends Mapper<LongWritable, Text, Text, Text> {
		static Text outkey = new Text(), outvalue = new Text();

		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			MovieDataMeta meta = new MovieDataMeta(value.toString());
			outkey.set(meta.Gettitle()+"\3"+meta.Getidindex());
			outvalue.set("info" + meta.Getid());
			context.write(outkey, outvalue);
		}
	}

	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
		static Text outkey = new Text(), outvalue = new Text();

		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException {
			String id = null;
			ArrayList<String> list = new ArrayList<String>();
			for (Text text : values) {
				String info = text.toString();
				if (info.startsWith("list")) {
					list.add(info.substring(4));
				} else if (info.startsWith("info")) {
					id = info.substring(4);
				}
			}
			if (id != null) {
				for (int i = 0; i < list.size(); i++) {
					String[] info = list.get(i).split("\2");
					outkey.set(info[0]);//电影+\t+推荐电影
					outvalue.set(info[1] + ":" + id);//key+id  or value+id+权重
					context.write(outkey, outvalue);
				}
			}
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException {
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "tv movie:ConvertName2IdA");
		job.setJarByClass(ConvertName2IdA.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
				RecListMapClass.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class,
				MovieInfoMapClass.class);

		job.setNumReduceTasks(1);
		job.setReducerClass(ReducerClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
