package com.youku.tv.movie.reclist20151228;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

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
import com.youku.tv.movie.reclist20151228.MovieDataMeta;

public class GetTypeHotMovie {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		static Text outkey = new Text();
		static Text outvalue = new Text();

		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			MovieDataMeta meta = new MovieDataMeta(value.toString());
			if (meta.Gettype().length() > 1) {
				String[] types = meta.Gettype().split(",");
				for (int i = 0; i < types.length; i++) {
					outkey.set(types[i]);
					outvalue.set(meta.Getid() + ":" + meta.Getrating() / 100.0);
					context.write(outkey, outvalue);
				}
			}
		}
	}

	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

		static Text outvalue = new Text();
		static int cutoff = 30;

		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException {
			StringBuffer sBuffer = new StringBuffer();
			for (Text text : values) {
				sBuffer.append(",").append(text.toString()).append(":A:hot");
			}
			String[] vids = Utils.sortRecListN(sBuffer.substring(1), 100, 1, ",", ":").split(",");
			ArrayList<String> list = new ArrayList<String>();
			for (int i = 0; i < vids.length; i++) {
				list.add(vids[i]);
			}
			Collections.shuffle(list, new Random(1));
			sBuffer = new StringBuffer();
			int len = list.size();
			if (list.size() > cutoff && cutoff > 0) {
				len = cutoff;
			}
			for (int i = 0; i < len; i++) {
				sBuffer.append(",").append(list.get(i));
			}
			outvalue.set(Utils.sortRecListN(sBuffer.substring(1), 0, 1, ",", ":"));
			context.write(key, outvalue);
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException {
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("cutoff", otherArgs[2]);

		Job job = Job.getInstance(conf, "tv movie:GetTypeHotMovie");
		job.setJarByClass(GetTypeHotMovie.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
				MapClass.class);

		job.setNumReduceTasks(1);
		job.setReducerClass(ReducerClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
