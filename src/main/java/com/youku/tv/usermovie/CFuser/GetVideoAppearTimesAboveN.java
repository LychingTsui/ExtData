package com.youku.tv.usermovie.CFuser;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;

public class GetVideoAppearTimesAboveN {
	public static class MapClass extends Mapper<LongWritable, Text, Text, LongWritable> {
		private Text vid = new Text();
		private LongWritable one = new LongWritable(1);

		public void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String[] vidList = value.toString().split(",");
			for (String v : vidList) {				
				vid.set(v); // guid \t 1
				context.write(vid, one);
			}
		}
	}

	public static class ReduceClass extends Reducer<Text, LongWritable, Text, LongWritable> {
		private int cutoff = 1;
		private long totalTimes = 0;

		private LongWritable allValue = new LongWritable(0);
		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			Counter counter = context.getCounter("counter", "counter");
			allValue.set(0);
			for (LongWritable value : values) {
				allValue.set(allValue.get() + value.get());
			}
			if (allValue.get() >= cutoff) {
				context.write(key, allValue);
				totalTimes += allValue.get();
			}
			else {
				counter.increment(1);
			}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			if (context.getConfiguration().get("cutoff") != null) {
				cutoff = Integer.parseInt(context.getConfiguration().get("cutoff"));
			}
			super.setup(context);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			context.write(new Text("total"), new LongWritable(totalTimes));
			super.cleanup(context);
		}
	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		conf.set("mapred.job.queue.name", "mouse");
		if (otherArgs.length == 3) {
			conf.set("cutoff", otherArgs[2]);
		}

		Job job = Job.getInstance(conf, "recommend:GetVideoAppearTimesAboveN");
		job.setJarByClass(GetVideoAppearTimesAboveN.class);

		FileInputFormat.setInputPaths(job, otherArgs[0]);

		job.setMapperClass(MapClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setCombinerClass(LongSumReducer.class);

		job.setNumReduceTasks(1);
		job.setReducerClass(ReduceClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}