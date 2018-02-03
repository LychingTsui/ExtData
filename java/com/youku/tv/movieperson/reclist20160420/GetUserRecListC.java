package com.youku.tv.movieperson.reclist20160420;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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

public class GetUserRecListC {
	public static class RecListMapClass extends Mapper<LongWritable, Text, TextPair, Text> {
		private TextPair tp = new TextPair();
		private Text outVal = new Text();
		static HashMap<String, String> datemap = new HashMap<String,String>();

		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String[] info = value.toString().split("\t");
			String[] data = info[1].split(",");
			tp.setText(info[0]);
			tp.setValue("right");
			for (int i = 0; i < data.length; i++) {
				String[] temp = data[i].split(":");
				if (datemap.containsKey(temp[0])) {
					temp[0] = datemap.get(temp[0]);
					data[i] = temp[0] + ":" + temp[1];
				}
				outVal.set("right" + data[i]);
				context.write(tp, outVal);
			}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Path[] filePathList = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for (Path filePath : filePathList) {
				loadIndex(filePath.toString(), context);
			}
			super.setup(context);
		}

		private void loadIndex(String file, Context context) throws IOException,
				InterruptedException {
			FileReader fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr);
			String line = null;
			while ((line = br.readLine()) != null) {
				MovieDataMeta meta = new MovieDataMeta(line);
				String name = String.valueOf(meta.Getidindex());
				datemap.put(name, meta.Getid());
			}
		}
	}

	public static class DataMapClass extends Mapper<LongWritable, Text, TextPair, Text> {
		private TextPair tp = new TextPair();
		private Text outVal = new Text();

		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String[] info = value.toString().split("\t");
			
			String vv = info[1].substring(3),click="";
			if (info[2].length()>6) {
				click = info[2].substring(6);
			}
			
			tp.setText(info[0]);
			tp.setValue("left");
			if (vv.length() > 1) {
				String[] data = vv.split(",");
				for (int i = 0; i < data.length; i++) {
					outVal.set("left" + data[i].split(":")[0]);
					context.write(tp, outVal);
				}
			}

			if (click.length() > 1) {
				String[] data = click.split(",");
				for (int i = 0; i < data.length; i++) {
					outVal.set("left" + data[i].split(":")[0]);
					context.write(tp, outVal);
				}
			}
		}
	}

	public static class ReducerClass extends Reducer<TextPair, Text, Text, Text> {
		static Text outkey = new Text(), outval = new Text();
		static int cutoff = 0;

		protected void reduce(TextPair key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			ArrayList<String> vid = new ArrayList<String>();
			StringBuffer sBuffer = new StringBuffer();
			Iterator<Text> it = values.iterator();
			while (it.hasNext()) {
				String fvalue = ((Text) it.next()).toString();
				if (fvalue.startsWith("left")) {
					vid.add(fvalue.substring(4));
				} else if (fvalue.startsWith("right")) {
					String[] info = fvalue.substring(5).split(":");
					if (!vid.contains(info[0])) {
						sBuffer.append(",").append(fvalue.substring(5)).append(":A:201");
					}
				}
			}
			if (sBuffer.length() > 1) {
				outkey.set(key.getText().toUpperCase());
				outval.set(Utils.sortRecList(sBuffer.substring(1), cutoff, 1, ",", ":"));
				context.write(outkey, outval);
			}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			cutoff = Integer.valueOf(context.getConfiguration().get("cutoff"));
			super.setup(context);
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException {
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		conf.set("mapreduce.reduce.shuffle.parallelcopies", "40");
		conf.set("mapreduce.reduce.shuffle.input.buffer.percent", "0.02");
		conf.set("mapreduce.job.reduces", "50");
		conf.set("mapreduce.task.timeout", "1800000");
		//conf.set("mapreduce.task.io.sort.factor", "100");
		//conf.set("mapreduce.task.io.sort.mb", "500");
		//conf.setBoolean("mapred.output.compress", true);
		//conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.BZip2Codec");
		conf.set("mapreduce.reduce.self.java.opts", "-Xmx7120m");
		conf.set("mapreduce.child.java.opts", "-Xmx5024m");
		conf.setBoolean("dfs.client.block.write.replace-datanode-on-failure.enable", true);
		conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
		//conf.setBoolean("mapred.output.compress", true);
		//conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.BZip2Codec");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("cutoff", otherArgs[3]);

		Job job = Job.getInstance(conf, "tv person movie:GetUserRecListC");
		Path cachePath = new Path(otherArgs[4]);
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] arr = fs.listStatus(cachePath);
		for (FileStatus fstatus : arr) {
			Path p = fstatus.getPath();
			if (fs.isFile(p)) {
				job.addCacheFile(p.toUri());
			}
		}
		job.setJarByClass(GetUserRecListC.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
				RecListMapClass.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class,
				DataMapClass.class);

		job.setReducerClass(ReducerClass.class);
		job.setNumReduceTasks(20);

		job.setGroupingComparatorClass(TextComparator.class);
		job.setPartitionerClass(KeyPartitioner.class);

		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
