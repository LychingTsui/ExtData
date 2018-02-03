package com.youku.tv.movieperson.reclist20160420;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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


public class GetUserRecListB {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		static Text outkey = new Text(), outval = new Text();

		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String[] info = value.toString().split("\t");
			// String[] data = info[1].split(",");
			// for (int i = 0; i < data.length; i++) {
			// outkey.set(info[0]);
			// outval.set(data[i]);
			// context.write(outkey, outval);
			// }
			outkey.set(info[0]);
			outval.set(info[1]);
			context.write(outkey, outval);
		}
	}

	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
		static Text outval = new Text();
		static HashMap<String, Double> datemap = new HashMap<String,Double>();
		static HashMap<String, Double> ratemap = new HashMap<String,Double>();
		static int cutoff = 30;
		static double type = 3.0;
		static DecimalFormat df = new DecimalFormat("0.0000");
		static String idname = "id";

		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException {
			HashMap<String, Double> map = new HashMap<String,Double>();
			for (Text text : values) {
				String[] info = text.toString().split(":");
				double d = Double.valueOf(info[1]);
				if (map.containsKey(info[0])) {
					d += map.get(info[0]);
				}	
				map.put(info[0], d);
			}

			Object[] obj = map.keySet().toArray();
			StringBuffer sBuffer = new StringBuffer();
			double temprate = Math.pow(0.65, type);
			for (int i = 0; i < obj.length; i++) {
				double d = map.get(obj[i].toString());

				if (ratemap.containsKey(obj[i].toString())) {
					d = d * ratemap.get(obj[i].toString());
				} else {
					d = d * temprate;
				}

				double daterate = 2;
				if (datemap.containsKey(obj[i].toString())) {
					daterate = datemap.get(obj[i].toString());
				}
				d = d / daterate;
				sBuffer.append(",").append(obj[i].toString()).append(":").append(df.format(d));
			}
			outval.set(Utils.sortRecList(sBuffer.substring(1), cutoff, 1, ",", ":"));
			context.write(key, outval);
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			cutoff = Integer.valueOf(context.getConfiguration().get("cutoff"));
			type = Double.valueOf(context.getConfiguration().get("type"));
			idname = context.getConfiguration().get("idname");
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

				// String name = meta.Getid();
				String name = String.valueOf(meta.Getidindex());
				if (idname.equals("title")) {
					name = meta.Gettitle().replaceAll(":", "");
				}

				if (meta.Getdate().length() > 1) {
					double daterate = 2;
					try {
						daterate = Math.log(3 + Utils.DateSubYear(meta.Getdate()));
					} catch (ParseException e) {
						e.printStackTrace();
					}
					datemap.put(name, daterate);
				}

				try {
					double rate = Double.valueOf(meta.Getrating()) / 100;
					rate = Math.pow(rate, type);
					if (ratemap.containsKey(name)) {
						if (rate > ratemap.get(name)) {
							rate = ratemap.get(name);
						}
					}
					ratemap.put(name, rate);
				} catch (Exception e) {
					continue;
				}
			}
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException {
		Configuration conf = new Configuration();
		//conf.set("mapred.reduce.parallel.copies", "25");
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
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		conf.set("cutoff", otherArgs[3]);
		conf.set("type", otherArgs[4]);
		conf.set("idname", otherArgs[5]);
        
		Job job = Job.getInstance(conf, "tv person movie:GetUserRecListB");
        job.setNumReduceTasks(10);
		Path cachePath = new Path(otherArgs[2]);
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] arr = fs.listStatus(cachePath);
		for (FileStatus fstatus : arr) {
			Path p = fstatus.getPath();
			if (fs.isFile(p)) {
				job.addCacheFile(p.toUri());
			}
		}

		job.setJarByClass(GetUserRecListB.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
				MapClass.class);

		job.setReducerClass(ReducerClass.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
