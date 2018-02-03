package com.youku.tv.movieperson.reclist20160420;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class NormalizeB {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		static Text outkey = new Text();
		static Text outvalue = new Text();
		static double max = 0, min = 0, typemax = 0, typemin = 0;
		static DecimalFormat df = new DecimalFormat("0.000");
		static double pow = 1.0,typepow=2.0;

		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String[] info = value.toString().split("\t");
			if (info[0].startsWith("\2")) {
				outkey.set(info[0]);
				outvalue.set(NormalizeRecList(info[1], typemax, typemin,typepow));
			} else {
				outkey.set(info[0]);
				outvalue.set(NormalizeRecList(info[1], max, min,pow));
			}

			context.write(outkey, outvalue);
		}

		private static String NormalizeRecList(String reclist, double mmax, double mmin,double d) {
			String[] data = reclist.split(",");
			double Max = 0.9, Min = 0.1;

			if (mmax != mmin) {
				StringBuffer sBuffer = new StringBuffer();
				for (int i = 0; i < data.length; i++) {
					String[] info = data[i].split(":");
					double sim = Math.pow(Double.valueOf(info[1]), 1 / d);
					double Sim = (Max - Min) / (mmax - mmin) * (sim - mmin) + Min;
					sBuffer.append(",").append(info[0]).append(":").append(df.format(Sim));
				}
				return sBuffer.substring(1);
			} else {
				StringBuffer sBuffer = new StringBuffer();
				for (int i = 0; i < data.length; i++) {
					String[] info = data[i].split(":");
					sBuffer.append(",").append(info[0]).append(":").append(0.1);
				}
				return sBuffer.substring(1);
			}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			pow = Double.valueOf(context.getConfiguration().get("pow"));
			typepow = Double.valueOf(context.getConfiguration().get("typepow"));
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
				String[] info = line.split("\t");
				if (info[0].equals("max")) {
					max = Math.pow(Double.valueOf(info[1]), 1 / pow);
				} else if (info[0].equals("min")) {
					min = Math.pow(Double.valueOf(info[1]), 1 / pow);
				} else if (info[0].equals("\2" + "max")) {
					typemax = Math.pow(Double.valueOf(info[1]), 1 / typepow);
				} else if (info[0].equals("\2" + "min")) {
					typemin = Math.pow(Double.valueOf(info[1]), 1 / typepow);
				}
			}
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException {
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("pow", otherArgs[3]);
		conf.set("typepow", otherArgs[4]);
		Job job = Job.getInstance(conf, "tv movie:NormalizeB");
		

		Path cachePath = new Path(otherArgs[2]);
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] arr = fs.listStatus(cachePath);
		for (FileStatus fstatus : arr) {
			Path p = fstatus.getPath();
			if (fs.isFile(p)) {
				job.addCacheFile(p.toUri());
			}
		}

		job.setJarByClass(NormalizeB.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
				MapClass.class);

		job.setNumReduceTasks(1);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
