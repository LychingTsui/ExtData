package com.youku.tv.usermovie.CFuser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.youku.tv.movieperson.reclist20160420.Utils;

public class GetUserMovieA {
	public static class SimUserMapClass extends Mapper<LongWritable, Text, Text, Text> {
		static Text outkey = new Text(), outvalue = new Text();

		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String[] info = value.toString().split("\t");

			String[] data = info[1].split(",");
			for (int i = 0; i < data.length; i++) {
				String[] temp = data[i].split("\2");
				outkey.set(temp[0]);
				outvalue.set("user" + info[0] + "\2" + temp[1]);
				context.write(outkey, outvalue);
			}
		}
	}

	public static class UserDataMapClass extends Mapper<LongWritable, Text, Text, Text> {
		static Text outkey = new Text(), outvalue = new Text();
		static HashMap<String, String> map =new HashMap<String, String>();
		static DecimalFormat df = new DecimalFormat("0.000");
		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String[] info = StringUtils.splitPreserveAllTokens(value.toString(), "\t");
			if (!info[1].startsWith("vv")) {
				return;
			}
			String vv = info[1].substring(3);
			if (vv.length() > 0) {
				if (map.containsKey(info[0])) {
					outkey.set(map.get(info[0]));
				}
				//outkey.set(info[0]);
				String[] data = vv.split(",");
				for (int i = 0; i < data.length; i++) {
					String[] temp = data[i].split(":");
					if (temp.length!=4) {
						return;
					}
					boolean one=StringUtils.isNumeric(temp[2]);
					boolean two=StringUtils.isNumeric(temp[1]);
					if (!(one&&two)) {
						return;
					}
					double playrate = Math.min(Double.valueOf(temp[2]) / Double.valueOf(temp[1]),
							1.0);
					if (playrate == 0) {
						playrate =1;
					}
					double daterate = 2;
					try {
						daterate = Math.log10(10 + Utils.DateSub(temp[3]));
					} catch (ParseException e) {
						e.printStackTrace();
					}
					double rate = playrate / daterate;
					outvalue.set("vv" + temp[0] + "\2" + df.format(rate));
					context.write(outkey, outvalue);
				}
			}
		}
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Path[] filePathList = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for (Path filePath : filePathList) {
				loadIndex(filePath.toString(), context);
			}
		}
		private void loadIndex(String file, Context context) throws IOException,
		InterruptedException {
	FileReader fr = new FileReader(file);
	BufferedReader br = new BufferedReader(fr);
	String line = null;
	while ((line = br.readLine()) != null) {
		String[] info = line.split("\t");
		if (info.length==5) {
			map.put(info[0], info[4]);
		}
		}
	}
	}

	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
		static Text outkey = new Text(), outvalue = new Text();
		static DecimalFormat df = new DecimalFormat("0.0000");

		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException {
			HashMap<String, Double> map = new HashMap<String,Double>();
			HashMap<String, Double> vv = new HashMap<String,Double>();
			for (Text text : values) {
				String info = text.toString();
				if (info.startsWith("user")) {
					String[] data = info.substring(4).split("\2");
					map.put(data[0], Double.valueOf(data[1]));
				} else if (info.startsWith("vv")) {
					String[] data = info.substring(2).split("\2");
					vv.put(data[0], Double.valueOf(data[1]));
				}
			}
			if (vv != null && map.size() > 0) {
				Object[] vvkey = vv.keySet().toArray();
				Object[] userkey = map.keySet().toArray();
				for (int i = 0; i < vvkey.length; i++) {
					for (int j = 0; j < userkey.length; j++) {
						double d = vv.get(vvkey[i].toString()) + map.get(userkey[j].toString());
						outkey.set(userkey[j].toString());
						outvalue.set(vvkey[i].toString() + ":" + df.format(d));
						context.write(outkey, outvalue);
					}
				}
			}
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "recommend:GetUserMovieA");
		job.setJarByClass(GetUserMovieA.class);
		Path cachePath = new Path(otherArgs[1]);
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] arr = fs.listStatus(cachePath);
		for (FileStatus fstatus : arr) {
			Path p = fstatus.getPath();
			if (fs.isFile(p)) {
				job.addCacheFile(p.toUri());
			}
		}

		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
				SimUserMapClass.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class,
				UserDataMapClass.class);

		job.setReducerClass(ReducerClass.class);
		job.setNumReduceTasks(10);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
