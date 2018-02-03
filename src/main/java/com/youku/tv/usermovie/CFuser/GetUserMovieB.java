package com.youku.tv.usermovie.CFuser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
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

public class GetUserMovieB {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		static Text outkey = new Text(), outvalue = new Text();

		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String[] info = value.toString().split("\t");
			outkey.set(info[0]);
			outvalue.set(info[1]);
			context.write(outkey, outvalue);
		}
	}

	public static class UserDataMapClass extends Mapper<LongWritable, Text, Text, Text> {
		static Text outkey = new Text(), outvalue = new Text();
		static HashMap<String, String> map =new HashMap<String, String>();
		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String[] info = value.toString().split("\t");
			String vv = "", click = "";
			if (info[1].length() > 3) {
				vv = info[1].substring(3);
			}
			if (info[2].length() > 6) {
				click = info[2].substring(6);
			}
			if (map.containsKey(info[0])) {
				outkey.set(map.get(info[0]));
			}
			//outkey.set(info[0]);
			if (click.length() > 0) {
				String[] data = click.split(",");
				for (int i = 0; i < data.length; i++) {
					String id = data[i].split(":")[0];
					outvalue.set("vv" + id);
					context.write(outkey, outvalue);
				}
			}
			if (vv.length() > 0) {
				String[] data = vv.split(",");
				for (int i = 0; i < data.length; i++) {
					String[] temp = data[i].split(":");
					outvalue.set("vv" + temp[0]);
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
		String[] info = StringUtils.splitPreserveAllTokens(line, "\t");
		if (info.length==5) {
			map.put(info[0], info[4]);
	      }
	    }
	  }
	}

	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
		static Text outvalue = new Text();
		static Text outkey = new Text();
		static DecimalFormat df = new DecimalFormat("0.000");
		static HashMap<String, String> maps =new HashMap<String, String>();
		static String version = ":C:203";

		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException {
			HashMap<String, Double> map = new HashMap<String,Double>();
			ArrayList<String> list = new ArrayList<String>();
			for (Text text : values) {
				String data = text.toString();
				if (data.startsWith("vv")) {
					if (!list.contains(data.substring(2))) {
						list.add(data.substring(2));
					}
				} else {
					String[] info = data.split(":");
					double d = Double.valueOf(info[1]);
					if (map.containsKey(info[0])) {
						d += map.get(info[0]);
					}
					map.put(info[0], d);
				}
			}

			StringBuffer sBuffer = new StringBuffer();
			Object[] obj = map.keySet().toArray();
			for (int i = 0; i < obj.length; i++) {
				if (!list.contains(obj[i].toString())) {
					sBuffer.append(",");
					sBuffer.append(obj[i].toString()).append(":");
					sBuffer.append(df.format(map.get(obj[i].toString())));
					sBuffer.append(version);
				}
			}

			if (sBuffer.length() > 1) {
				outvalue.set(Utils.sortRecList(sBuffer.substring(1), 100, 1, ",", ":"));
				if (maps.containsKey(key.toString())) {
					outkey.set(maps.get(key.toString()));
					context.write(outkey,outvalue);
				}
				//context.write(key, outvalue);
			}
		}
		
	private void loadIndex(String file,Context context)throws IOException,
		InterruptedException {
	FileReader fr = new FileReader(file);
	BufferedReader br = new BufferedReader(fr);
	String line = null;
	while ((line = br.readLine()) != null) {
		String[] info = StringUtils.splitPreserveAllTokens(line, "\t");
		if (info.length==5) {
			maps.put(info[4], info[0]);
	    	}
		}
	 }

	@Override
	protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		Path[] filePathList = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		for (Path filePath : filePathList) {
			loadIndex(filePath.toString(), context);
		}
	}
	
	}

	public static void main(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "recommend:GetUserMovieB");
		job.setJarByClass(GetUserMovieB.class);
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
				MapClass.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class,
				UserDataMapClass.class);

		job.setReducerClass(ReducerClass.class);
		job.setNumReduceTasks(1);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
