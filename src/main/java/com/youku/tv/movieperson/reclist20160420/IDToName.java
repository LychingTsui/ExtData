package com.youku.tv.movieperson.reclist20160420;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class IDToName {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		static Text outkey = new Text();
		static Text outvalue = new Text();
		Map<String, String>map=new HashMap<String, String>();

		protected void map(LongWritable key, Text value, Context context) throws IOException,
		InterruptedException {
			String[] info = value.toString().split("\t");
			String[] data = info[1].split(",");
			StringBuffer buffer=new StringBuffer();
			for (String string : data) {
				String id=string.split(":")[0];
				if (map.containsKey(id)) {
					buffer.append(map.get(id)+",");
				}
				else {
					buffer.append(id+",");
				}

			}
			if (map.containsKey(info[0])) {
				outkey.set(map.get(info[0]));
			}
			else{
			outkey.set(info[0]);
			}
			outvalue.set(buffer.deleteCharAt(buffer.length()-1).toString());
			context.write(outkey, outvalue);
		}
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Path[] filePathList = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for (Path filePath : filePathList) {
				loadIndex(filePath.toString(), context);
			}
			super.setup(context);
		}
		//加入电影的详细信息，key idindex, value :movieid
		private void loadIndex(String file, Context context) throws IOException,
				InterruptedException {
			FileReader fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr);
			String line = null;
			while ((line = br.readLine()) != null) {
				MovieDataMeta meta = new MovieDataMeta(line);
				
				map.put(meta.Getid(),meta.Gettitle());
			}
		}
	}
	public static void main(String[] args) throws IOException, InterruptedException,
	ClassNotFoundException {
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "tv movie person:GetDataFromUser");
		Path cachePath = new Path(otherArgs[2]);
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] arr = fs.listStatus(cachePath);
		for (FileStatus fstatus : arr) {
			Path p = fstatus.getPath();
			if (fs.isFile(p)) {
				job.addCacheFile(p.toUri());
			}
		}
		job.setJarByClass(IDToName.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
				MapClass.class);
		job.setNumReduceTasks(1);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}


}
