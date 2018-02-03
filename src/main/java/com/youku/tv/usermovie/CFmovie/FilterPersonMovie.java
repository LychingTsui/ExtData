package com.youku.tv.usermovie.CFmovie;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class FilterPersonMovie {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		private static Map<String, Double> vidTimesMap = new HashMap<String, Double>();
		List<String>list=new ArrayList<String>();
		private Text outkey = new Text();
		private Text outvalue = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException,
		InterruptedException {
			String []info=value.toString().split("\t");
			
				StringBuffer buffer=new StringBuffer();
				String str[]=info[1].split(",");
				for (String string : str) {
					if (list.contains(string.split(":")[0])&&string.split(":")[0].length()==32) {
						buffer.append(string+",");
					}
				}
				outkey.set(info[0]);
				if (buffer.length()>1) {
					outvalue.set(buffer.deleteCharAt(buffer.length()-1).toString());
					context.write(outkey, outvalue);
				}
				
			
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {

			Path[] filePathList = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for (Path filePath : filePathList) {
				loadIndex(filePath.toString());
			}

			super.setup(context);
		}

		private void loadIndex(String file) throws IOException {
			BufferedReader br = new BufferedReader(new FileReader(file));
			try {
				String line = null;
				while ((line = br.readLine()) != null) {
					String[] vid = line.split("\t");
					list.add(vid[0]);
				}
			} finally {
				br.close();
			}
		}
	}
	public static void main(String[] args) throws IOException, InterruptedException,
	ClassNotFoundException {
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "tv movie:FilterOldmovie");


		Path cachePath = new Path(otherArgs[2]);
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] arr = fs.listStatus(cachePath);
		for (FileStatus fstatus : arr) {
			Path p = fstatus.getPath();
			if (fs.isFile(p)) {
				job.addCacheFile(p.toUri());
			}
		}

		job.setJarByClass(FilterPersonMovie.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
				MapClass.class);

		job.setNumReduceTasks(1);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
