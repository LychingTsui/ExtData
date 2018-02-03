package com.youku.tv.usermovie.CFmovie;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

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

import com.youku.tv.movieperson.reclist20160420.MovieDataMeta;

public class TransRecListName {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		Text outkey = new Text(), outvalue = new Text();
		static HashMap<String, String> map = new HashMap<String,String>();

		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String[] info = value.toString().split("\t");
			String mkey = "", mval = "";
			if (map.containsKey(info[0])) {
				mkey = map.get(info[0]);
			} else {
				mkey = info[0];
			}
			StringBuffer sBuffer = new StringBuffer();
			String[] data = info[1].split(",");
			for (int i = 0; i < data.length; i++) {

				String[] tmp = data[i].split(":");
				if (map.containsKey(tmp[0])) {
					mval = map.get(tmp[0]);
				} else {
					mval = tmp[0];
				}
				sBuffer.append(",").append(mval).append(":").append(tmp[1]);
			}
			outkey.set(mkey);
			outvalue.set(sBuffer.substring(1));
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

		private void loadIndex(String file, Context context) throws IOException,
				InterruptedException {
			FileReader fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr);
			String line = null;
			while ((line = br.readLine()) != null) {
				MovieDataMeta meta = new MovieDataMeta(line);
				map.put(String.valueOf(meta.Getid()), meta.Gettitle());
			}
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "recommend:TransRecListName");

		Path cachePath = new Path(otherArgs[2]);
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] arr = fs.listStatus(cachePath);
		for (FileStatus fstatus : arr) {
			Path p = fstatus.getPath();
			if (fs.isFile(p)) {
				job.addCacheFile(p.toUri());
			}
		}

		job.setJarByClass(TransRecListName.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
				MapClass.class);

		job.setNumReduceTasks(1);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
