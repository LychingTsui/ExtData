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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.youku.tv.movie.reclist20151228.MovieDataMeta;

public class GetCompletionTags {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{
		Text outkey = new Text();
		Text outvalue = new Text();
		static Map<String, String> doubanTag = new HashMap<String, String>();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			MovieDataMeta meta = new MovieDataMeta(value.toString());
			String title = meta.Gettitle();
			String tags = meta.Gettags();
			if (tags.length() <1) {
				if (doubanTag.containsKey(title)) {
					meta.Settags(doubanTag.get(title));
					outkey.set(meta.Getid());
					outvalue.set(meta.ToValueString());
					//context.write(outkey, outvalue);
				}
				else {
					outkey.set(meta.Getid());
					outvalue.set(meta.ToValueString());
					context.write(outkey, outvalue);
				}
			}
			else {
				outkey.set(meta.Getid());
				outvalue.set(meta.ToValueString());
				//context.write(outkey, outvalue);
			}
		}
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for (Path path : paths) {
				loadIndex(path.toString(), context);
			}
		}
		private void loadIndex(String file, Context context) throws IOException,
		InterruptedException {
			FileReader fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr);
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] info = line.split("\t");
				doubanTag.put(info[0], info[1]);
			}
		}
	}
	public static void main(String[] args) throws IOException, InterruptedException,
	ClassNotFoundException {
		Configuration conf = new Configuration();
		//conf.set("mapreduce.reduce.parallel.copies", "25");
		//conf.set("mapred.reduce.parallel.copies", "25");
		//conf.set("mapreduce.job.reduces", "1000");
		//conf.set("mapreduce.task.io.sort.factor", "100");
		//conf.set("mapreduce.task.io.sort.mb", "500");
//		conf.setBoolean("mapred.output.compress", true);
//		conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.BZip2Codec");
//		conf.set("mapreduce.reduce.self.java.opts", "-Xmx7120m");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "tv person movie:GettagsCompletion");
		job.setJarByClass(GetCompletionTags.class);
		Path cachePath = new Path(otherArgs[2]);
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
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
