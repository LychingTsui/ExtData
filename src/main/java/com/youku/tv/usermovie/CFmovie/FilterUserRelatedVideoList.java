package com.youku.tv.usermovie.CFmovie;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class FilterUserRelatedVideoList {
//继续对相关电影进行过滤，当一个电影出现在上一步abTimes中已经计算出来的数据，那么保留,置信度是一部电影被用户观看的次数是30次以上。
	public static class MapClass extends Mapper<LongWritable, Text, Text, NullWritable> {
		//保存缓存加载的数据。
		private Set<String> vidSet = new HashSet<String>();
		private Text newKey = new Text();
		private NullWritable newValue = NullWritable.get();

		public void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String[] vidList = value.toString().split(",");
			StringBuffer newKeyStr = new StringBuffer();
			for (String vid : vidList) {
				String[] elems = vid.split(":");
				if (vidSet.contains(elems[0])) {
					newKeyStr.append(",");
					newKeyStr.append(vid);
				}
			}
			if (newKeyStr.length() > 1) {
				newKey.set(newKeyStr.substring(1));
				context.write(newKey, newValue);
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
					String[] vidAndTimes = line.split("\t");
					if (!vidAndTimes[0].equals("total")) {
						vidSet.add(vidAndTimes[0]);
					}
				}
			} finally {
				br.close();
			}
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
		Job job = Job.getInstance(conf, "recommend:FilterUserRelatedVideoList");

		Path cachePath = new Path(otherArgs[2]);
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] arr = fs.listStatus(cachePath);
		for (FileStatus fstatus : arr) {
			Path p = fstatus.getPath();
			if (fs.isFile(p)) {
				job.addCacheFile(p.toUri());
			}
		}

		job.setJarByClass(FilterUserRelatedVideoList.class);

		FileInputFormat.setInputPaths(job, otherArgs[0]);

		job.setMapperClass(MapClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setNumReduceTasks(0);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
