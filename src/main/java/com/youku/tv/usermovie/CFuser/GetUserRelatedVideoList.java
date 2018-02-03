package com.youku.tv.usermovie.CFuser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.net.imap.IMAP;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class GetUserRelatedVideoList {

	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		private Text myKey = new Text();
		private Text myValues = new Text();
		private Map<String, String> map = new HashMap<String, String>();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String[] elems = value.toString().split("\t");
			if (elems[1].length() > 3) {
				String[] vvs = elems[1].substring(3).split(",");
				if (map.containsKey(elems[0])) {
					myKey.set(map.get(elems[0]));
				}
				
				for (int i = 0; i < vvs.length; i++) {
					String[] info = vvs[i].split(":");
					myValues.set(info[0]);  //  key=movieId value=guid;
					context.write(myValues,myKey);
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
	public static class ReduceClass extends Reducer<Text, Text, Text, NullWritable> {
		private Text vidList = new Text();
		private int filterMaxListLen = 600;

		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException {
			Counter counter1 = context.getCounter("counter1", "counter1");
			Counter counter2 = context.getCounter("counter2", "counter2");
			TreeMap<String, String> vidsTreeMap = new TreeMap<String, String>();
			for (Text val : values) {
				String vid = val.toString();				
				vidsTreeMap.put(vid, val.toString());
			}

			if (vidsTreeMap.size() <= 1 || vidsTreeMap.size() > filterMaxListLen) {
				if (vidsTreeMap.size() <= 1) {
					counter1.increment(1);//15W
//					StringBuffer resultStr = new StringBuffer();
//					for (Object obj : vidsTreeMap.keySet()) {
//						resultStr.append(vidsTreeMap.get(obj));
//					}
//					vidList.set(key+"\t"+resultStr.toString());
//					context.write(vidList, NullWritable.get());
				}
					
				else {
					counter2.increment(1);//768
				}
				return;
			}

			StringBuffer resultStr = new StringBuffer();
			for (Object obj : vidsTreeMap.keySet()) {
				resultStr.append(",");
				resultStr.append(vidsTreeMap.get(obj));
			}
			vidList.set(resultStr.substring(1));
			context.write(vidList, NullWritable.get());
		}

		protected void setup(Context context) throws IOException, InterruptedException {
			if (context.getConfiguration().get("max.filterListSize") != null) {
				filterMaxListLen = Integer.parseInt(context.getConfiguration().get(
						"max.filterListSize"));
			}
			super.setup(context);
		}
	}

	/**
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length == 4) {
			conf.set("max.filterListSize", otherArgs[3]);
		}
		
		conf.set("mapred.job.queue.name", "mouse");

		Job job = Job.getInstance(conf, "recommend:GetUserRelatedVideoList");
		Path cachePath = new Path(otherArgs[2]);
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] arr = fs.listStatus(cachePath);
		for (FileStatus fstatus : arr) {
			Path p = fstatus.getPath();
			if (fs.isFile(p)) {
				job.addCacheFile(p.toUri());
			}
		}
		job.setJarByClass(GetUserRelatedVideoList.class);

		FileInputFormat.setInputPaths(job, otherArgs[0]);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setNumReduceTasks(10);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
