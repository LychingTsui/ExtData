package com.youku.tv.usermovie.CFmovie;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.youku.tv.movieperson.reclist20160420.Utils;

public class GetRecList {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		private DecimalFormat df = new DecimalFormat("0.0000");
		//置信度，大于该值则保留
		private double minConfidence = 0.0;
		private String version = "####";
		//保存抛除可能是热点数据之后的相关推荐数据信息即 movieID1 345 等等 total 348585，来自appeartopN的信息
		private static Map<String, Double> vidTimesMap = new HashMap<String, Double>();
		//保存电影ID和与之相关的推荐的电影的ID
		private Text vid = new Text();
		private Text recVid = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			//保存来自abTimes的数据信息，movieID1,movieID2 \t 3.78,14 其中置信度是3，小于3的抛弃。
			String[] vidTimesArray = value.toString().split("\t");
			String[] vidArray = vidTimesArray[0].split(",");
			String vidFirst = "";
			String vidSecond = "";

			if (vidArray[0].split(":").length >= 2) {
				vidFirst = vidArray[0].split(":")[0];
			} else {
				vidFirst = vidArray[0];
			}

			if (vidArray[1].split(":").length >= 2) {
				vidSecond = vidArray[1].split(":")[0];
			} else {
				vidSecond = vidArray[1];
			}

			/*
			 * lift=N*N(AB)/N(A)*N(B) >1正相关 <1负相关 =1独立
			 * 利用此公式计算两个电影是否相关。符合相关的电影出现的总次数，刨除了单个电影出现次数大于600次或者小于3次的。
			 */
			double lift = vidTimesMap.get("total") * Double.parseDouble(vidTimesArray[2])
					/ (vidTimesMap.get(vidFirst) * vidTimesMap.get(vidSecond));
			if (lift <= 1) {
				return;
			}

			// 计算weight的得分
			double weight = Double.parseDouble(vidTimesArray[1])
					/ Math.sqrt(1 + vidTimesMap.get(vidFirst) * vidTimesMap.get(vidSecond));
			if (weight >= minConfidence) {

				vid.set(vidFirst);
				recVid.set(vidSecond + ":" + df.format(weight) + ":" + version);
				context.write(vid, recVid);

				vid.set(vidSecond);
				recVid.set(vidFirst + ":" + df.format(weight) + ":" + version);
				context.write(vid, recVid);

			}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			minConfidence = Double.parseDouble(context.getConfiguration().get("minConfidence"));
			version = context.getConfiguration().get("version");

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
					vidTimesMap.put(vidAndTimes[0], Double.parseDouble(vidAndTimes[1]));
				}
			} finally {
				br.close();
			}
		}
	}

	public static class ReduceClass extends Reducer<Text, Text, Text, Text> {
		private int cutoff = 0; // 0代表不截断
		private Text recList = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException {
			StringBuffer sbResult = new StringBuffer();
			for (Text value : values) {
				sbResult.append(",");
				sbResult.append(value.toString());
			}
			//对相关推荐获取的数据进行排序。
			recList.set(Utils.sortRecList(sbResult.substring(1), cutoff, 1, ",", ":"));
			context.write(key, recList);
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			if (context.getConfiguration().get("cutoff") != null) {
				cutoff = Integer.parseInt(context.getConfiguration().get("cutoff"));
			}

			super.setup(context);
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

		conf.set("minConfidence", otherArgs[3]);
		conf.set("version", otherArgs[4]);
		conf.set("cutoff", otherArgs[5]);

		Job job = Job.getInstance(conf, "recommend:GetRecList");

		Path cachePath = new Path(otherArgs[2]);
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] arr = fs.listStatus(cachePath);
		for (FileStatus fstatus : arr) {
			Path p = fstatus.getPath();
			if (fs.isFile(p)) {
				job.addCacheFile(p.toUri());
			}
		}

		job.setJarByClass(GetRecList.class);

		job.setMapperClass(MapClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setNumReduceTasks(1);

		job.setReducerClass(ReduceClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, otherArgs[0]);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}