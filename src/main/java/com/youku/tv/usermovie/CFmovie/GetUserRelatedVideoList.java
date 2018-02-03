package com.youku.tv.usermovie.CFmovie;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class GetUserRelatedVideoList {
//统计用户的观看纪录数据，如果一步电影出现大于200次的时候证明该电影是热点数据，剔除。
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		private Text myKey = new Text();
		private Text myValues = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {//输入的一条记录：guid\t vv:movieid:movielength:times:time ,movieid:movielength:times:time \t click:movieid:times:time,show:
			String[] elems = value.toString().split("\t");
			if (elems[1].length() > 3) {
				//整理vv的行为数据
				String[] vvs = elems[1].substring(3).split(",");
				myKey.set(elems[0]);
				for (int i = 0; i < vvs.length; i++) {
					String[] info = vvs[i].split(":");
					myValues.set(info[0] + ":" + info[3]);
					context.write(myKey, myValues);
				}

			}
		}
	}

	public static class ReduceClass extends Reducer<Text, Text, Text, NullWritable> {
		private Text vidList = new Text();
		private int filterMaxListLen = 600;

		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException {
			TreeMap<String, String> vidsTreeMap = new TreeMap<String, String>();
			for (Text val : values) {
				String[] elems = val.toString().split(":");
				String vid;
				vid = elems[0];
				vidsTreeMap.put(vid, val.toString());
			}

			if (vidsTreeMap.size() <= 1 || vidsTreeMap.size() > filterMaxListLen) {
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

		if (otherArgs.length == 3) {
			conf.set("max.filterListSize", otherArgs[2]);
		}
		conf.set("mapred.job.queue.name", "mouse");

		Job job = Job.getInstance(conf, "recommend:GetUserRelatedVideoList");
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
