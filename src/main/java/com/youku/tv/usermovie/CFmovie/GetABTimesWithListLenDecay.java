package com.youku.tv.usermovie.CFmovie;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class GetABTimesWithListLenDecay {

	public static class MapClass extends Mapper<LongWritable, Text, Text, DoubleWritable> {
		private Text cooccurVids = new Text();
		private DoubleWritable abTimes = new DoubleWritable();
		//对数据进一步整理，去掉不满足topN 的数据。
		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String[] vidArray = value.toString().split(",");
			int len = vidArray.length;
			// 列表长度衰减，列表长度越长，排在后面的数据权重越小。
			double abTimesWithLenDecay = 1 / Math.log(3 + len);

			for (int i = 0; i < len - 1; i++) {
				for (int j = i + 1; j < len; j++) {
					StringBuffer sb = new StringBuffer();

					// 为了做到通用，最早以前的格式是没有频道和时间戳
					String[] iElems = vidArray[i].split(":");
					String[] jElems = vidArray[j].split(":");
					if (iElems.length >= 2 && jElems.length >= 2) {
						sb.append(iElems[0]);
						sb.append(",");
						sb.append(jElems[0]);
					} else {
						sb.append(vidArray[i]);
						sb.append(",");
						sb.append(vidArray[j]);
					}
					//相关电影 以及相关的权重
					cooccurVids.set(sb.toString());
					abTimes.set(abTimesWithLenDecay);
					context.write(cooccurVids, abTimes);
				}
			}
		}
	}

	public static class ReducerClass extends Reducer<Text, DoubleWritable, Text, Text> {
		private DecimalFormat df = new DecimalFormat("0.0000");
		private Text abTimes = new Text();
		private double filterABTimes = 0.0;

		protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			//对权重进行汇总，同时统计两个电影之间出现的次数
			double abTimesAdjust = 0.0;
			double abTimesNatural = 0.0;
			for (DoubleWritable value : values) {
				abTimesAdjust += value.get();
				abTimesNatural += 1.0;
				
			}
			//如果权重大于置信度时候保留
			if (abTimesAdjust >= filterABTimes) {
				StringBuffer sbValue = new StringBuffer();
				sbValue.append(df.format(abTimesAdjust));
				sbValue.append("\t");
				sbValue.append(abTimesNatural);

				abTimes.set(sbValue.toString());
				context.write(key, abTimes);
			}
		}
		//获取动态设置的置信度
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			filterABTimes = Double.parseDouble(context.getConfiguration().get("filterABTimes"));
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
		conf.set("filterABTimes", otherArgs[2]);

		conf.setInt("mapred.max.split.size", 128 * 1024 * 1024);
		conf.setInt("io.sort.mb", 512);

		conf.setFloat("mapred.job.shuffle.input.buffer.percent", 0.75f);
		conf.setFloat("mapred.job.shuffle.merge.percent", 0.8f);

		Job job = Job.getInstance(conf, "recommend:GetABTimesWithListLenDecay");
		job.setJarByClass(GetABTimesWithListLenDecay.class);

		FileInputFormat.setInputPaths(job, otherArgs[0]);

		job.setMapperClass(MapClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		// 这个地方不能用combine，因为reduce阶段要统计相同key出现次数
		job.setNumReduceTasks(10);
		job.setReducerClass(ReducerClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
