package com.youku.tv.movieperson.reclist20160420;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ReverseUserShowDataB {
	public static class UserMovieMapClass extends Mapper<LongWritable, Text, TextPair, Text> {
		private Text outval = new Text();
		private TextPair outkey = new TextPair();

		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String[] info = StringUtils.split(value.toString(), '\t');
			String[] data = StringUtils.split(info[1], ',');
			outkey.setText(info[0]);
			outkey.setValue("right");
			for (int i = 0; i < data.length; i++) {
				outval.set(data[i]);
				context.write(outkey, outval);
			}
		}
	}

	public static class FeedBackMapClass extends Mapper<LongWritable, Text, TextPair, Text> {
		private Text outval = new Text();
		private TextPair outkey = new TextPair();

		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String[] info = StringUtils.split(value.toString(), '\t');
			outkey.setText(info[0]);
			outkey.setValue("left");
			String[] data = StringUtils.split(info[1], ',');
			for (int i = 0; i < data.length; i++) {
				outval.set("left" + data[i]);
				context.write(outkey, outval);
			}
		}
	}

	public static class ReducerClass extends Reducer<TextPair, Text, Text, Text> {
		private Text outkey = new Text(), outval = new Text();
		static DecimalFormat df = new DecimalFormat("0.000");
		private int typecutoff = 100;

		protected void reduce(TextPair key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> it = values.iterator();
			String fvalue;
			HashMap<String, Double> map = new HashMap<String,Double>();
			StringBuffer sBuffer = new StringBuffer();
			while (it.hasNext()) {
				fvalue = ((Text) it.next()).toString();
				//是用户反馈的仅仅show没有点击的数据
				if (fvalue.startsWith("left")) {
					String[] info = StringUtils.split(fvalue.substring(4), ':');
					map.put(info[0], Double.valueOf(info[1]));
				}
				//抖动推荐库中的数据
				else {
					if (map.size() > 0) {
						String[] data = StringUtils.split(fvalue, ':');
						if (map.containsKey(data[0])) {
							//重新计算推荐数据中的权重
							double adjust = Math.min(map.get(data[0]), 1.0);
							double score = Double.valueOf(data[1]) * (1 - adjust);
							sBuffer.append(",");
							sBuffer.append(data[0]).append(":").append(df.format(score));
							//重新组装入库格式
							if (data.length > 2) {
								for (int i = 2; i < data.length; i++) {
									sBuffer.append(":").append(data[i]);
								}
							}

						} else {
							sBuffer.append(",").append(fvalue);
						}
					} else {
						sBuffer.append(",").append(fvalue);
					}
				}
			}
			if (sBuffer.length() > 1) {
				outval.set(Utils.sortRecList(sBuffer.substring(1), typecutoff, 1, ",", ":"));
				outkey.set(key.getText());

				context.write(outkey, outval);
			}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			typecutoff = Integer.valueOf(context.getConfiguration().get("typecutoff"));
			super.setup(context);
		}

	}

	public static void main(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("typecutoff", otherArgs[3]);

		Job job = Job.getInstance(conf, "tv movie:ReverseUserShowDataB");
		job.setJarByClass(ReverseUserShowDataB.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
				UserMovieMapClass.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class,
				FeedBackMapClass.class);

		job.setNumReduceTasks(20);
		job.setReducerClass(ReducerClass.class);

		job.setGroupingComparatorClass(TextComparator.class);
		job.setPartitionerClass(KeyPartitioner.class);

		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
