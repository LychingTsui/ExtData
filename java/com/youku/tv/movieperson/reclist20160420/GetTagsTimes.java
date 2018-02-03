package com.youku.tv.movieperson.reclist20160420;

import java.io.IOException;
import java.text.DecimalFormat;

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

public class GetTagsTimes {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		static Text outkey = new Text();
		static Text outvalue = new Text();
		static double interal = 0;

		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			MovieDataMeta meta = new MovieDataMeta(value.toString());
			
			double d = 1.0;
			if (meta.Gettags().length() > 0) {
				String[] info = meta.Gettags().split(",");
				for (int i = 0; i < info.length; i++) {
					/*计算每个标签的权重*/
					double sim = d - i * interal;
					outkey.set(info[i]);
					outvalue.set(sim + "");
					context.write(outkey, outvalue);
				}
			}
		}
		//设置加权计算的值
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			interal = Double.valueOf(context.getConfiguration().get("interal"));
			super.setup(context);
		}

	}

	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
		static DecimalFormat df = new DecimalFormat("0.0");//格式化double类型
		static double min = 0;

		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException {
			double time = 0;
			for (Text text : values) {
				time += Double.valueOf(text.toString());
			}
			String outkey = key.toString().replaceAll(":", "").replaceAll("：", "");
			if (time > min) {//统计最终的标签权重信息
				context.write(new Text(outkey), new Text(df.format(time)));
			}
		}
		//获取min的初始值
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			min = Double.valueOf(context.getConfiguration().get("min"));
			super.setup(context);
		}

	}

	public static void main(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException {
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("interal", otherArgs[2]);
		conf.set("min", otherArgs[3]);

		Job job = Job.getInstance(conf, "tv movie:GetTagsTimes");
		job.setJarByClass(GetTagsTimes.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
				MapClass.class);

		job.setNumReduceTasks(1);
		job.setReducerClass(ReducerClass.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
