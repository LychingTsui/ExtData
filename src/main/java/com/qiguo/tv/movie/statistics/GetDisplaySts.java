package com.qiguo.tv.movie.statistics;

import java.io.IOException;

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
import org.codehaus.jettison.json.JSONException;

import com.youku.tv.json.JSONObject;

public class GetDisplaySts {
	public static class MapClass extends Mapper<LongWritable, Text, Text, LongWritable>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			//Counter counter = context.getCounter("countDisplay", "countDisplay");
			String info[] = value.toString().split("\t", -1);
			if (info.length < 27) {
				return;
			}
			if (info[25].equals("/WEB/listDisplay") || info[25] == "/WEB/listDisplay") {

				boolean mark = getMark(info[27]);
				if (mark == true) {
					context.write(new Text(info[15]), new LongWritable(1L));
				}
			}
		}
		public static boolean getMark(String str){
			JSONObject object =null;
			try {
				if (str.startsWith("{")) {
					object = new JSONObject(str);

					if (object.has("ctg")) {
						String temp = object.get("ctg").toString();
						if (temp.startsWith("movie")) {
							return true;
						}	
					}
					else if (object.has("category")) {
						String temp = object.get("category").toString();
						if (temp.startsWith("movie")) {
							return true;
						}	
					}
				}
				else {
					str = "{" + str + "}";
					object = new JSONObject(str);
					if (object.has("ctg")) {
						String temp = object.get("ctg").toString();
						if (temp.startsWith("movie")) {
							return true;
						}	
					}
					else if (object.has("category")) {
						String temp = object.get("category").toString();
						if (temp.startsWith("movie")) {
							return true;
						}	
					}

				}
			} catch (com.youku.tv.json.JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return false;
		}
	}
	public static class ReduceClass extends Reducer<Text, LongWritable, Text, LongWritable>{
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
			long count = 0;
			for (LongWritable longWritable : values) {
				count+= longWritable.get();

			}
			context.write(key, new LongWritable(count));
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String otherArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("mapred.reduce.parallel.copies", "25");
		Job job = Job.getInstance(conf, "GetDisplayStatics");
		job.setJarByClass(GetDIspalyStatistics.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, MapClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setReducerClass(ReduceClass.class);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0:1);
	}
}
