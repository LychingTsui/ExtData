package com.youku.tv.movieperson.reclist20160420;

import java.io.IOException;
import java.util.ArrayList;

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

import com.youku.tv.json.JSONArray;
import com.youku.tv.json.JSONException;
import com.youku.tv.json.JSONObject;

public class GetTagAndArgsFromRecListSource {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		static Text outkey = new Text();
		static Text outvalue = new Text();

		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String info = value.toString();
			try {
				JSONObject jObject = new JSONObject(info);
				String title = jObject.getString("title");
				String arg = jObject.get("args").toString();

				StringBuffer tagBuffer = new StringBuffer();
				if (jObject.get("tags").toString().length() > 1) {
					JSONArray tags = jObject.getJSONArray("tags");
					for (int i = 0; i < tags.length(); i++) {
						String tmp = tags.get(i).toString().replaceAll(":", "：")
								.replaceAll(",", "").replaceAll("，", ",").replaceAll(" ", "")
								.replaceAll("\t", "").replaceAll("\\.", "").trim();
						if (tmp.length() > 0) {
							tagBuffer.append(",").append(tmp);
						}
					}
				}

				outkey.set(title);
				if (tagBuffer.length() > 1) {
					String[] data = tagBuffer.substring(1).split(",");
					StringBuffer sBuffer = new StringBuffer();
					for (int i = 0; i < data.length; i++) {
						if (data[i].length() > 0) {
							sBuffer.append(",").append(data[i]);
						}
					}
					outvalue.set("data" + arg + "\t" + sBuffer.substring(1));
				} else {
					outvalue.set("data" + arg);
				}

				context.write(outkey, outvalue);

			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
	}

	public static class MovieMapClass extends Mapper<LongWritable, Text, Text, Text> {
		static Text outkey = new Text();
		static Text outvalue = new Text();

		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			MovieDataMeta mDataMeta = new MovieDataMeta(value.toString());
			outkey.set(mDataMeta.Gettitle());
			outvalue.set("movie" + value.toString());
			context.write(outkey, outvalue);

		}
	}

	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
		static Text outkey = new Text();
		static Text outvalue = new Text();

		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException {
			int args = 0;
			String tags = "";
			ArrayList<String> movie = new ArrayList<String>();
			for (Text text : values) {
				String info = text.toString();
				if (info.startsWith("data")) {
					String[] data = info.substring(4).split("\t");
					args = Integer.valueOf(data[0]);
					if (data.length > 1) {
						tags = data[1];
					}
				} else if (info.startsWith("movie")) {
					movie.add(info.substring(5));
				}
			}
			for (int i = 0; i < movie.size(); i++) {
				MovieDataMeta meta = new MovieDataMeta(movie.get(i));
				meta.Setargs(args);
				meta.Settags(tags);
				outkey.set(meta.Getid());
				outvalue.set(meta.ToValueString());
				context.write(outkey, outvalue);
			}
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException {
		Configuration conf = new Configuration();
		// conf.set("mapred.job.queue.name", "mouse");
		conf.set("mapred.reduce.parallel.copies", "25");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "tv movie:GetTagAndArgsFromRecListSource");

		job.setJarByClass(GetTagAndArgsFromRecListSource.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
				MapClass.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class,
				MovieMapClass.class);

		job.setNumReduceTasks(1);
		job.setReducerClass(ReducerClass.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
