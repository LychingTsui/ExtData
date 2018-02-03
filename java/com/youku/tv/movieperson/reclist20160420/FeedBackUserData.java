package com.youku.tv.movieperson.reclist20160420;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

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

public class FeedBackUserData {
	//统计用户反馈已看过电影信息
	public static class UserLikeMapClass extends Mapper<LongWritable, Text, Text, Text> {
		static Text outkey = new Text(), outvalue = new Text();
		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String info = value.toString();
			if (info.length() > 0) {
				String[] data = info.split("\t");
				if (data.length<2) {
					return;
				}
				else {
					outkey.set(data[0]);
					//like+movieID1,+movieID2...
					outvalue.set("like" + data[1]);
					context.write(outkey, outvalue);
				}
			}
		}
	}

	public static class UserUnlikeMapClass extends Mapper<LongWritable, Text, Text, Text> {
		static Text outkey = new Text(), outvalue = new Text();

		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String info = value.toString();
			if (info.length() > 0) {
				String[] data = info.split("\t");
				outkey.set(data[0]);
				//unlike+movieID1,+movieID2...
				outvalue.set("unlike" + data[1]);
				context.write(outkey, outvalue);
			}
		}
	}
	//sourcedata数据
	public static class DataMapClass extends Mapper<LongWritable, Text, Text, Text> {
		static Text outkey = new Text(), outvalue = new Text();

		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String[] info = value.toString().split("\t");
			if (info.length<2) {
				return;
			}
			else {
				outkey.set(info[0]);
				//data+vv格式数据\t+click数据格式\t+show格式数据
				outvalue.set("data" + info[1] + "\t" + info[2] + "\t" + info[3]);
				context.write(outkey, outvalue);
			}
			
		}
	}

	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
		static Text outvalue = new Text();

		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException {
			String vvlog = null;
			ArrayList<String> likeid = new ArrayList<String>(), unlikeid = new ArrayList<String>();

			for (Text text : values) {
				String info = text.toString();
				//添加用户喜爱的电影信息
				if (info.startsWith("like")) {
					if (info.length()>4) {
						String[] like = info.substring(4).split(",");
						for (int i = 0; i < like.length; i++) {
							likeid.add(like[i]);
						}
					}
					
				}
				//添加用户不喜欢电影信息
				else if (info.startsWith("unlike")) {
					if (info.length()>6) {
						String[] unlike = info.substring(6).split(",");
						for (int i = 0; i < unlike.length; i++) {
							unlikeid.add(unlike[i]);
						}
					}
					
				}
				//添加用户行为数据：包括vv，click show
				else if (info.startsWith("data")) {
					if (info.length()>4) {
						vvlog = info.substring(4);
					}
					
				}
			}
			if (vvlog != null) {
				String vv="",click="",show="";
				if (likeid.size() > 0 || unlikeid.size() > 0) {
					String[] data = vvlog.split("\t");
					if (data[0].length()>3) {
						//占位符vv：
						 vv = data[0].substring(3);
					}
					if (click.length()>6) {
						//占位符click：
						click = data[1].substring(6);
					}
					if (data[2]!=null) {
						 show = data[2];
					}
					
					String[] vvdata = vv.split(",");

					double rate = 0;
					int num = 0;
					StringBuffer sBuffer = new StringBuffer();
					for (int i = 0; i < vvdata.length; i++) {
						String[] temp = vvdata[i].split(":");
						//统计在data中没有被用户标注为unlike的电影权重
						if (!unlikeid.contains(temp[0])) {
							sBuffer.append(",").append(vvdata[i]);
							//如果不存在，取播放频次
							double playrate = Math.min(
									//temp[1]表示的是播放时间或者播放位置，temp［2］表示的是播放次数
									Double.valueOf(temp[2]) / Double.valueOf(temp[1]), 1.0);
							rate += playrate;
							//该电影已经加大权重，因此在likeID除去该电影
							likeid.remove(temp[0]);
							num++;
						}
					}

					java.text.SimpleDateFormat format = new java.text.SimpleDateFormat("yyyyMMdd");
					String date = format.format(new Date());
					if (likeid.size() > 0) {
						//取均值，同时纪录用户喜爱的电影纪录
						//vv:c2653cd1d8ce48274753a2c5c3e45a71:1:1:20160715,09e7d91f7748200769c05f4cdaf337fe:1:1:20160710	click:	show:
						rate = rate / num;
						for (int i = 0; i < likeid.size(); i++) {
							sBuffer.append(",");
							sBuffer.append(likeid.get(i)).append(":");
							sBuffer.append("1").append(":");
							sBuffer.append(rate).append(":");
							
							sBuffer.append(date);
						}
					}
					if (sBuffer.length()>1) {
						vv = "vv:" + sBuffer.substring(1);
					}
					
					sBuffer.setLength(0);
					if (unlikeid.size() > 0) {
						for (int i = 0; i < unlikeid.size(); i++) {
							sBuffer.append(",");
							sBuffer.append(unlikeid.get(i)).append(":");
							sBuffer.append("1").append(":");
							sBuffer.append(date);
						}
					
					if (click.length() > 0) {
						click = "click:" + click + sBuffer.toString();
					} else {
						
						click = "click:" + sBuffer.substring(1);
					}
					 }
					vvlog = vv + "\t" + click + "\t" + show;					
				}
				outvalue.set(vvlog);
				context.write(key, outvalue);
			}
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException {
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		Job job = Job.getInstance(conf, "tv person movie:FeedBackUserData");
		job.setJarByClass(FeedBackUserData.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class,
				UserLikeMapClass.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[2]), TextInputFormat.class,
				UserUnlikeMapClass.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
				DataMapClass.class);

		job.setReducerClass(ReducerClass.class);
		job.setNumReduceTasks(10);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
