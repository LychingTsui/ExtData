package com.youku.tv.movieperson.reclist20160420;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class GetDataFromLog {
	public static class VVMapClass extends Mapper<LongWritable, Text, Text, Text> {
		//整理VV日志的信息
		static Text outkey = new Text();
		static Text outvalue = new Text();
		static Map<String, String>map=new HashMap<String, String>();
		protected void map(LongWritable key, Text value, Context context) throws IOException,
		InterruptedException {
			 Counter c = context.getCounter("countvv", "countvv");
			String[] info = value.toString().split("\t");
			//抛弃不符合的guid
			if (info[0].equals("00:00:00:00:00:00")||info[0].equals("")) {
				return;
			}
			for (int j = 1; j < info.length; j++) {
				long times = 0, date = 0;
				String[] data = info[j].split(",");
				if (data.length<3) {
					return;
				}
				for (int i = 2; i < data.length; i++) {
					//20160718:3
					String[] temp = data[i].split(":");
					//检验数据的合法性
					if (temp.length<2) {
						return;
					}
					//检验数据的合法性
					if (temp[0].length()=="20160510".length()) {
						c.increment(1);
						if (date < Long.valueOf(temp[0])) {
							date = Long.valueOf(temp[0]);
							times += Long.valueOf(temp[1]);
						}
					}

				}
				//如果集合中包含此mac，说明该用户已经升级，对其过去的日志进行整理到统一版本，检查可能需要归并的日志信息
				if (!info[0].equals("")) {
					if (info[0].length()=="00:00:00:00:00:00".length()) {
						
					
					String sb=info[0].toUpperCase();
					// 如果map集合中存在该ID，证明该用户在3.74之前的版本播放过电影，进而需要合并
					if (map.containsKey(sb)&&!map.get(sb).equals("")) {
						outkey.set(map.get(sb));
						//vv+movieid+pt+frenq+date
						outvalue.set("vv" + data[0] + ":" + data[1] + ":" + times + ":" + date);
						context.write(outkey, outvalue);
					}
					//该用户并未升级到最新版本，所以仍将使用Mac进行个性化推荐
					else{
						outkey.set(info[0]);
						outvalue.set("vv" + data[0] + ":" + data[1] + ":" + times + ":" + date);
						context.write(outkey, outvalue);
					}

				}
					else{
						outkey.set(info[0]);
						outvalue.set("vv" + data[0] + ":" + data[1] + ":" + times + ":" + date);
						context.write(outkey, outvalue);
					}
				}
				//该用户在3.73之前的版本没有信息，直接使用的是3.74版本的guid
				


			}
		}
		@Override
		//guid与Mac映射
		protected void setup(Context context) throws IOException, InterruptedException {
			Path[] filePathList = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for (Path filePath : filePathList) {
				loadIndex(filePath.toString(), context);
			}
			super.setup(context);
		}

		private void loadIndex(String file, Context context) throws IOException,
		InterruptedException {
			FileReader fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr);
			String line = null;
			while ((line = br.readLine()) != null) {
				String str[]=line.split("\t");
				if (str.length==2&&!str[0].equals("")) {
					String temp[]=str[1].split(",");
					map.put(temp[0],str[0]);
				}

			}
		}
	}
 //整理click日志信息
	public static class ClickMapClass extends Mapper<LongWritable, Text, Text, Text> {
		static Text outkey = new Text();
		static Text outvalue = new Text();
		static Map<String, String>map=new HashMap<String, String>();
		protected void map(LongWritable key, Text value, Context context) throws IOException,
		InterruptedException {
			 Counter c = context.getCounter("countclick", "countclick");
			String[] info = value.toString().split("\t");
			//检查数据的合法性
			if (info[0].equals("00:00:00:00:00:00")) {
				return;
			}
			for (int j = 1; j < info.length; j++) {
				int times = 0, date = 0;
				String[] data = info[j].split(",");
				for (int i = 1; i < data.length; i++) {
					//数据样本：20160510:3
					String[] temp = data[i].split(":");
					if (temp.length<2) {
						return;
					}
					if (temp[0].length()=="20160510".length()) {
						if (date < Integer.valueOf(temp[0])) {
							date = Integer.valueOf(temp[0]);
							c.increment(1);
						}
						times += Integer.valueOf(temp[1]);
					}
				}
				//如果集合中包含此mac，说明该用户已经升级，对其过去的日志进行整理到统一版本，检查可能需要归并的日志信息
				if (!info[0].equals("")) {
					if (info[0].length()=="00:00:00:00:00:00".length()) {
						
					
					String sb=info[0].toUpperCase();
					// 如果map集合中存在该ID，证明该用户在3.74之前的版本播放过电影，进而需要合并
					if (map.containsKey(sb)&&!map.get(sb).equals("")) {
						outkey.set(map.get(sb));
						outvalue.set("click" + data[0] + ":" + times + ":" + date);
						context.write(outkey, outvalue);
					}
					//该用户并未升级到最新版本，所以仍将使用Mac进行个性化推荐
					else{
						outkey.set(info[0]);
						outvalue.set("click" + data[0] + ":" + times + ":" + date);
						context.write(outkey, outvalue);
					}
				}
					else{
						outkey.set(info[0]);
						outvalue.set("click" + data[0] + ":" + times + ":" + date);
						context.write(outkey, outvalue);
					}
					}
				//该用户在3.73之前的版本没有信息，直接使用的是3.74版本的guid
				

			}
		}
		@Override
		//建立Mac与guid映射
		protected void setup(Context context) throws IOException, InterruptedException {
			Path[] filePathList = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for (Path filePath : filePathList) {
				loadIndex(filePath.toString(), context);
			}
			super.setup(context);
		}

		private void loadIndex(String file, Context context) throws IOException,
		InterruptedException {
			FileReader fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr);
			String line = null;
			while ((line = br.readLine()) != null) {
				String str[]=line.split("\t");
				if (str.length==2&&!str[0].equals("")) {
					//str[1]mac str[0] guid
					String temp[]=str[1].split(",");
					map.put(str[0],str[0]);
				}

			}
		}

	}
  //整理show的日志，暂时未使用
	public static class ShowMapClass extends Mapper<LongWritable, Text, Text, Text> {
		static Text outkey = new Text();
		static Text outvalue = new Text();

		protected void map(LongWritable key, Text value, Context context) throws IOException,
		InterruptedException {
			String[] info = value.toString().split("\t");
			for (int j = 1; j < info.length; j++) {
				int times = 0, date = 0;
				String[] data = info[j].split(",");
				for (int i = 1; i < data.length; i++) {
					String[] temp = data[i].split(":");
					if (date < Integer.valueOf(temp[0])) {
						date = Integer.valueOf(temp[0]);
					}
					times += Integer.valueOf(temp[1]);
				}
				outkey.set(info[0]);
				outvalue.set("show" + data[0] + ":" + times + ":" + date);
				context.write(outkey, outvalue);
			}
		}
	}

	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
		static Text outvalue = new Text();

		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
		InterruptedException {
			//整理vv click show的日志
			StringBuffer showBuffer = new StringBuffer();
			StringBuffer clickBuffer = new StringBuffer();
			StringBuffer vvBuffer = new StringBuffer();
			for (Text text : values) {
				String info = text.toString();
				if (info.startsWith("vv")) {
					vvBuffer.append(",").append(info.substring(2));
				} else if (info.startsWith("click")) {
					clickBuffer.append(",").append(info.substring(5));
				} else if (info.startsWith("show")) {
					showBuffer.append(",").append(info.substring(4));
				}
			}
			//统计每一位用户的所有vv纪录
			String vv = "vv:";
			if (vvBuffer.length() > 1) {
				vv += Utils.sortRecList(vvBuffer.substring(1), 0, 3, ",", ":");
			}
			//统计每一位用户所有的click纪录
			String click = "click:";
			if (clickBuffer.length() > 1) {
				click += Utils.sortRecList(clickBuffer.substring(1), 0, 2, ",", ":");
			}
			//统计每一位用户的所有show纪录
			String show = "show:";
			if (showBuffer.length() > 1) {
				show += Utils.sortRecList(showBuffer.substring(1), 0, 2, ",", ":");
			}

			outvalue.set(vv + "\t" + click + "\t" + show);
			context.write(key, outvalue);
		}
	}

public static void main(String[] args) throws IOException, InterruptedException,
	ClassNotFoundException, ParseException {
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		conf.set("mapred.child.java.opts", "-XX:-UseGCOverheadLimit");
		conf.set("mapreduce.map.java.opts", "-Xmx4096m");
		conf.set("mapreduce.reduce.self.java.opts", "-Xmx2048m");
		conf.set("mapreduce.child.java.opts", "-Xmx5024m");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String vvpath = otherArgs[0];
		String clickpath = otherArgs[1];
		String date = otherArgs[2];
		String period = otherArgs[3];

		Job job = Job.getInstance(conf, "tv movie person:GetDataFromLog");
		Path cachePath = new Path(otherArgs[4]);
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] arr = fs.listStatus(cachePath);
		for (FileStatus fstatus : arr) {
			Path p = fstatus.getPath();
			if (fs.isFile(p)) {
				job.addCacheFile(p.toUri());
			}
		}
		job.setJarByClass(GetDataFromLog.class);
		//获取当前时间为初始化时间
		Calendar start = Calendar.getInstance();
		Calendar end = Calendar.getInstance();
		//设置当前时间的样式
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        //格式化当前时间的样式
		start.setTime(sdf.parse(date));
		start.add(Calendar.DAY_OF_MONTH, 0);
        //格式化当前时间的样式
		end.setTime(sdf.parse(date));
		end.add(Calendar.DAY_OF_MONTH, -Integer.parseInt(period));

		while (start.after(end)) {

			MultipleInputs.addInputPath(job, new Path(vvpath + sdf.format(start.getTime())),
					TextInputFormat.class, VVMapClass.class);
			MultipleInputs.addInputPath(job, new Path(clickpath + sdf.format(start.getTime())),
					TextInputFormat.class, ClickMapClass.class);

			start.add(Calendar.DAY_OF_MONTH, -1);
		}

		job.setNumReduceTasks(10);
		job.setReducerClass(ReducerClass.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
