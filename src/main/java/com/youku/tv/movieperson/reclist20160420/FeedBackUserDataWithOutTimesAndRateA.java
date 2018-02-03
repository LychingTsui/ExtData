package com.youku.tv.movieperson.reclist20160420;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class FeedBackUserDataWithOutTimesAndRateA {
	//统计用户喜爱的电影信息
		public static class UserLikeMapClass extends Mapper<LongWritable, Text, Text, Text> {
			static Text outkey = new Text(), outvalue = new Text();
			static Map<String, String>map=new HashMap<String, String>();
			protected void map(LongWritable key, Text value, Context context) throws IOException,
					InterruptedException {
				String info = value.toString();
				if (info.length() > 0) {
					String[] data = info.split("\t");
					if (data.length<2) {
						return;
					}
					String[] ids = data[1].split(",");
					ArrayList<String> list = new ArrayList<String>();
					for (int i = 0; i < ids.length; i++) {
						if (!list.contains(ids[i])) {
							list.add(ids[i]);
						}
					}
					if (list.size() > 0) {
						StringBuffer sBuffer = new StringBuffer();
						for (int i = 0; i < list.size(); i++) {
							sBuffer.append(",").append(list.get(i));
						}
						//如果集合中包含此mac，说明该用户已经升级，对其过去的日志进行整理到统一版本，检查可能需要归并的日志信息
						if (!data[0].equals("")&&data[0].length()=="00:00:00:00:00:00".length()) {
							String sb=data[0].toUpperCase();
							// 如果map集合中存在该ID，证明该用户在3.74之前的版本播放过电影，进而需要合并
							if (map.containsKey(sb)&&!map.get(sb).equals("")) {
								outkey.set(map.get(sb));
								//vv+movieid+pt+frenq+date
								outvalue.set("like" + sBuffer.substring(1));
								context.write(outkey, outvalue);
							}
							//该用户并未升级到最新版本，所以仍将使用Mac进行个性化推荐
							else{
								outkey.set(data[0]);
								outvalue.set("like" + sBuffer.substring(1));
								context.write(outkey, outvalue);
							}

						}
						//该用户在3.73之前的版本没有信息，直接使用的是3.74版本的guid
						else {
							outkey.set(data[0]);
							outvalue.set("like" + sBuffer.substring(1));
							context.write(outkey, outvalue);
						}

//						outkey.set(data[0]);
//						outvalue.set("like" + sBuffer.substring(1));
//						context.write(outkey, outvalue);
					}
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
		//统计用户不喜欢看的纪录
		public static class UserUnlikeMapClass extends Mapper<LongWritable, Text, Text, Text> {
			static Text outkey = new Text(), outvalue = new Text();
			static Map<String, String>map=new HashMap<String, String>();
			protected void map(LongWritable key, Text value, Context context) throws IOException,
					InterruptedException {
				String info = value.toString();
				if (info.length() > 0) {
					String[] data = info.split("\t");
					if (data.length<2) {
						return ;
					}
					String[] ids = data[1].split(",");
					ArrayList<String> list = new ArrayList<String>();
					for (int i = 0; i < ids.length; i++) {
						if (!list.contains(ids[i])) {
							list.add(ids[i]);
						}
					}
					if (list.size() > 0) {
						StringBuffer sBuffer = new StringBuffer();
						for (int i = 0; i < list.size(); i++) {
							sBuffer.append(",").append(list.get(i));
						}
						//如果集合中包含此mac，说明该用户已经升级，对其过去的日志进行整理到统一版本，检查可能需要归并的日志信息
						if (!data[0].equals("")&&data[0].length()=="00:00:00:00:00:00".length()) {
							String sb=data[0].toUpperCase();
							// 如果map集合中存在该ID，证明该用户在3.74之前的版本播放过电影，进而需要合并
							if (map.containsKey(sb)&&!map.get(sb).equals("")) {
								outkey.set(map.get(sb));
								//vv+movieid+pt+frenq+date
								outvalue.set("unlike" + sBuffer.substring(1));
								context.write(outkey, outvalue);
							}
							//该用户并未升级到最新版本，所以仍将使用Mac进行个性化推荐
							else{
								outkey.set(data[0]);
								outvalue.set("unlike" + sBuffer.substring(1));
								context.write(outkey, outvalue);
							}

						}
						//该用户在3.73之前的版本没有信息，直接使用的是3.74版本的guid
						else {
							outkey.set(data[0]);
							outvalue.set("unlike" + sBuffer.substring(1));
							context.write(outkey, outvalue);
						}

//						outkey.set(data[0]);
//						outvalue.set("unlike" + sBuffer.substring(1));
//						context.write(outkey, outvalue);
					}
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

		public static class DataMapClass extends Mapper<LongWritable, Text, Text, Text> {
			static Text outkey = new Text(), outvalue = new Text();

			protected void map(LongWritable key, Text value, Context context) throws IOException,
					InterruptedException {
				String[] info = value.toString().split("\t");
				outkey.set(info[0]);
				String vv = info[1];
				if (vv.substring(3).length() > 0) {
					//取用户的vv播放记录
					String[] data = vv.substring(3).split(",");
					HashMap<String, String> map = new HashMap<String, String>();//统计该电影vv的纪录
					HashMap<String, Double> hmap = new HashMap<String,Double>();//统计电影的权重
					for (int i = 0; i < data.length; i++) {
						String[] tmp = data[i].split(":");	
						//不考虑用户的播放次数和播放位置
						tmp[2]="1";tmp[1]="1";
						data[i]=tmp[0]+":"+tmp[1]+":"+tmp[2]+":"+tmp[3];
						double d = Double.valueOf(tmp[2]) / Double.valueOf(tmp[1]);
						if (map.containsKey(tmp[0])) {
							if (hmap.get(tmp[0]) < d) {
								hmap.put(tmp[0], d);
								map.put(tmp[0], data[i]);
							}
						} else {
							hmap.put(tmp[0], d);
							map.put(tmp[0], data[i]);
						}
					}
					StringBuffer sBuffer = new StringBuffer();
					Object[] objects = map.keySet().toArray();
					for (int i = 0; i < objects.length; i++) {
						sBuffer.append(",").append(map.get(objects[i].toString()));
					}
					vv = "vv:" + sBuffer.substring(1);
				}
	          //此处只是处理了vv
				outvalue.set("data" + vv + "\t" + info[2] + "\t" + info[3]);
				context.write(outkey, outvalue);
			}
		}

		public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
			static Text outvalue = new Text();

			protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
					InterruptedException {
				String vvlog = null;
				ConcurrentHashMap<String, String> likeid = new ConcurrentHashMap<String, String>();ConcurrentHashMap<String, String> unlikeid = new ConcurrentHashMap<String, String>();
				//添加用户like集合
				for (Text text : values) {
					String info = text.toString();
					if (info.startsWith("like")) {
						String[] like = info.substring(4).split(",");
						for (int i = 0; i < like.length; i++) {
							String sb[]=like[i].split(":");
							likeid.put(sb[0],sb[1]);
						}
					}
					//添加用户的unlike集合
					else if (info.startsWith("unlike")) {
						String[] unlike = info.substring(6).split(",");
						for (int i = 0; i < unlike.length; i++) {
							String sb[]=unlike[i].split(":");
							unlikeid.put(sb[0],sb[1]);
						}
					}
					//添加用户行为数据
					else if (info.startsWith("data")) {
						vvlog = info.substring(4);
					}
				}
				//如果用户提交了反馈信息对反馈信息整理
				if (vvlog != null) {
					if (likeid.size() > 0 || unlikeid.size() > 0) {
						String[] data = vvlog.split("\t");
						//对data数据进行细分
						String vv = data[0].substring(3), click = data[1].substring(6), show = data[2];
						String[] vvdata = vv.split(",");
						double rate = 0;
						int num = 0;
						StringBuffer sBuffer = new StringBuffer();
						for (int i = 0; i < vvdata.length; i++) {
							String[] temp = vvdata[i].split(":");
							//如果一个电影用户点击播放并没有提出不喜欢，表明该电影是一个正向因子，对该电影的权重进行计算
							
							if (!unlikeid.containsKey(temp[0])) {
								sBuffer.append(",").append(vvdata[i]);
	                           if (temp.length<3) {
								return;
							}
								double playrate = Math.min(
										Double.valueOf(temp[2]) / Double.valueOf(temp[1]), 1.0);
								rate += playrate;
								//电影的权重已经计算，因此如果用户的vv中在已经本记录中看中包含该电影，应该remove，剩余的是不在本系统看过的电影
								for (Entry<String, String>map:likeid.entrySet()) {
									
								   String log=map.getKey();
								   if (log.equals(temp[0])) {
									   likeid.remove(temp[0]);
									  
								}
								
								}
								 num++;
							}
						
						}

						//java.text.SimpleDateFormat format = new java.text.SimpleDateFormat("yyyyMMdd");
						//String date = format.format(new Date());
						//用户已看过的电影的权重计算
						if (likeid.size() > 0) {
							rate = rate / num;
							for (Entry<String, String>map:likeid.entrySet()) {
								sBuffer.append(",");
								sBuffer.append(map.getKey()).append(":");
								sBuffer.append("1").append(":");
								sBuffer.append(rate).append(":");
								sBuffer.append(map.getValue());
							}
						}
						if (sBuffer.length()>0) {
							vv = "vv:" + sBuffer.substring(1);
						}
						
						sBuffer.setLength(0);
						if (unlikeid.size() > 0) {
							for (Entry<String, String>map:unlikeid.entrySet()) {
								sBuffer.append(",");
								sBuffer.append(map.getKey()).append(":");
								sBuffer.append("1").append(":");
								sBuffer.append(map.getValue());
							}
						
							
						}
						if (click.length() > 0) {
							//用户click＋不喜欢的纪录
							click = "click:" + click + sBuffer.toString();
						} else {
							if (sBuffer.length()>1) {
								click = "click:" + sBuffer.substring(1);
							}
							
						}
						vvlog = vv + "\t" + click + "\t" + show;
						outvalue.set(vvlog);
						
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
			Path cachePath = new Path(otherArgs[3]);
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] arr = fs.listStatus(cachePath);
			for (FileStatus fstatus : arr) {
				Path p = fstatus.getPath();
				if (fs.isFile(p)) {
					job.addCacheFile(p.toUri());
				}
			}
			job.setJarByClass(FeedBackUserDataWithOutTimesAndRateA.class);
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

			FileOutputFormat.setOutputPath(job, new Path(otherArgs[4]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
}
