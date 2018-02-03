package com.youku.tv.movieperson.reclist20160420;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class GetUserRecListBtest {
	//对GetUserRecListA环节中的数据进一步处理
		public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
			static Text outkey = new Text(), outval = new Text();

			protected void map(LongWritable key, Text value, Context context) throws IOException,
					InterruptedException {
				String[] info = value.toString().split("\t");
				// String[] data = info[1].split(",");
				// for (int i = 0; i < data.length; i++) {
				// outkey.set(info[0]);
				// outval.set(data[i]);
				// context.write(outkey, outval);
				// }
				outkey.set(info[0]);
				outval.set(info[1]);
				context.write(outkey, outval);
			}
		}

		public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
			static Text outval = new Text();
			static HashMap<String, Double> datemap = new HashMap<String,Double>();//储存电影时间权重
			static HashMap<String, Double> ratemap = new HashMap<String,Double>();//储存打分权重
			static HashMap<String, String> typemap = new HashMap<String,String>();//储存打分权重
			static int cutoff = 30;
			static double type = 3.0;
			static DecimalFormat df = new DecimalFormat("0.0000");
			static String idname = "id";

			protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
					InterruptedException {
				Counter counter=context.getCounter("count","count");
				Counter counter1=context.getCounter("count","count");
				HashMap<String, Double> map = new HashMap<String,Double>();
				HashMap<String, Integer> map1 = new HashMap<String,Integer>();
				for (Text text : values) {
					//对权值进行加和计算，IDindex：权重
					String[] info = text.toString().split(":");
					double d = Double.valueOf(info[1]);
					if (map.containsKey(info[0])) {
						d += map.get(info[0]);
					}
					//权重集合重新整理 IDIndex：权重
					map.put(info[0], d);
				}

				Object[] obj = map.keySet().toArray();
				StringBuffer sBuffer = new StringBuffer();
				double temprate = Math.pow(0.65, type);//基准值
				for (int i = 0; i < obj.length; i++) {
					double d = map.get(obj[i].toString());

					if (ratemap.containsKey(obj[i].toString())) {
						//如果该用户所观看的电影在特征库打分中存在，对当前用户观看的电影与特征库进行计算
						d = d * ratemap.get(obj[i].toString());
					} else {
						//利用基准值打分计算
						d = d * temprate;
					}

					double daterate = 2;
					//如果该用户所观看的电影在特征库中存在，对当前用户观看的电影与特征库进行计算，时间间隔依据
					if (datemap.containsKey(obj[i].toString())) {
						daterate = datemap.get(obj[i].toString());
					}
					//与该电影的出版时间再次融合，时间越久，该权重越小
					d = d / daterate;
					if (typemap.containsKey(obj[i].toString())) {
						String []types=typemap.get(obj[i].toString()).split(",");
						int m=0;
						if (types.length>3) {
							m=3;
						}
						else {
							m=types.length;
						}
						for (int j=0;j<m;j++) {
							if (map1.containsKey(types[j])) {
								Integer a=map1.get(types[j]);
								if (a<20) {
									a++;
									sBuffer.append(",").append(obj[i].toString()).append(":").append(df.format(d));
								}
								
								map1.put(types[j], a);
							}
							else {
								map1.put(types[j], 1);
								sBuffer.append(",").append(obj[i].toString()).append(":").append(df.format(d));
							}
						}
					}
					//sBuffer.append(",").append(obj[i].toString()).append(":").append(df.format(d));
				}
				outval.set(Utils.sortRecList(sBuffer.substring(1), cutoff, 1, ",", ":"));
				//输出数据样例：用户guid IDIndex：98
				context.write(key, outval);
			}

			@Override
			protected void setup(Context context) throws IOException, InterruptedException {
				cutoff = Integer.valueOf(context.getConfiguration().get("cutoff"));
				type = Double.valueOf(context.getConfiguration().get("type"));
				idname = context.getConfiguration().get("idname");
				Path[] filePathList = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				for (Path filePath : filePathList) {
					loadIndex(filePath.toString(), context);
				}
				super.setup(context);
			}
	     //加载tagsarg数据，主要是电影的详细信息 key 是电影ID value包括title，type，actor，director，introduction，rating tags，date，，IDIndex等
			private void loadIndex(String file, Context context) throws IOException,
					InterruptedException {
				FileReader fr = new FileReader(file);
				BufferedReader br = new BufferedReader(fr);
				String line = null;
				while ((line = br.readLine()) != null) {
					MovieDataMeta meta = new MovieDataMeta(line);

					// String name = meta.Getid();
					String name = String.valueOf(meta.Getidindex());
					//进行逻辑判断
					if (idname.equals("title")) {
						name = meta.Gettitle().replaceAll(":", "");
					}

					if (meta.Getdate().length() > 1) {
						double daterate = 2;
						try {//判断电影年限
							daterate = Math.log(3 + Utils.DateSubYear(meta.Getdate()));
						} catch (ParseException e) {
							e.printStackTrace();
						}
						//储存Idindex 时间处理之后的值
						datemap.put(name, daterate);
					}
					if (meta.Gettype().length()>1) {
						typemap.put(name, meta.Gettype());
					}

					try {//对电影的评分进行重新加权计算
						double rate = Double.valueOf(meta.Getrating()) / 100;
						rate = Math.pow(rate, type);
						if (ratemap.containsKey(name)) {
							if (rate > ratemap.get(name)) {
								rate = ratemap.get(name);
							}
						}
						//储存打分权重
						ratemap.put(name, rate);
					} catch (Exception e) {
						continue;
					}
				}
			}
		}

		public static void main(String[] args) throws IOException, InterruptedException,
				ClassNotFoundException {
			Configuration conf = new Configuration();
			//conf.set("mapred.reduce.parallel.copies", "25");
			conf.set("mapreduce.reduce.shuffle.parallelcopies", "40");
			conf.set("mapreduce.reduce.shuffle.input.buffer.percent", "0.02");
			conf.set("mapreduce.job.reduces", "50");
			conf.set("mapreduce.task.timeout", "1800000");
			//conf.set("mapreduce.task.io.sort.factor", "100");
			//conf.set("mapreduce.task.io.sort.mb", "500");
			//conf.setBoolean("mapred.output.compress", true);
			//conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.BZip2Codec");
			conf.set("mapreduce.reduce.self.java.opts", "-Xmx7120m");
			conf.set("mapreduce.child.java.opts", "-Xmx5024m");
			conf.setBoolean("dfs.client.block.write.replace-datanode-on-failure.enable", true);
			conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

			conf.set("cutoff", otherArgs[3]);
			conf.set("type", otherArgs[4]);
			conf.set("idname", otherArgs[5]);
	        
			Job job = Job.getInstance(conf, "tv person movie:GetUserRecListB");
	        job.setNumReduceTasks(300);
			Path cachePath = new Path(otherArgs[2]);
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] arr = fs.listStatus(cachePath);
			for (FileStatus fstatus : arr) {
				Path p = fstatus.getPath();
				if (fs.isFile(p)) {
					job.addCacheFile(p.toUri());
				}
			}

			job.setJarByClass(GetUserRecListBtest.class);

			MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
					MapClass.class);

			job.setReducerClass(ReducerClass.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
}
