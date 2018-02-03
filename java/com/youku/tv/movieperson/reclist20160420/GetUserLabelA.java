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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class GetUserLabelA {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		static int actorcutoff = 3, dirtorcutoff = 2, typecutoff = 0;
		static HashMap<String, Double> timemap = new HashMap<String,Double>();
		static HashMap<String, String> actorMap = new HashMap<String,String>(), dirtorMap = new HashMap<String,String>(),
				typeMap = new HashMap<String,String>();
		static Text outkey = new Text(), outvalue = new Text();
		static DecimalFormat df = new DecimalFormat("0.000");

		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String[] info = value.toString().split("\t");
			if (info.length < 4) {
				return;
			}
			String vv = info[1].substring(3);
			HashMap<String, Double> person = new HashMap<String,Double>(), tag = new HashMap<String,Double>();
			if (vv.length() > 0) {
				String[] data = vv.split(",");
				for (int i = 0; i < data.length; i++) {
					String[] temp = data[i].split(":");
					//temp[1]表示电影时长temp［2］表示播放位置
					double playrate = Math.min(Double.valueOf(temp[2]) / Double.valueOf(temp[1]),
							1.0);
					double daterate = 0;
					try {
						//得到与今天之间的相隔日期
						daterate = Math.log10(10 + Utils.DateSub(temp[3]));
					} catch (ParseException e) {
						e.printStackTrace();
					}
					//时间越久，权重越小
					double rate = playrate / daterate;
					//此处开始利用豆瓣的信息与电视家用户行为结合，进行计算
					//计算演员的权重
					if (actorMap.containsKey(temp[0])) {
						String[] vid = actorMap.get(temp[0]).split(",");
						
						for (int j = 0; j < vid.length; j++) {
							double d = rate * (1 - 0.2 * j);
							if (d > 0) {
								//演员权重叠加
								if (person.containsKey(vid[j])) {
									d += person.get(vid[j]);
								}
								//演员与对应的权重汇总
								person.put(vid[j], d);
							}
						}
					}
					//此处开始利用豆瓣的信息与电视家用户行为结合，进行计算
					//计算director的权重
					if (dirtorMap.containsKey(temp[0])) {
						String[] vid = dirtorMap.get(temp[0]).split(",");
						for (int j = 0; j < vid.length; j++) {
							double d = rate;
							//导演权重叠加
							if (person.containsKey(vid[j])) {
								d += person.get(vid[j]);
							}
							//导演权重汇总
							person.put(vid[j], d);
						}
					}
					//此处开始利用豆瓣的信息与电视家用户行为结合，进行计算
					//计算type的权重 例如 惊悚 爱情 武打
					if (typeMap.containsKey(temp[0])) {
						String[] vid = typeMap.get(temp[0]).split(",");
						for (int j = 0; j < vid.length; j++) {
							double d = rate * (1 - 0.1 * j);
							if (d > 0) {
								//标签权重均值 例如爱情美好 1.0
								if (timemap.containsKey(vid[j])) {
									d = d / timemap.get(vid[j]);
								}
								//标签权重叠加
								if (tag.containsKey(vid[j])) {
									d += tag.get(vid[j]);
								}
								tag.put(vid[j], d);
							}
						}
					}
				}
				if (person.size() > 0 || tag.size() > 0) {
					outkey.set(info[0]);
					Object[] objects = person.keySet().toArray();
					StringBuffer sBuffer = new StringBuffer();
					for (int i = 0; i < objects.length; i++) {
						sBuffer.append(",").append(objects[i].toString()).append(":")
								.append(df.format(person.get(objects[i].toString())));
					}
					String perlist = "";
					if (sBuffer.length() > 1) {
						//演员和导演权值排序
						perlist = Utils.sortRecList(sBuffer.substring(1), 0, 1, ",", ":");
					}

					objects = tag.keySet().toArray();
					StringBuffer sBufferA = new StringBuffer();
					for (int i = 0; i < objects.length; i++) {
						sBufferA.append(",").append(objects[i].toString()).append(":")
								.append(df.format(tag.get(objects[i].toString())));
					}
					//标签权值排序
					String taglist = "";
					if (sBufferA.length() > 1) {
						taglist = Utils.sortRecList(sBufferA.substring(1), 0, 1, ",", ":");
					}

					outvalue.set(perlist + "\t" + taglist);
					context.write(outkey, outvalue);
				}
			}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			actorcutoff = Integer.valueOf(context.getConfiguration().get("actorcutoff"));
			typecutoff = Integer.valueOf(context.getConfiguration().get("typecutoff"));
			dirtorcutoff = Integer.valueOf(context.getConfiguration().get("dirtorcutoff"));
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
				String[] info = line.split("\t");

				if (info.length > 2) {
					MovieDataMeta meta = new MovieDataMeta(line);

					String id = meta.Getid();
					//取actor director和type作为用户画像的特征
					String[] actor = meta.Getactor().split(",");
					String[] dirtor = meta.Getdiretor().split(",");
					String[] type = meta.Gettags().split(",");

					StringBuffer sBuffer = new StringBuffer();
					int len = actor.length;
					if (actorcutoff > 0 && actorcutoff < len) {
						len = actorcutoff;
					}
					for (int i = 0; i < len; i++) {
						if (actor[i].length() > 0) {
							//添加actor
							sBuffer.append(",").append(actor[i]);
						}
					}
					if (sBuffer.length() > 1) {
						//按照电影的ID接收actor
						actorMap.put(id, sBuffer.substring(1));
					}

					sBuffer.setLength(0);
					len = dirtor.length;
					if (dirtorcutoff > 0 && dirtorcutoff < len) {
						len = dirtorcutoff;
					}
					for (int i = 0; i < len; i++) {
						if (dirtor[i].length() > 0) {
							//添加导演
							sBuffer.append(",").append(dirtor[i]);
						}
					}
					if (sBuffer.length() > 1) {
						//按照电影的ID接收director
						dirtorMap.put(id, sBuffer.substring(1));
					}

					sBuffer.setLength(0);
					len = type.length;
					if (typecutoff > 0 && typecutoff < len) {
						len = typecutoff;
					}
					for (int i = 0; i < len; i++) {
						if (type[i].length() > 0) {
							sBuffer.append(",").append(type[i].replaceAll(":", ""));

						}
					}
					//按照电影的ID接收type
					if (sBuffer.length() > 1) {
						typeMap.put(id, sBuffer.substring(1));
					}

				} else {
					double d = Double.valueOf(info[1]);
					if (d > 2 && !isInteger(info[0])) {
						//降低权重
						d = Math.pow(d, 1 / 1.5) / 10;
						if (d < 1.5) {
							d = 1.5;
						}
						//豆瓣用户所标注的标签集合
						timemap.put(info[0].replaceAll(":", ""), d);
					}
				}
			}
		}
	}

	public static boolean isInteger(String value) {
		try {
			Double.parseDouble(value);
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException {
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String[] data = otherArgs[3].split(":");
		conf.set("actorcutoff", data[0]);
		conf.set("dirtorcutoff", data[1]);
		conf.set("typecutoff", data[2]);

		Job job = Job.getInstance(conf, "tv person movie:GetUserLabelA");

		Path cachePath = new Path(otherArgs[2]);
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] arr = fs.listStatus(cachePath);
		for (FileStatus fstatus : arr) {
			Path p = fstatus.getPath();
			if (fs.isFile(p)) {
				job.addCacheFile(p.toUri());
			}
		}

		job.setJarByClass(GetUserLabelA.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
				MapClass.class);

		job.setNumReduceTasks(1);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
