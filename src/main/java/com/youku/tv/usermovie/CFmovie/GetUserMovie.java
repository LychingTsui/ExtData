package com.youku.tv.usermovie.CFmovie;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.ArrayList;
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

import com.youku.tv.movieperson.reclist20160420.Utils;

public class GetUserMovie {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		static Text outkey = new Text(), outvalue = new Text();
		static HashMap<String, String> map = new HashMap<String,String>();
		static DecimalFormat df = new DecimalFormat("0.0000");
		static int cutoff = 0; static String mark="0";

		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			try {
				//对用户的行为数据进行分析，去掉观看纪录
				String[] info = value.toString().split("\t");
				String vv = "", click = "", show = "";
				if (info[1].length() > 3) {
					vv = info[1].substring(3);
				}
				if (info[2].length() > 6) {
					click = info[2].substring(6);
				}
				if (info[3].length() > 5) {
					show = info[3].substring(5);
				}
				//保存推荐的电影和电影的权重大小
				HashMap<String, Double> vidmap = new HashMap<String,Double>();

				if (vv.length() > 0) {
					//保存用户已经点击和已经播放的视频
					ArrayList<String> list = new ArrayList<String>();
					if (click.length() > 0) {
						String[] data = click.split(",");
						for (int i = 0; i < data.length; i++) {
							String id = data[i].split(":")[0];
							if (!list.contains(id)) {
								list.add(id);
							}
						}
					}

					String[] data = vv.split(",");
					for (int i = 0; i < data.length; i++) {
						String[] temp = data[i].split(":");
						if (!list.contains(temp[0])) {
							list.add(temp[0]);
						}
					}
					ArrayList<String> midList = new ArrayList<String>();
					for (int i = 0; i < data.length; i++) {
						String[] temp = data[i].split(":");						
						// double playrate = Math.min(Double.valueOf(temp[2]) /
						// Double.valueOf(temp[1]),1.0);
						if (midList.contains(temp[0])) {
							continue;
						}else {
							midList.add(temp[0]); 
						}
						//初始值设置
						double playrate = 1.0;
						double daterate = 1;
						try {
							//利用时间天数衡量
							daterate = Math.log10(40 + Utils.DateSub(temp[3]));
						} catch (ParseException e) {
							e.printStackTrace();
						}
						double rate = playrate / daterate;
						//利用时间间隔作为权重的衡量方式
						if (map.containsKey(temp[0])) {
							String[] vid = map.get(temp[0]).split(",");
							for (int j = 0; j < vid.length; j++) {
								String[] tmp = vid[j].split(":");
								if (!list.contains(tmp[0])) {
									//tmp[0]表示是ID，tmp[1]表示的是相关推荐计算出的权重
									double d = Double.valueOf(tmp[1]) * rate;
									//如果多个电影推荐出该电影，那么该电影的权重应该加大
									if (vidmap.containsKey(tmp[0])) {
										d += vidmap.get(tmp[0]);
									}
									vidmap.put(tmp[0], d);
								}
							}
						}
					}
					//对数据样式进行整理输出
					if (vidmap.size() > 0) {
						Object[] objects = vidmap.keySet().toArray();
						StringBuffer sBuffer = new StringBuffer();
						for (int i = 0; i < objects.length; i++) {
							sBuffer.append(",");
							sBuffer.append(objects[i].toString()).append(":");
							sBuffer.append(df.format(vidmap.get(objects[i].toString()))+":"+mark);
						}
						outkey.set(info[0]);
						//重新排序
						outvalue.set(Utils.sortRecList(sBuffer.substring(1), cutoff, 1, ",", ":"));
						context.write(outkey, outvalue);
					}
				}
			} catch (Exception e) {
				context.getCounter("wrong123", "inputnum").increment(1);
				return;
			}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			cutoff = Integer.valueOf(context.getConfiguration().get("cutoff"));
			mark=String.valueOf(context.getConfiguration().get("mark"));
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
			String subreclist=null;
			while ((line = br.readLine()) != null) {
				String[] info = line.split("\t");
				subreclist=Utils.sortRecList(info[1], 8, 1, ",", ":");
				map.put(info[0], subreclist);
			}
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("mark", otherArgs[3]);
		conf.set("cutoff", otherArgs[4]);
		Job job = Job.getInstance(conf, "recommend:GetUserMovie");

		Path cachePath = new Path(otherArgs[2]);
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] arr = fs.listStatus(cachePath);
		for (FileStatus fstatus : arr) {
			Path p = fstatus.getPath();
			if (fs.isFile(p)) {
				job.addCacheFile(p.toUri());
			}
		}

		job.setJarByClass(GetUserMovie.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
				MapClass.class);

		job.setNumReduceTasks(10);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
