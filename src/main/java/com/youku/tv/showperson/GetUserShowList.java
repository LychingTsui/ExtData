package com.youku.tv.showperson;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
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

public class GetUserShowList {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		private Text outkey = new Text(), outval = new Text();
		private HashMap<String, Integer> totalepside = new HashMap<String,Integer>();//保存总集数
		private HashMap<String, Integer> hasepside = new HashMap<String,Integer>();//保存更新集数
		Date now = new Date();
		SimpleDateFormat dateformat = new SimpleDateFormat("yyyyMMdd");
		private DecimalFormat df = new DecimalFormat("0.000");
		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String[] info = value.toString().split("\t");
			if (info[1].length() > 3) {
				String[] data = info[1].substring(3).split(",");
				StringBuffer sBuffer = new StringBuffer();
				for (int i = 0; i < data.length; i++) {
					String[] temp = data[i].split(":");
					if (totalepside.containsKey(temp[0])) {
						//获取用户观看该动漫或者电视剧的集数
						String[] epi = temp[1].split( "\2");
						double score = Double.valueOf(epi.length) / hasepside.get(temp[0]);

						long datelong = 1;
						try {//获取观看的纪录与当前时间的之间的时差
							datelong = CommonCalculate.DateSub(temp[4], dateformat.format(now));
						} catch (ParseException e) {
							e.printStackTrace();
						}
						if (datelong < 1) {
							datelong = 1;
						}
						//时间间隔越久权重越低
						score = score / Math.pow(datelong, 1 / 2.5);
						//观看的最大集数
						double maxnum = Double.valueOf(epi[epi.length - 1]);
						//电视剧综艺库中更新的数据信息，即总集数，已经更新的集数
						int maxepi = totalepside.get(temp[0]);
						int maxhasepi = hasepside.get(temp[0]);

						// 已更新结束
						if (maxepi == maxhasepi && (maxnum / maxepi > 0.9)) {
							score *= 0.01;
						}
						// 更新中
						if (maxepi == 0 && maxnum >= maxhasepi) {
							score *= 1.2;
						}

						sBuffer.append(",").append(temp[0]).append(":").append(df.format(score));
					}
				}
				if (sBuffer.length() > 1) {
					outkey.set(info[0]);
					String []arr=Utils.sortRecList(sBuffer.substring(1), 0,1,",",":").split(",");
					StringBuffer buffer=new StringBuffer();
					for (String string : arr) {
						buffer.append(string+":A:301");
						buffer.append(",");
					}
					//outval.set(Utils.sortRecList(sBuffer.substring(1), 0, 1, ",", ":"));
					outval.set(buffer.deleteCharAt(buffer.length()-1).toString());
					context.write(outkey, outval);
				}
			}

		}

		@Override
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
			String line = null;//尝试减少错误发生
			while ((line = br.readLine()) != null) {
				String info[]=line.split("\t");
				if (info.length<6) {
					if (info.length==5&&!info[4].equals("")) {
						int totalnum=Integer.valueOf(info[4]);
						int num=Integer.valueOf(info[4]);
						totalepside.put(info[0], totalnum);
						hasepside.put(info[0], num);
					}
				}
				else {
					if (info[2].equals("zongyi")) {
						int totalnum=Integer.valueOf("0");
						int num=Integer.valueOf(info[4]);
						totalepside.put(info[0], totalnum);
						hasepside.put(info[0], num);
					}
					if (info[2].equals("tv")) {
						if (info[3].equals("0")&&!info[5].equals("")&&!info[4].equals("")) {
							int totalnum=Integer.valueOf(info[4]);
							int num=Integer.valueOf(info[4]);
							totalepside.put(info[0], totalnum);
							hasepside.put(info[0], num);
						}
						else if((info[3].equals("1")&&!info[4].equals(""))){
							int totalnum=Integer.valueOf(info[4]);
							int num=Integer.valueOf(info[4]);
							totalepside.put(info[0], totalnum);
							hasepside.put(info[0], num);
						}
					}
					if (info[2].equals("comic")) {
						if ((info[3].equals("0")&&!info[5].equals("")&&!info[4].equals(""))) {
							int totalnum=Integer.valueOf(info[4]);
							int num=Integer.valueOf(info[4]);
							totalepside.put(info[0], totalnum);
							hasepside.put(info[0], num);
						}
						else if((info[3].equals("1")&&!info[4].equals(""))){
							int totalnum=Integer.valueOf(info[4]);
							int num=Integer.valueOf(info[4]);
							totalepside.put(info[0], totalnum);
							hasepside.put(info[0], num);
						}	
					}	
				}
			}
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "tv show:GetUserShowList");

		Path cachePath = new Path(otherArgs[2]);
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] arr = fs.listStatus(cachePath);
		for (FileStatus fstatus : arr) {
			Path p = fstatus.getPath();
			if (fs.isFile(p)) {
				job.addCacheFile(p.toUri());
			}
		}

		job.setJarByClass(GetUserShowList.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
				MapClass.class);

		job.setNumReduceTasks(1);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
