package com.youku.tv.movie.reclist20151228;
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
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import com.youku.tv.movie.reclist20151228.MovieDataMeta;
import com.youku.tv.movieperson.reclist20160420.Utils;
public class GetRecListB {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		static HashMap<String, String> map = new HashMap<String,String>();//存储演员 和电影名称的map集合
		static HashMap<String, Double> ratemap = new HashMap<String,Double>();//存储电影名称和分数的map集合
		static HashMap<String, Double> tagmap = new HashMap<String,Double>();//存储电影name\2tag 权重
		static HashMap<String, Double> datemap= new HashMap<String, Double>();
		static Text outkey = new Text();
		static Text outvalue = new Text();
		static int cutoff = 10;
		static double type = 3.0, rate = 0.7, ban = 0.85;

		static DecimalFormat df = new DecimalFormat("0.000");

		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			Counter counter=context.getCounter("日期间隔", "count");
			String[] info = value.toString().split("\t");
			HashMap<String, Double> mapdata = new HashMap<String,Double>();
			String[] data;
			if (info[1].length() > 1) {
				//对电影的分数进行分水岭判断
				if (ratemap.containsKey(info[0])) {
					if (ratemap.get(info[0]) >= ban) {
						type = 1.5;
					} else {
						type = 0.1;
					}
				} else {
					type = 1.0;
				}
				if (ratemap.containsKey(info[0]) && ratemap.get(info[0]) <= 0.85) {
					data = info[1].split(",");//获取对该电影的画像
					data=RemoveRepeat(data);
					for (int i = 0; i < data.length; i++) {
						String[] temp = data[i].split(":");
						if (map.containsKey(temp[0])) {
							String[] tmp = map.get(temp[0]).split("\2");//获取该演员或者导演画像下的所有的电影
							for (int j = 0; j < tmp.length; j++) {
								double d = Double.valueOf(temp[1]);
								//对权重进行优化。
								if (ratemap.containsKey(tmp[j])) {
									d = d * Math.pow(ratemap.get(tmp[j]), type);
								} else {
									d = d * Math.pow(0.65, type);
								}

								if (mapdata.containsKey(tmp[j])) {
									d += mapdata.get(tmp[j]);
								}
								mapdata.put(tmp[j], d);
							}
						}
					}
				}
			}

			if (ratemap.containsKey(info[0])) {
				if (ratemap.get(info[0]) >= ban) {
					type = 1.5;
				} else {
					type = 0.3;
				}
			} else {
				type = 1.0;
			}//计算标签的权重
			HashMap<String, Double> mapda = new HashMap<String,Double>();
			if (info.length > 2 && info[2].length() > 1) {
				data = info[2].split(",");
				data=RemoveRepeat(data);
				for (int i = 0; i < data.length; i++) {
					String[] temp = data[i].split(":");
					if (map.containsKey(temp[0])) {
						String[] tmp = map.get(temp[0]).split("\2");//获取该标签下的所有电影

						for (int j = 0; j < tmp.length; j++) {
							double d = Double.valueOf(temp[1]);
							//对有电影与标签的权重进行规格化
							if (tagmap.containsKey(tmp[j] + "\2" + temp[0])) {
								d = d * tagmap.get(tmp[j] + "\2" + temp[0]);//tmp[j] + "\2" + temp[0] 功夫\38747\2搞笑
							}
							//对有电影与标签的权重进行规格化
							if (ratemap.containsKey(tmp[j])) {
								d = d * Math.pow(ratemap.get(tmp[j]), type);
							} else {
								d = d * Math.pow(0.65, type);
							}
							//降低周星驰权重，同时
							if (mapda.containsKey(tmp[j])) {
								if (mapda.containsKey("周星驰")) {
									d = (d + mapda.get(tmp[j]) * rate)/100;
								}
								else{
								d = d + mapda.get(tmp[j]) * rate;
								  }
								}
							mapda.put(tmp[j], d);
						}
					}
				}
			}
			//将person和tags推荐的电影进行合并。
			if (mapda.size() > 0) {
				Object[] obj = mapda.keySet().toArray();
				for (int i = 0; i < obj.length; i++) {
					double d = mapda.get(obj[i].toString());
					if (mapdata.containsKey(obj[i].toString())) {
						d = mapda.get(obj[i].toString()) + mapdata.get(obj[i].toString());
					}
					mapdata.put(obj[i].toString(), d);
				}
			}

			Object[] obj = mapdata.keySet().toArray();
			StringBuffer sBuffer = new StringBuffer();
			double ratedate=2;
			for (int i = 0; i < obj.length; i++) {
				//如果相关推荐中推荐出本身自己，那么将排除在外。
				if (obj[i].toString().equals(info[0])) {
					continue;
				}
				sBuffer.append(",");
				sBuffer.append(obj[i].toString()).append(":");
				if (datemap.containsKey(obj[i].toString())) {
					double d=mapdata.get(obj[i].toString())/datemap.get(obj[i].toString());
					sBuffer.append(df.format(d));
					counter.increment(1);
				}
				else {
					sBuffer.append(df.format(mapdata.get(obj[i].toString())/ratedate));
				}
				

			}

			if (sBuffer.length() > 1) {
				outkey.set(info[0]);
				outvalue.set(Utils.sortRecList(sBuffer.substring(1), cutoff, 1, ",", ":"));
				context.write(outkey, outvalue);
			}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			cutoff = Integer.valueOf(context.getConfiguration().get("cutoff"));
			type = Double.valueOf(context.getConfiguration().get("type"));
			rate = Double.valueOf(context.getConfiguration().get("rate"));
			ban = Double.valueOf(context.getConfiguration().get("ban"));

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
				//map集合汇总，key是导演或者演员或者标签，value是电影的标题
				MovieDataMeta meta = new MovieDataMeta(line);
				String[] data = meta.Getactor().split(",");
				int len = data.length;
				
				if (len >= 2)
					len = 2;
				for (int i = 0; i < len; i++) {
					if (!data[i].equals("null")) {
						String name = meta.Gettitle();
						int args=meta.Getidindex();
						if (map.containsKey(data[i])) {
							if (!IsContain(map.get(data[i]), name+"\3"+args)) {
								name = map.get(data[i]) + "\2" + name+"\3"+args;
							} else {
								name = map.get(data[i]);
							}
						}
						map.put(data[i], name);
					}
				}
				//获取所有的导演
				data = meta.Getdiretor().split(",");
				len = data.length;
				if (len >= 1)
					len = 1;
				for (int i = 0; i < len; i++) {
					if (!data[i].equals("null")) {
						int args=meta.Getidindex();
						String name = meta.Gettitle();
						if (map.containsKey(data[i])) {
							if (!IsContain(map.get(data[i]), name+"\3"+args)) {
								name = map.get(data[i]) + "\2" + name+"\3"+args;
							} else {
								name = map.get(data[i]);
							}
						}
						map.put(data[i], name);
					}

				}

				data = meta.Gettags().split(",");
				len = data.length;
				//标签和电影的名称进行
				for (int i = 0; i < len; i++) {
					if (!data[i].equals("null") && data[i].length() > 0) {
						String name = meta.Gettitle();
						int args=meta.Getidindex();
						String tag = data[i].replaceAll(":", "").replaceAll("：", "");
						double sim = 1 - i * 0.1;
						if (sim > 0) {
							if (map.containsKey(tag)) {
								if (!IsContain(map.get(tag), name+"\3"+args)) {
									name = map.get(tag) + "\2" + name+"\3"+args;
									tagmap.put(name + "\2" + tag, sim);//tagmap保存的是电影＋标签的权重 key 功夫\30393\2幽默 value 0.8
								} else {
									name = map.get(tag);
								}

							}
							map.put(tag, name);
						}
					}
				}
				if (meta.Getdate().length() > 1) {
					double daterate = 2;
					int args=meta.Getidindex();
					String name=meta.Gettitle();
					try {//判断电影年限
						daterate = Math.log(3 + Utils.DateSubYear(meta.Getdate()));
					} catch (ParseException e) {
						e.printStackTrace();
					}
					//储存Idindex 时间处理之后的值
					datemap.put(name+"\3"+args, daterate);
				}
				//获取ratemap key是电影名称，value是电影分数
				try {
					double rate = Double.valueOf(meta.Getrating()) / 100;
					if (ratemap.containsKey(meta.Gettitle())) {
						if (rate > ratemap.get(meta.Gettitle()+"\3"+meta.Getidindex())) {
							rate = ratemap.get(meta.Gettitle()+"\3"+meta.Getidindex());
						}
					}
					ratemap.put(meta.Gettitle()+"\3"+meta.Getidindex(), rate);
				} catch (Exception e) {
					continue;
				}
			}
		}
		//判断字符串中是否包含某个字符串
		private static boolean IsContain(String data, String name) {
			String[] info = data.split("\2");
			for (int i = 0; i < info.length; i++) {
				if (info[i].equals(name)) {
					return true;
				}
			}
			return false;
		}
		//当标签中包含香港电影和香港的时候，利用该方法去掉香港保留香港电影。防止马太效应
				private String[] RemoveRepeat(String[] tags) {
					if (tags.length < 2) {
						return tags;
					} else {

						ArrayList<String> list = new ArrayList<String>();
						for (int i = 0; i < tags.length; i++) {
							list.add(tags[i]);
						}
		            
						int num = 0;
						for (int i = 0; i < list.size(); i++) {
							String tmp = list.get(i) + "电影";
							if (list.contains(tmp)) {
								list.set(i, "");
								num++;
							}
						}
						if (num > 0) {
							int index = 0;
							String[] newtags = new String[tags.length - num];
							for (int i = 0; i < list.size(); i++) {
								if (list.get(i).length() > 0) {
									newtags[index] = list.get(i);
									index++;
								}
							}
							return newtags;
						} else {
							return tags;
						}
					}
				}
	}

	public static void main(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException {
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		conf.set("mapreduce.map.java.opts", "-Xmx4096m");
		//conf.set("mapred.child.java.opts", "-Xmx2048m");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		conf.set("cutoff", otherArgs[3]);
		conf.set("type", otherArgs[4]);
		conf.set("rate", otherArgs[5]);
		conf.set("ban", otherArgs[6]);

		Job job = Job.getInstance(conf, "tv movie:GetRecListB");

		Path cachePath = new Path(otherArgs[2]);
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] arr = fs.listStatus(cachePath);
		for (FileStatus fstatus : arr) {
			Path p = fstatus.getPath();
			if (fs.isFile(p)) {
				job.addCacheFile(p.toUri());
			}
		}

		job.setJarByClass(GetRecListB.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
				MapClass.class);

		job.setNumReduceTasks(1);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
