package com.youku.tv.movieperson.reclist20160420;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

public class GetUserLabelB {
	//此处用到normalizB和userlabelA的数据，userlabelA相当于对电视家用户的一个画像，normalizeB相当于是一个
	//电影标签库
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		static HashMap<String, String> map = new HashMap<String,String>();//保存相似度标签
		static HashMap<String, String> actorMap = new HashMap<String,String>();
		static HashMap<String, String> movieMap = new HashMap<String,String>();
		static HashMap<String, ArrayList<Integer>> typeMap = new HashMap<String,ArrayList<Integer>>();
		static HashMap<String, ArrayList<Integer>>tf=new HashMap<String, ArrayList<Integer>>();//tf-idf算法
		static Text outkey = new Text(), outvalue = new Text();
		//权重分割线
		static double cutoff = 0.17;
		//数据格式化
		static DecimalFormat df = new DecimalFormat("0.0000");
		//电影类型和演员的截取标志位
		static int typecutoff = 5, actorcutoff = 2;
		public static boolean getNumbers(String content) {  
			content=content.trim();
			String str2="";
			if(content != null&&content.length()==4 && !"".equals(content)){
				String reg="^\\d+$";
				return content.matches(reg);
			} 
			return false;
		}
		protected void map(LongWritable key, Text value, Context context) throws IOException,
		InterruptedException {
			String[] info = value.toString().split("\t");
			if (info.length<3) {
				return;
			}
			//获取导演和演员
			if (info[1].length() > 6) {
				
			}
			String[] person = info[1].substring(6).split(",");
			//获取类型标签
			String[] type = null;
			if (info.length > 2) {
				//去重，例如一个用户的标签中包含香港、香港电影，那么去掉香港。
				type = RemoveRepeat(info[2].substring(5).split(","),"电影");
				type =RemoveRepeat(type, "动画");
			}
			//储存person和type的map集合
			HashMap<String, Double> permapdata = new HashMap<String,Double>();
			HashMap<String, Double> typemapdata = new HashMap<String,Double>();
			for (int i = 0; i < person.length; i++) {
				//对演员和导演的权重进行计算
				String[] temp = person[i].split(":");
				//开始对用户与电影标签进行深度整合，对一个用户进行画像，temp是演员和导演的数组
				if (map.containsKey(temp[0])) {
					String[] data = map.get(temp[0]).split(",");//标签库，基于该导演和演员以及type的一系列的相似性的训练数据 爱情   情爱：0.8，温馨：0.4，可爱：0.2
					int len = Math.min(data.length, 2);
					for (int j = 0; j < len; j++) {
						String[] tmp = data[j].split(":");
						//cutoff=0.17
						if (Double.valueOf(tmp[1]) >= cutoff) {
							//对该标签的权重重新整理
							//if (permapdata.containsKey(tmp[0])) {
							double d = Double.valueOf(tmp[1]) * Double.valueOf(temp[1]);
								if (tf.containsKey(tmp[0])) {
									ArrayList<Integer> list=tf.get(tmp[0]);
									double b=list.size();
									double c=movieMap.size();
									d=d*(Math.pow(Math.log10(c/b), 1.5)/10);
									if (permapdata.containsKey(tmp[0])) {
										d += permapdata.get(tmp[0]);
										permapdata.put(tmp[0], d);
									
//									if (permapdata.get(tmp[0])<d) {
//										permapdata.put(tmp[0], d);
//									}
								  }
									else{
										permapdata.put(tmp[0], d);
									}
									}
								else{
								double b=1;
								double c=movieMap.size();
								d=d*(Math.pow(Math.log10(c/b), 1.5)/10);
								permapdata.put(tmp[0], d);
								}
								//d += permapdata.get(tmp[0]);
							//}
							//permapdata.put(tmp[0], d);
						}
					}
				}
			}
			//对标签格式化
			Object[] obj = permapdata.keySet().toArray();
			StringBuffer sBuffer = new StringBuffer();
			for (int i = 0; i < obj.length; i++) {
				sBuffer.append(",");
				sBuffer.append("\2" + obj[i].toString()).append(":");
				sBuffer.append(df.format(permapdata.get(obj[i].toString())));
			}

			StringBuffer sBufferA = new StringBuffer();
			if (type != null) {
				for (int i = 0; i < type.length; i++) {//与上述person类似，此处是对type进行权重计算
					String[] temp = type[i].split(":");//
					if (map.containsKey("\2" + temp[0])) {
						String[] data = map.get("\2" + temp[0]).split(",");
						//剔除年代标签
						if (getNumbers(temp[0])) {
							continue;
						}//原始是8
						int len = Math.min(data.length, 8);
						for (int j = 0; j < len; j++) {
							String[] tmp = data[j].split(":");
							double d = Double.valueOf(tmp[1]) * Double.valueOf(temp[1]);
							//if (typemapdata.containsKey(tmp[0])) {
								if (typeMap.containsKey(tmp[0])) {
									ArrayList<Integer> list=typeMap.get(tmp[0]);
									double b=list.size();
									double c=movieMap.size();
									d=d*(Math.pow(Math.log10(c/b), 1.5)/10);
									if (typemapdata.containsKey(tmp[0])) {
									d+=d+typemapdata.get(tmp[0]);
									typemapdata.put(tmp[0], d);
									//double a = typemapdata.get(tmp[0]);
//									if (a<d) {
//										typemapdata.put(tmp[0], d);
//									}
								}
									else {
										typemapdata.put(tmp[0], d);
									}
									}
							else{
								double b=1;
								double c=movieMap.size();
								d=d*(Math.pow(Math.log10(c/b), 1.5)/10);
									typemapdata.put(tmp[0], d);
									}
								
							
							//}
							//else{
							//typemapdata.put(tmp[0], d);
							//}
						}
					}
				}
				obj = typemapdata.keySet().toArray();
				//对type进行格式化
				for (int i = 0; i < obj.length; i++) {
					double dd = 1.0;
					if (obj[i].toString().equals("美国")) {
						dd = 0.5;
					}
					sBufferA.append(",");
					sBufferA.append(obj[i].toString()).append(":")
					.append(df.format(typemapdata.get(obj[i].toString()) * dd));
				}
			}
			String str1 = "";
			String str2 = "";
			String str = "";
			if (sBuffer.length() > 1) {
				str = sBuffer.substring(1);
			}
			if (sBufferA.length() > 1) {
				if (str.length() > 1) {
					str = str + sBufferA.toString();
				} else {
					str = sBufferA.substring(1);
				}
			}
			if (str.length()<1) {
				return;
			}
			str = Utils.sortRecList(str, typecutoff + actorcutoff, 1, ",", ":");
			String[] data = str.split(",");
			for (int i = 0; i < data.length-1; i++) {
				if (data[i].startsWith("\2")) {
					for (int j = i+1; j < data.length; j++) {
						if (data[j].split(":")[0].equals(data[i].substring(1).split(":")[0])) {
							data[j]="1";
						}
						
					}
					str1 = str1 + "," + data[i].substring(1);
					
				} else {
					for (int j = i+1; j < data.length; j++) {
						if (data[j].substring(1).split(":")[0].equals(data[i].split(":")[0])) {
							data[i]="1";
						}
					}
					if (!data[i].equals("1")) {
						str2 = str2 + "," + data[i];
					}
					
				}
			}
			if (str1.length() > 1) {
				str1 = str1.substring(1);
			}
			if (str2.length() > 1) {
				str2 = str2.substring(1);
				StringBuffer buffer=new StringBuffer();
				String []sort=RemoveRepeat(RemoveRepeat(RemoveRepeat(str2.split(","), "日本"),"电影"), "动画");
				//sort=Removecomic(sort, "动画");
				for (String string : sort) {
					buffer.append(string+",");
				}
				str2=buffer.deleteCharAt(buffer.length()-1).toString();
			}
			outkey.set(info[0]);
			outvalue.set("person"+str1 + "\t" + "label" + str2);
			context.write(outkey, outvalue);

		}
		//去重,当对用户画像的时候即出现了美国，又出现了美国电影，我们默认保存美国电影，去掉美国
		private String[] RemoveRepeat(String[] tags,String st) {
			if (tags.length < 2) {
				return tags;
			} else {

				ArrayList<String> list = new ArrayList<String>();
				for (int i = 0; i < tags.length; i++) {
					list.add(tags[i].split(":")[0]);
				}

				int num = 0;
				for (int i = 0; i < list.size(); i++) {
					String tmp = list.get(i) + st;
					String temp=st+list.get(i);
					if (list.contains(tmp)||list.contains(temp)) {
						list.set(i, "");
						tags[i] = "";
						num++;
					}
				}
				if (num > 0) {
					int index = 0;
					String[] newtags = new String[tags.length - num];
					for (int i = 0; i < list.size(); i++) {
						if (tags[i].length() > 0) {
							newtags[index] = tags[i];
							index++;
						}
					}

					return newtags;
				} else {
					return tags;
				}
			}
		}
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			cutoff = Double.valueOf(context.getConfiguration().get("cutoff"));
			typecutoff = Integer.valueOf(context.getConfiguration().get("typecutoff"));
			actorcutoff = Integer.valueOf(context.getConfiguration().get("actorcutoff"));
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
				if (info.length==2) {
					map.put(info[0], info[1]);
				}
				else {
					MovieDataMeta meta = new MovieDataMeta(line);

					String id = meta.Getid();
					//取actor director和type作为用户画像的特征
					String[] actor = meta.Getactor().split(",");
					String[] dirtor = meta.Getdiretor().split(",");
					String[] type = meta.Gettags().split(",");
					movieMap.put(id, id);
					StringBuffer sBuffer = new StringBuffer();
					int len = actor.length;
					if (actorcutoff > 0 && actorcutoff < len) {
						len = actorcutoff;
					}
					for (int i = 0; i < len; i++) {
						if (actor[i].length() > 0) {
							if (tf.containsKey(actor[i])) {
								ArrayList<Integer>list=tf.get(actor[i]);
								if (!list.contains(meta.Getidindex())) {
									list.add(meta.Getidindex());
								}
								tf.put(actor[i], list);
							}
							else {
								ArrayList<Integer>list=new ArrayList<Integer>();
								list.add(meta.Getidindex());
								tf.put(actor[i], list);
							}
							//添加actor
							sBuffer.append(",").append(actor[i]);
						}
					}
					if (sBuffer.length() > 1) {
						//按照电影的ID接收actor
						actorMap.put(id, sBuffer.substring(1));
					}
					sBuffer.setLength(0);
					int length=type.length;
					if (typecutoff>0&& typecutoff<length) {
						length=typecutoff;
						for (int i = 0; i < type.length; i++) {
							if (type[i].length()>0) {
								if (typeMap.containsKey(type[i])) {
									ArrayList<Integer>list=typeMap.get(type[i]);
									if (!list.contains(meta.Getidindex())) {
										list.add(meta.Getidindex());
									}
									typeMap.put(type[i], list);
								}
								else {
									ArrayList<Integer>list=new ArrayList<Integer>();
									list.add(meta.Getidindex());
									typeMap.put(type[i], list);
								}
							}
						}
					}
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
		conf.set("mapreduce.job.reduces", "2000");
		conf.set("mapreduce.task.timeout", "1800000");
		conf.set("mapreduce.reduce.self.java.opts", "-Xmx7120m");
		conf.set("mapreduce.child.java.opts", "-Xmx5024m");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		conf.set("typecutoff", otherArgs[3]);
		conf.set("actorcutoff", otherArgs[4]);
		conf.set("cutoff", otherArgs[5]);

		Job job = Job.getInstance(conf, "tv person movie:GetUserLabelB");

		Path cachePath = new Path(otherArgs[2]);
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] arr = fs.listStatus(cachePath);
		for (FileStatus fstatus : arr) {
			Path p = fstatus.getPath();
			if (fs.isFile(p)) {
				job.addCacheFile(p.toUri());
			}
		}

		job.setJarByClass(GetUserLabelB.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
				MapClass.class);

		job.setNumReduceTasks(20);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
