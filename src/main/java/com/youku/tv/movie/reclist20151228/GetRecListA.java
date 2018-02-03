package com.youku.tv.movie.reclist20151228;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
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
import com.youku.tv.movie.reclist20151228.MovieDataMeta;
public class GetRecListA {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		//map集合存储normalizeB的数据，typemap集合存储tagtimes数据。
		static HashMap<String, String> map = new HashMap<String,String>();
		static HashMap<String, Double> typemap = new HashMap<String,Double>();
		static Text outkey = new Text();
		static Text outvalue = new Text();
		static int typecutoff = 5;//7
		static int actorcutoff = 20;//3
		static DecimalFormat df = new DecimalFormat("0.000");
		static double rate = 0.1, cutoff = 0.17;//0.14
		//输入的是tagsarg数据。
		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			MovieDataMeta meta = new MovieDataMeta(value.toString());
			//获取电影名称
			String name = meta.Gettitle();
			int id=meta.Getidindex();
			//获取电影的演员
			String data = meta.Getactor() + "," + meta.Getdiretor();
			String[] t1 = meta.Getactor().split(",");
			int len = t1.length;
			StringBuffer sbBuffer = new StringBuffer();
			if (t1.length > 2) {
				len = 2;
			}
			for (int i = 0; i < len; i++) {
				sbBuffer.append(",").append(t1[i]);
			}
			//获取电影的导演
			t1 = meta.Getdiretor().split(",");
			len = t1.length;
			if (t1.length > 1) {
				len = 1;
			}
			for (int i = 0; i < len; i++) {
				sbBuffer.append(",").append(t1[i]);
			}
			//将导演和演员作为一个标签数组
			String[] actor = sbBuffer.substring(1).split(",");
			// String[] actor = data.split(",");
			//mapdata 储存演员导演和权重的map集合，datamap存储的是tags和权重的信息
			HashMap<String, Double> mapdata = new HashMap<String,Double>();
			HashMap<String, Double> datamap = new HashMap<String,Double>();
			//对演员和导演数据处理
			for (int i = 0; i < actor.length; i++) {
				if (map.containsKey(actor[i])) {
					String[] temp = map.get(actor[i]).split(",");
					int le = temp.length;
					if (le > 2) {
						le = 2;
					}
					for (int j = 0; j < le; j++) {
						String[] tmp = temp[j].split(":");
						//对权重进行矫正,typemap保存的是每个导演，演员以及标签的出现次数。如果存在该标签的次数
						if (Double.valueOf(tmp[1]) >= cutoff) {
							if (typemap.containsKey(tmp[0].replaceAll(":", "").replaceAll("：", ""))) {
								double d = (Double.valueOf(tmp[1]) * (1 - 0.2 * i))/typemap.get(tmp[0].replaceAll(":", "").replaceAll("：", ""));
								if (mapdata.containsKey(tmp[0])) {
									if (tmp[0].equals("周星驰")) {
										d=(d+mapdata.get(tmp[0]))/temp.length;
									}
									else{
									d += mapdata.get(tmp[0]);
									}
								}
								
								mapdata.put(tmp[0], d);
							}
							//不存在就按照正常衰减的机制。
							else {
								double d = (Double.valueOf(tmp[1]) * (1 - 0.2 * i));
								if (mapdata.containsKey(tmp[0])) {
									d += mapdata.get(tmp[0]);
								}
								mapdata.put(tmp[0], d);
							}	
						}
					}
				}
			}
			//对mapdata进行整理
			Object[] obj = mapdata.keySet().toArray();
			StringBuffer sBuffer = new StringBuffer();
			for (int i = 0; i < obj.length; i++) {
				sBuffer.append(",");
				if (obj[i].toString().equals("周星驰")) {
					double d=mapdata.get(obj[i].toString())/obj.length;
					mapdata.put(obj[i].toString(), d);
			}
				sBuffer.append(obj[i].toString()).append(":");
				sBuffer.append(df.format(mapdata.get(obj[i].toString())));
			}
			
			//对tags标签进行处理,去掉某些重复标签，如美国 美国电影，保留美国电影
			String[] type = RemoveRepeat(meta.Gettags().split(","));

			for (int i = 0; i < type.length; i++) {

				if (map.containsKey("\2" + type[i].replaceAll(":", "").replaceAll("：", ""))) {
					//获取normalizeB中的type标签
					String[] temp = map.get("\2" + type[i].replaceAll(":", "").replaceAll("：", ""))
							.split(",");
					int le = 8;
					if (le > temp.length) {
						le = temp.length;
					}
					//对标签的权重进行矫正
					for (int j = 0; j < le; j++) {
						String[] tmp = temp[j].split(":");
						double sim = (Double.valueOf(tmp[1]) - i * 0.06)/(i+1);
						if (sim > 0) {
							double d = sim
									/ typemap.get(type[i].replaceAll(":", "").replaceAll("：", ""));
							if (datamap.containsKey(tmp[0])) {
								d += datamap.get(tmp[0]);
							}
							datamap.put(tmp[0], d);
						}
					}
				}
			}
          //对datamap进行整理
			obj = datamap.keySet().toArray();
			StringBuffer sBufferA = new StringBuffer();
			//降低美国标签的权重
			for (int i = 0; i < obj.length; i++) {
				double dd = 1.0;
				if (obj[i].toString().equals("美国")) {
					dd = 0.5;
				}
				sBufferA.append(",");
				sBufferA.append(obj[i].toString()).append(":")
						.append(df.format(datamap.get(obj[i].toString()) * rate * dd));
			}

			if (sBuffer.length() > 1 || sBufferA.length() > 1) {
				String str = "";
				if (sBuffer.length() > 1) {
					str = Utils.sortRecList(sBuffer.substring(1), actorcutoff, 1, ",", ":");
					if (sBufferA.length() > 1) {
						str = str + "\t"
								+ Utils.sortRecList(sBufferA.substring(1), typecutoff, 1, ",", ":");
					}
				} else {
					str += "\t" + Utils.sortRecList(sBufferA.substring(1), typecutoff, 1, ",", ":");
				}
           
				outkey.set(name+"\3"+id);
				outvalue.set(str);
				context.write(outkey, outvalue);
			}

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

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			rate = Double.valueOf(context.getConfiguration().get("rate"));
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
				//此目录下有两种格式数据，第一种是normalizeB 李连杰 功夫：0.90，武打：0.80，第二种格式是tagtimes，形式如：李连杰 9
				if (isInteger(info[1])) {
					double d = Double.valueOf(info[1]);
					d = Math.pow(d, 1 / 1.5) / 10;
					if (d < 1.5) {
						d = 1.5;
					}
					typemap.put(info[0], d);
				} else {
					map.put(info[0], info[1]);
				}

			}
		}

		public static boolean isInteger(String value) {
			try {
				Double.parseDouble(value);
				// if (value.contains("."))
				return true;
				// return false;
			} catch (NumberFormatException e) {
				return false;
			}
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException {
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		conf.set("typecutoff", otherArgs[3]);
		conf.set("actorcutoff", otherArgs[4]);
		conf.set("rate", otherArgs[5]);
		conf.set("cutoff", otherArgs[6]);

		Job job = Job.getInstance(conf, "tv movie:GetRecListA");

		Path cachePath = new Path(otherArgs[2]);
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] arr = fs.listStatus(cachePath);
		for (FileStatus fstatus : arr) {
			Path p = fstatus.getPath();
			if (fs.isFile(p)) {
				job.addCacheFile(p.toUri());
			}
		}

		job.setJarByClass(GetRecListA.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
				MapClass.class);

		job.setNumReduceTasks(1);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
