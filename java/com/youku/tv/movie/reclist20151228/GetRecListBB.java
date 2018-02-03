package com.youku.tv.movie.reclist20151228;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobContextImpl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.youku.tv.movie.reclist20151228.MovieDataMeta;
public class GetRecListBB {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		static HashMap<String, String> map = new HashMap<String,String>();
		static HashMap<String, Double> ratemap = new HashMap<String,Double>();
		static HashMap<String, Double> tagmap = new HashMap<String,Double>();
		static Text outkey = new Text();
		static Text outvalue = new Text();
		static int cutoff = 10;
		static double type = 3.0, rate = 0.7, ban = 0.85;

		static DecimalFormat df = new DecimalFormat("0.000");

		protected void map(LongWritable key, Text value, Context context) throws IOException,
		
		
		
				InterruptedException {
			String[] info = value.toString().split("\t");
			HashMap<String, Double> mapdata = new HashMap<String,Double>();
			String[] data;
			if (info[1].length() > 1) {
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
					data = info[1].split(",");
					for (int i = 0; i < data.length; i++) {
						String[] temp = data[i].split(":");
						if (map.containsKey(temp[0])) {
							String[] tmp = map.get(temp[0]).split("\2");
							for (int j = 0; j < tmp.length; j++) {
								double d = Double.valueOf(temp[1]);
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
			}
			HashMap<String, Double> mapda = new HashMap<String,Double>();
			if (info.length > 2 && info[2].length() > 1) {
				data = info[2].split(",");
				for (int i = 0; i < data.length; i++) {
					String[] temp = data[i].split(":");
					if (map.containsKey(temp[0])) {
						String[] tmp = map.get(temp[0]).split("\2");

						for (int j = 0; j < tmp.length; j++) {
							double d = Double.valueOf(temp[1]);
							if (tagmap.containsKey(tmp[j] + "\2" + temp[0])) {
								d = d * tagmap.get(tmp[j] + "\2" + temp[0]);
							}

							if (ratemap.containsKey(tmp[j])) {
								d = d * Math.pow(ratemap.get(tmp[j]), type);
							} else {
								d = d * Math.pow(0.65, type);
							}

							if (mapda.containsKey(tmp[j])) {
								d = d + mapda.get(tmp[j]) * rate;
							}
							mapda.put(tmp[j], d);
						}
					}
				}
			}

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
			for (int i = 0; i < obj.length; i++) {
				if (obj[i].toString().equals(info[0])) {
					continue;
				}
				sBuffer.append(",");
				sBuffer.append(obj[i].toString()).append(":");
				sBuffer.append(df.format(mapdata.get(obj[i].toString()))).append(":");
				sBuffer.append("A").append(":").append("101");
			}

			if (sBuffer.length() > 1) {
				outkey.set(info[0]);
				outvalue.set(Utils.sortRecListBB(sBuffer.substring(1), cutoff, 1, ",", ":"));
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

				MovieDataMeta meta = new MovieDataMeta(line);

				String[] data = meta.Getactor().split(",");
				int len = data.length;
				if (len >= 2)
					len = 2;
				for (int i = 0; i < len; i++) {
					if (!data[i].equals("null")) {
						String name = meta.Getid();
						if (map.containsKey(data[i])) {
							if (!IsContain(map.get(data[i]), name)) {
								name = map.get(data[i]) + "\2" + name;
							} else {
								name = map.get(data[i]);
							}
						}
						map.put(data[i], name);
					}
				}

				data = meta.Getdiretor().split(",");
				len = data.length;
				if (len >= 1)
					len = 1;
				for (int i = 0; i < len; i++) {
					if (!data[i].equals("null")) {
						String name = meta.Getid();
						if (map.containsKey(data[i])) {
							if (!IsContain(map.get(data[i]), name)) {
								name = map.get(data[i]) + "\2" + name;
							} else {
								name = map.get(data[i]);
							}
						}
						map.put(data[i], name);
					}

				}

				data = meta.Gettags().split(",");
				len = data.length;
				for (int i = 0; i < len; i++) {
					if (!data[i].equals("null") && data[i].length() > 0) {
						String name = meta.Getid();
						String tag = data[i].replaceAll(":", "").replaceAll("：", "");
						double sim = 1 - i * 0.1;
						if (sim > 0) {
							if (map.containsKey(tag)) {
								if (!IsContain(map.get(tag), name)) {
									name = map.get(tag) + "\2" + name;
									tagmap.put(name + "\2" + tag, sim);
								} else {
									name = map.get(tag);
								}

							}
							map.put(tag, name);
						}
					}
				}

				try {
					double rate = Double.valueOf(meta.Getrating()) / 100;
					String name = meta.Gettitle();
					if (ratemap.containsKey(name)) {
						if (rate > ratemap.get(name)) {
							rate = ratemap.get(name);
						}
					}
					ratemap.put(name, rate);
				} catch (Exception e) {
					continue;
				}
			}
		}

		private static boolean IsContain(String data, String name) {
			String[] info = data.split("\2");
			for (int i = 0; i < info.length; i++) {
				if (info[i].equals(name)) {
					return true;
				}
			}
			return false;
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException {
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
//		conf.set("mapreduce.map.self.java.opts","-Xmx4096m");
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

		job.setJarByClass(GetRecListBB.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
				MapClass.class);

		job.setNumReduceTasks(1);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
