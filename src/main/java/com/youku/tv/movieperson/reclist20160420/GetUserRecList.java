package com.youku.tv.movieperson.reclist20160420;

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

public class GetUserRecList {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		static HashMap<String, String> map = new HashMap<String,String>(), datemap = new HashMap<String,String>(),
				tagmap = new HashMap<String,String>();
		static HashMap<String, Double> ratemap = new HashMap<String,Double>();
		static Text outkey = new Text(), outvalue = new Text();
		static int cutoff = 30;
		static double type = 3.0, rate = 0.7, repeatlabel = 0.2;

		static DecimalFormat df = new DecimalFormat("0.0000");

		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String[] info = value.toString().split("\t");

			ArrayList<String> nameList = Gettypename(info);

			HashMap<String, Double> mapdata = new HashMap<String,Double>();
			if (info[1].length() > 1) {
				String[] data = info[1].split(",");
				for (int i = 0; i < data.length; i++) {
					String[] temp = data[i].split(":");

					if (map.containsKey(temp[0])) {
						String[] tmp = map.get(temp[0]).split("\2");
						for (int j = 0; j < tmp.length; j++) {
							String[] vid = tmp[j].split(":");
							double d = Double.valueOf(temp[1]) * Double.valueOf(vid[1]);
							if (nameList.contains(temp[0])) {
								d = d * repeatlabel;
							}

							if (mapdata.containsKey(vid[0])) {
								d += mapdata.get(vid[0]);
							}
							mapdata.put(vid[0], d);

						}
					}
				}
			}

			HashMap<String, Double> mapda = new HashMap<String,Double>();
			if (info.length > 2 && info[2].length() > 1) {
				String[] data = info[2].split(",");
				for (int i = 0; i < data.length; i++) {
					String[] temp = data[i].split(":");
					if (tagmap.containsKey(temp[0])) {
						String[] tmp = tagmap.get(temp[0]).split("\2");
						for (int j = 0; j < tmp.length; j++) {
							String[] vid = tmp[j].split(":");
							double d = Double.valueOf(temp[1]) * Double.valueOf(vid[1]);

							if (mapda.containsKey(vid[0])) {
								d = d + mapda.get(vid[0]) * rate;
							}
							mapda.put(vid[0], d);
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
					if (ratemap.containsKey(obj[i].toString())) {
						d = d * Math.pow(ratemap.get(obj[i].toString()), type);
					} else {
						d = d * Math.pow(0.65, type);
					}

					if (datemap.containsKey(obj[i].toString())) {
						double daterate = 2;
						try {
							daterate = Math.log(3 + Utils.DateSubYear(obj[i].toString()));
						} catch (ParseException e) {
							e.printStackTrace();
						}
						d = d / daterate;
					}
					mapdata.put(obj[i].toString(), d);
				}
			}

			Object[] obj = mapdata.keySet().toArray();
			StringBuffer sBuffer = new StringBuffer();
			for (int i = 0; i < obj.length; i++) {
				sBuffer.append(",");
				sBuffer.append(obj[i].toString()).append(":");
				sBuffer.append(df.format(mapdata.get(obj[i].toString())));
			}

			if (sBuffer.length() > 1) {
				outkey.set(info[0]);
				try {
					outvalue.set(Utils.sortRecList(sBuffer.substring(1), cutoff, 1, ",", ":"));
//					context.write(outkey, outvalue);
				} catch (Exception e) {					
					outvalue.set(sBuffer.substring(1));
					context.write(outkey, outvalue);
				}
				
			}
		}

		private static ArrayList<String> Gettypename(String[] info) {
			ArrayList<String> nameList = new ArrayList<String>();
			if (info.length > 2 && info[2].length() > 1) {
				String[] data = info[2].split(",");
				for (int i = 0; i < data.length; i++) {
					String[] temp = data[i].split(":");
					nameList.add(temp[0]);
				}
			}
			return nameList;
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			cutoff = Integer.valueOf(context.getConfiguration().get("cutoff"));
			type = Double.valueOf(context.getConfiguration().get("type"));
			rate = Double.valueOf(context.getConfiguration().get("rate"));
			repeatlabel = Double.valueOf(context.getConfiguration().get("repeatlabel"));

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
					String name = meta.Gettitle().replaceAll(":", "") + ":" + (1 - 0.2 * i);
					if (map.containsKey(data[i])) {
						if (!IsContain(map.get(data[i]), name)) {
							name = map.get(data[i]) + "\2" + name;
						} else {
							name = map.get(data[i]);
						}
					}
					map.put(data[i], name);

				}

				data = meta.Getdiretor().split(",");
				len = data.length;
				if (len >= 1)
					len = 1;
				for (int i = 0; i < len; i++) {
					String name = meta.Gettitle().replaceAll(":", "") + ":" + (1 - 0.2 * i);
					if (map.containsKey(data[i])) {
						if (!IsContain(map.get(data[i]), name)) {
							name = map.get(data[i]) + "\2" + name;
						} else {
							name = map.get(data[i]);
						}
					}
					map.put(data[i], name);
				}

				data = meta.Gettags().split(",");
				len = data.length;
				for (int i = 0; i < len; i++) {
					if (!data[i].equals("null") && data[i].length() > 0) {
						double sim = 1 - i * 0.1;
						String name = meta.Gettitle().replaceAll(":", "") + ":" + sim;
						String tag = data[i].replaceAll(":", "");
						if (sim > 0) {
							if (tagmap.containsKey(tag)) {
								if (!IsContain(tagmap.get(tag), name)) {
									name = tagmap.get(tag) + "\2" + name;
								} else {
									name = tagmap.get(tag);
								}

							}
							tagmap.put(tag, name);
						}
					}
				}

				if (meta.Getdate().length() > 1) {
					datemap.put(meta.Gettitle().replaceAll(":", ""), meta.Getdate());
				}

				try {
					double rate = Double.valueOf(meta.Getrating()) / 100;
					if (ratemap.containsKey(meta.Gettitle())) {
						if (rate > ratemap.get(meta.Gettitle())) {
							rate = ratemap.get(meta.Gettitle());
						}
					}
					ratemap.put(meta.Gettitle(), rate);
				} catch (Exception e) {
					continue;
				}
			}
		}

		private static boolean IsContain(String data, String name) {
			String[] info = data.split("\2");
			for (int i = 0; i < info.length; i++) {
				if (info[i].split(":")[0].equals(name.split(":")[0])) {
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
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		conf.set("cutoff", otherArgs[3]);
		conf.set("type", otherArgs[4]);
		conf.set("rate", otherArgs[5]);		
		conf.set("repeatlabel", otherArgs[6]);

		Job job = Job.getInstance(conf, "tv person movie:GetUserRecList");

		Path cachePath = new Path(otherArgs[2]);
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] arr = fs.listStatus(cachePath);
		for (FileStatus fstatus : arr) {
			Path p = fstatus.getPath();
			if (fs.isFile(p)) {
				job.addCacheFile(p.toUri());
			}
		}

		job.setJarByClass(GetUserRecList.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
				MapClass.class);

		job.setNumReduceTasks(1);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
