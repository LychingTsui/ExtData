package com.youku.tv.movieperson.reclist20160420;
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

public class GetUserLabelB {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		static HashMap<String, String> map = new HashMap<String,String>();
		static Text outkey = new Text(), outvalue = new Text();
		static double cutoff = 0.17;
		static DecimalFormat df = new DecimalFormat("0.0000");
		static int typecutoff = 5, actorcutoff = 2;

		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String[] info = value.toString().split("\t");
			if (info.length<3) {
				return;
			}
			String[] person = info[1].split(",");
			String[] type = null;
			if (info.length > 2) {
				type = RemoveRepeat(info[2].split(","));
			}

			HashMap<String, Double> permapdata = new HashMap<String,Double>();
			HashMap<String, Double> typemapdata = new HashMap<String,Double>();
			for (int i = 0; i < person.length; i++) {
				String[] temp = person[i].split(":");
				if (map.containsKey(temp[0])) {
					String[] data = map.get(temp[0]).split(",");
					int len = Math.min(data.length, 2);
					for (int j = 0; j < len; j++) {
						String[] tmp = data[j].split(":");
						if (Double.valueOf(tmp[1]) >= cutoff) {
							double d = Double.valueOf(tmp[1]) * Double.valueOf(temp[1]);
							if (permapdata.containsKey(tmp[0])) {
								d += permapdata.get(tmp[0]);
							}
							permapdata.put(tmp[0], d);
						}
					}
				}
			}

			Object[] obj = permapdata.keySet().toArray();
			StringBuffer sBuffer = new StringBuffer();
			for (int i = 0; i < obj.length; i++) {
				sBuffer.append(",");
				sBuffer.append("\2" + obj[i].toString()).append(":");
				sBuffer.append(df.format(permapdata.get(obj[i].toString())));
			}
			
			StringBuffer sBufferA = new StringBuffer();
			if (type != null) {
				for (int i = 0; i < type.length; i++) {
					String[] temp = type[i].split(":");
					if (map.containsKey("\2" + temp[0])) {
						String[] data = map.get("\2" + temp[0]).split(",");
						int len = Math.min(data.length, 8);
						for (int j = 0; j < len; j++) {
							String[] tmp = data[j].split(":");
							double d = Double.valueOf(tmp[1]) * Double.valueOf(temp[1]);
							if (typemapdata.containsKey(tmp[0])) {
								d += typemapdata.get(tmp[0]);
							}
							typemapdata.put(tmp[0], d);
						}
					}
				}
				obj = typemapdata.keySet().toArray();
				
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
			// if (sBuffer.length() > 1) {
			// str1 = Utils.sortRecList(sBuffer.substring(1), actorcutoff, 1,
			// ",", ":");
			// }
			//
			String str2 = "";
			// if (sBufferA.length() > 1) {
			// str2 = Utils.sortRecList(sBufferA.substring(1), typecutoff, 1,
			// ",", ":");
			// }
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
			for (int i = 0; i < data.length; i++) {
				if (data[i].startsWith("\2")) {
					str1 = str1 + "," + data[i].substring(1);
				} else {
					str2 = str2 + "," + data[i];
				}
			}
			if (str1.length() > 1) {
				str1 = str1.substring(1);
			}
			if (str2.length() > 1) {
				str2 = str2.substring(1);
			}

			outkey.set(info[0]);
			outvalue.set("person" + str1 + "\t" + "label" + str2);
			context.write(outkey, outvalue);

		}

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
				map.put(info[0], info[1]);
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

		job.setNumReduceTasks(1);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
