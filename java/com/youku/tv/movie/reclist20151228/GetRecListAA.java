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
public class GetRecListAA {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		static HashMap<String, String> map = new HashMap<String,String>();
		static HashMap<String, Double> typemap = new HashMap<String,Double>();
		static Text outkey = new Text();
		static Text outvalue = new Text();
		static int typecutoff = 5;
		static int actorcutoff = 20;
		static DecimalFormat df = new DecimalFormat("0.000");
		static double rate = 0.1, cutoff = 0.17;

		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			MovieDataMeta meta = new MovieDataMeta(value.toString());

//			String name = meta.Gettitle();
			String name = meta.Getid();

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

			t1 = meta.Getdiretor().split(",");
			len = t1.length;
			if (t1.length > 1) {
				len = 1;
			}
			for (int i = 0; i < len; i++) {
				sbBuffer.append(",").append(t1[i]);
			}

			String[] actor = sbBuffer.substring(1).split(",");
			// String[] actor = data.split(",");

			HashMap<String, Double> mapdata = new HashMap<String,Double>();
			HashMap<String, Double> datamap = new HashMap<String,Double>();

			for (int i = 0; i < actor.length; i++) {
				if (map.containsKey(actor[i])) {
					String[] temp = map.get(actor[i]).split(",");
					int le = temp.length;
					if (le > 2) {
						le = 2;
					}
					for (int j = 0; j < le; j++) {
						String[] tmp = temp[j].split(":");
						if (Double.valueOf(tmp[1]) >= cutoff) {
							double d = Double.valueOf(tmp[1]) * (1 - 0.2 * i);
							if (mapdata.containsKey(tmp[0])) {
								d += mapdata.get(tmp[0]);
							}
							mapdata.put(tmp[0], d);
						}
					}
				}
			}

			Object[] obj = mapdata.keySet().toArray();
			StringBuffer sBuffer = new StringBuffer();
			for (int i = 0; i < obj.length; i++) {
				sBuffer.append(",");
				sBuffer.append(obj[i].toString()).append(":");
				sBuffer.append(df.format(mapdata.get(obj[i].toString())));
			}

			String[] type = RemoveRepeat(meta.Gettags().split(","));

			for (int i = 0; i < type.length; i++) {

				if (map.containsKey("\2" + type[i].replaceAll(":", "").replaceAll("：", ""))) {
					String[] temp = map.get("\2" + type[i].replaceAll(":", "").replaceAll("：", ""))
							.split(",");
					int le = 8;
					if (le > temp.length) {
						le = temp.length;
					}
					for (int j = 0; j < le; j++) {
						String[] tmp = temp[j].split(":");
						double sim = Double.valueOf(tmp[1]) - i * 0.1;
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

			obj = datamap.keySet().toArray();
			StringBuffer sBufferA = new StringBuffer();

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

				outkey.set(name);
				outvalue.set(str);
				context.write(outkey, outvalue);
			}

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

		job.setJarByClass(GetRecListAA.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
				MapClass.class);

		job.setNumReduceTasks(1);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
