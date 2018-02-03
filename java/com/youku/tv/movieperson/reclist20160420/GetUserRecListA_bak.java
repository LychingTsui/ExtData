package com.youku.tv.movieperson.reclist20160420;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class GetUserRecListA_bak {
	public static class UserLabelMapClass extends Mapper<LongWritable, Text, TextPair, Text> {
		private TextPair tp = new TextPair();
		private Text outVal = new Text();

		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String[] info = value.toString().split("\t");

			String[] data = info[1].split(",");
			if (info[1].length() > 1) {
				for (int i = 0; i < data.length; i++) {
					String[] tmp = data[i].split(":");
					tp.setText(tmp[0] + "\2" + "person");
					tp.setValue("auser");
					outVal.set("auser" + tmp[1] + "\2" + info[0]);
					context.write(tp, outVal);
				}
			}

			data = info[2].split(",");
			for (int i = 0; i < data.length; i++) {
				String[] tmp = data[i].split(":");
				tp.setText(tmp[0] + "\2" + "label");
				tp.setValue("auser");
				outVal.set("auser" + tmp[1] + "\2" + info[0]);
				context.write(tp, outVal);
			}
		}
	}

	public static class MovieMapClass extends Mapper<LongWritable, Text, TextPair, Text> {
		private TextPair tp = new TextPair();
		private Text outVal = new Text();
		private String idname = "id";

		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			MovieDataMeta meta = new MovieDataMeta(value.toString());

//			String name = meta.Getid();
			String name = String.valueOf(meta.Getidindex());
			if (idname.equals("title")) {
				name = meta.Gettitle().replaceAll(":", "").replaceAll(",", "");
			}

			String[] data = meta.Getactor().split(",");
			int len = Math.min(data.length, 2);
			for (int i = 0; i < len; i++) {
				double sim = 1 - 0.2 * i;
				tp.setText(data[i] + "\2" + "person");
				tp.setValue("movie");
				outVal.set("movie" + sim + "\2" + name);
				context.write(tp, outVal);
			}

			data = meta.Getdiretor().split(",");
			len = Math.min(data.length, 1);
			for (int i = 0; i < len; i++) {
				double sim = (1 - 0.2 * i);
				tp.setText(data[i] + "\2" + "person");
				tp.setValue("movie");
				outVal.set("movie" + sim + "\2" + name);
				context.write(tp, outVal);
			}

			data = meta.Gettags().split(",");
			len = data.length;
			for (int i = 0; i < len; i++) {
				double sim = 1 - i * 0.1;
				String tag = data[i].replaceAll(":", "");
				if (sim > 0) {
					tp.setText(tag + "\2" + "label");
					tp.setValue("movie");
					outVal.set("movie" + sim + "\2" + name);
					context.write(tp, outVal);
				}
			}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			idname = context.getConfiguration().get("idname");
			super.setup(context);
		}
	}

	public static class ReducerClass extends Reducer<TextPair, Text, Text, Text> {
		private Text outKey = new Text(), outVal = new Text();
		static int cutoff = 100;
		static DecimalFormat df = new DecimalFormat("0.000");

		protected void reduce(TextPair key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			HashMap<String, StringBuffer> userList = new HashMap<String,StringBuffer>();
			HashMap<String, Double> userscore = new HashMap<String,Double>();

			Iterator<Text> it = values.iterator();
			String fvalue = ((Text) it.next()).toString();
			if (!fvalue.startsWith("auser")) {
				return;
			} else {
				String[] data = fvalue.substring(5).split("\2");
				userscore.put(data[1], Double.valueOf(data[0]));
			}
			int times = 0;
			while (it.hasNext()) {
				fvalue = ((Text) it.next()).toString();
				if (fvalue.startsWith("auser")) {
					String[] data = fvalue.substring(5).split("\2");
					userscore.put(data[1], Double.valueOf(data[0]));
				} else if (fvalue.startsWith("movie")) {
					String[] data = fvalue.substring(5).split("\2");
					Object[] obj = userscore.keySet().toArray();
					times++;
					for (int i = 0; i < obj.length; i++) {
						double d = Double.valueOf(data[0]) * userscore.get(obj[i].toString());
						if (d < 0.003) {
							continue;
						}

						StringBuffer title = null;
						if (userList.containsKey(obj[i].toString())) {
							title = userList.get(obj[i].toString());
							title.append(",").append(data[1]).append(":").append(df.format(d));
						} else {
							title = new StringBuffer();
							title.append(data[1]).append(":").append(d);
						}

						if (times > cutoff + 100) {
//							String str = Utils.sortRecList(title.toString(), cutoff, 1, ",", ":");
//							title.setLength(0);
//							title.append(str);
//							times = times - 100;
						}
						userList.put(obj[i].toString(), title);
					}
				}
			}

			Object[] obj = userList.keySet().toArray();
			for (int i = 0; i < obj.length; i++) {
				outKey.set(obj[i].toString());
				outVal.set(Utils.sortRecList(userList.get(obj[i].toString()).toString(), cutoff, 1,
						",", ":"));
				context.write(outKey, outVal);
			}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			cutoff = Integer.valueOf(context.getConfiguration().get("cutoff"));
			super.setup(context);
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException {
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		conf.set("mapreduce.reduce.self.java.opts", "-Xmx5120m");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("cutoff", otherArgs[3]);
		conf.set("idname", otherArgs[4]);

		Job job = Job.getInstance(conf, "tv person movie:GetUserRecListA");
		job.setJarByClass(GetUserRecListA_bak.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
				UserLabelMapClass.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class,
				MovieMapClass.class);

		job.setReducerClass(ReducerClass.class);
		job.setNumReduceTasks(20);

		job.setGroupingComparatorClass(TextComparator.class);
		job.setPartitionerClass(KeyPartitioner.class);

		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
