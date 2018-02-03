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

import com.sun.tools.classfile.Code_attribute.InvalidIndex;
public class GetUserRecListA {
	//用户标签信息的处理，Map阶段
	public static class UserLabelMapClass extends Mapper<LongWritable, Text, TextPair, Text> {
		private TextPair tp = new TextPair();
		private Text outVal = new Text();
		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			//对行数据进行分割
			String[] info = value.toString().split("\t");
			//获取演员或者导演的权重
			String[] data = info[1].substring(6).split(",");
			if (info[1].substring(6).length() > 1) {
				for (int i = 0; i < data.length; i++) {
					String[] tmp = data[i].split(":");
					//for example 刘德华+\2+person
					tp.setText(tmp[0] + "\2" + "person");
					tp.setValue("auser");
					//auser+0.06+\2 da:45:65:uy:dd:yj:23
					outVal.set("auser" + tmp[1] + "\2" + info[0]);
					context.write(tp, outVal);
				}
			}
			//设置标签的权重
			if (info[2].substring(5).length()>1) {
				data = info[2].substring(5).split(",");
				for (int i = 0; i < data.length; i++) {
					String[] tmp = data[i].split(":");
					//恐怖＋\2+label+label
					tp.setText(tmp[0] + "\2" + "label");
					tp.setValue("auser");
					//auser+0.89+\2+da:45:65:uy:dd:yj:23
					outVal.set("auser" + tmp[1] + "\2" + info[0]);
					context.write(tp, outVal);
				}
			}
		}
	}

	public static class MovieMapClass extends Mapper<LongWritable, Text, TextPair, Text> {
		private TextPair tp = new TextPair();
		private Text outVal = new Text();
		private String idname = "id";

		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			//解析电影信息
			MovieDataMeta meta = new MovieDataMeta(value.toString());

			// String name = meta.Getid();
			String name = String.valueOf(meta.Getidindex());
			if (idname.equals("title")) {
				name = meta.Gettitle().replaceAll(":", "").replaceAll(",", "");
			}

			String[] data = meta.Getactor().split(",");
			//获取小于2的演员人数
			int len = Math.min(data.length, 2);
			for (int i = 0; i < len; i++) {
				double sim = 1 - 0.2 * i;
				//刘德华+\2+person
				tp.setText(data[i] + "\2" + "person");
				//是movie形式
				tp.setValue("movie");
				//movie+sim+\2+5665
				outVal.set("movie" + sim + "\2" + name);
				context.write(tp, outVal);
			}
			//获取电影的导演信息
			data = meta.Getdiretor().split(",");
			len = Math.min(data.length, 1);
			for (int i = 0; i < len; i++) {
				double sim = (1 - 0.2 * i);
				//李安＋\2+person
				tp.setText(data[i] + "\2" + "person");
				//是movie形式
				tp.setValue("movie");
				//movie+sim+\2+5665
				outVal.set("movie" + sim + "\2" + name);
				context.write(tp, outVal);
			}
			//获取电影的标签信息
			data = meta.Gettags().split(",");
			len = data.length;
			for (int i = 0; i < len; i++) {
				double sim = 1 - i * 0.1;
				String tag = data[i].replaceAll(":", "");
				if (sim > 0) {
					//爱情+\2+label
					tp.setText(tag + "\2" + "label");
					tp.setValue("movie");
					//movie+sim+\2+5665
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
		static double cutoff = 0.001;
		static DecimalFormat df = new DecimalFormat("0.0000");

		protected void reduce(TextPair key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			HashMap<String, Double> userscore = new HashMap<String,Double>();

			Iterator<Text> it = values.iterator();
			String fvalue = ((Text) it.next()).toString();
			if (!fvalue.startsWith("auser")) {
				return;
			} else {
				String[] data = fvalue.substring(5).split("\2");
				//da:45:65:uy:dd:yj:23 0.89 用户ID 演员或者导演的权重
				userscore.put(data[1], Double.valueOf(data[0]));
			}

			while (it.hasNext()) {
				fvalue = ((Text) it.next()).toString();
				if (fvalue.startsWith("auser")) {
					String[] data = fvalue.substring(5).split("\2");
					//da:45:65:uy:dd:yj:23 0.89 用户ID 演员或者导演的权重
					userscore.put(data[1], Double.valueOf(data[0]));
				} else if (fvalue.startsWith("movie")) {
					String[] data = fvalue.substring(5).split("\2");
					Object[] obj = userscore.keySet().toArray();
					for (int i = 0; i < obj.length; i++) {
						//计算每一个用户对每一部电影的导演、演员和标签的权重
						double d = Double.valueOf(data[0]) * userscore.get(obj[i].toString());
						if (d < cutoff) {
							continue;
						}
						StringBuffer title = new StringBuffer();
						//idindex:0.0786
						title.append(data[1]).append(":").append(df.format(d));
						//用户ID
						outKey.set(obj[i].toString());
						outVal.set(title.toString());
						context.write(outKey, outVal);
					}
				}
			}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			cutoff = Double.valueOf(context.getConfiguration().get("cutoff"));
			super.setup(context);
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException {
		Configuration conf = new Configuration();
		conf.set("mapreduce.reduce.parallel.copies", "25");
		//conf.set("mapred.reduce.parallel.copies", "25");
		conf.set("mapreduce.job.reduces", "1000");
		//conf.set("mapreduce.task.io.sort.factor", "100");
		//conf.set("mapreduce.task.io.sort.mb", "500");
		conf.setBoolean("mapred.output.compress", true);
		conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.BZip2Codec");
		conf.set("mapreduce.reduce.self.java.opts", "-Xmx7120m");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("cutoff", otherArgs[3]);
		conf.set("idname", otherArgs[4]);
		

		Job job = Job.getInstance(conf, "tv person movie:GetUserRecListA");
		job.setJarByClass(GetUserRecListA.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
				UserLabelMapClass.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class,
				MovieMapClass.class);

		job.setReducerClass(ReducerClass.class);
		job.setNumReduceTasks(30);

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
