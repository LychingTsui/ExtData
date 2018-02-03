package com.youku.tv.movieperson.reclist20160420;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
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

import com.youku.tv.showperson.GetDatafromLog.ClickMapClass;
import com.youku.tv.showperson.GetDatafromLog.VVMapClass;

public class ReverseUserShowDataA {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		private Text outkey = new Text(), outval = new Text();
		private double alpha = 3.0;

		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String[] info = StringUtils.split(value.toString(), '\t');
			if (info.length > 2) {
				//此时用户既有点击又有跳过show
				String[] data = StringUtils.split(info[2].substring(2), ',');
				outkey.set(info[0]);
				for (int i = 0; i < data.length; i++) {
					String[] temp = StringUtils.split(data[i], ':');
					//如果一个越靠前的电影用户没点击，那么他的抖动权重将增大 次数 ／限定数 ＋位置／2
					double score = Double.valueOf(temp[3]) / (alpha + Double.valueOf(temp[1]) / 2);
					outval.set(temp[0] + "\t" + score);
					context.write(outkey, outval);

				}
			}else if (info.length==2) {
				String[] data = StringUtils.split(info[1].substring(2), ',');
				outkey.set(info[0]);
				for (int i = 0; i < data.length; i++) {
					String[] temp = StringUtils.split(data[i], ':');
					double score = Double.valueOf(temp[3]) / (alpha + Double.valueOf(temp[1]) / 2);
					outval.set(temp[0] + "\t" + score);
					context.write(outkey, outval);

				}
			}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			alpha = Double.valueOf(context.getConfiguration().get("alpha"));
			super.setup(context);
		}

	}

	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
		private Text outval = new Text();
		static DecimalFormat df = new DecimalFormat("0.000");

		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException {
			HashMap<String, Double> map = new HashMap<String,Double>();
			for (Text text : values) {
				String[] info = StringUtils.split(text.toString(), '\t');
				double d = Double.valueOf(info[1]);
				if (map.containsKey(info[0])) {
					d += map.get(info[0]);
				}
				map.put(info[0], d);
			}
			StringBuffer sBuffer = new StringBuffer();
			Object[] objects = map.keySet().toArray();
			for (int i = 0; i < objects.length; i++) {
				sBuffer.append(",");
				sBuffer.append(objects[i].toString()).append(":")
						.append(df.format(map.get(objects[i].toString())));
			}
			outval.set(sBuffer.substring(1));
			context.write(key, outval);
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException, ParseException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("alpha", otherArgs[2]);
		String date=otherArgs[3];
        String period=otherArgs[4];
        String showpath=otherArgs[0];
		Job job = Job.getInstance(conf, "tv movie:ReverseUserShowDataA");
		job.setJarByClass(ReverseUserShowDataA.class);
		 Calendar start=Calendar.getInstance();
	        Calendar end=Calendar.getInstance();
	        SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd");
	        //格式化当前时间的样式
	        start.setTime(sdf.parse(date));
	        start.add(Calendar.DAY_OF_MONTH, 0);
	        end.setTime(sdf.parse(date));
	        end.add(Calendar.DAY_OF_MONTH, -Integer.parseInt(period));
	        while (start.after(end)) {
	        	MultipleInputs.addInputPath(job, new Path(showpath+sdf.format(start.getTime())), TextInputFormat.class,
	    				MapClass.class);
	    		start.add(Calendar.DAY_OF_MONTH, -1);
			}

		job.setNumReduceTasks(10);
		job.setReducerClass(ReducerClass.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
