package com.youku.tv.showperson;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

public class GetDatafromLog {
	public static class VVMapClass extends Mapper<LongWritable, Text, Text, Text> {
		private Text outkey = new Text(), outval = new Text();

		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String[] info = StringUtils.splitPreserveAllTokens(value.toString(), "\t");
			String uid = info[0];
			outkey.set(uid);

			for (int i = 1; i < info.length; i++) {
				if (info[i].length() < 20) {
					continue;
				}
				String[] data = StringUtils.splitPreserveAllTokens(info[i], ",");
	            if (data.length<4) {
					return;
				}
				String[] temp = StringUtils.splitPreserveAllTokens(data[3], ":");
				String vidlen = data[1], ts = data[2], date = temp[0], times = temp[1];
				if (!(isNumeric(vidlen)&&isNumeric(ts))) {
					continue;
				}
				String[] episode = StringUtils.splitPreserveAllTokens(data[0], ":");
				String showid = episode[0];
				StringBuffer sBuffer = new StringBuffer();
				for (int j = 1; j < episode.length; j++) {
					if (episode[j].split("_").length<2) {
						return;
					}
					sBuffer.append("\2").append(StringUtils.splitPreserveAllTokens(episode[j], "_")[1]);
				}
				if (sBuffer.length() == 0) {
					sBuffer.append("\2").append("1");
				}

				outval.set("vv" + showid + "," + sBuffer.substring(1) + "," + vidlen + "," + ts
						+ "," + date);
				context.write(outkey, outval);
			}
		}
		public boolean isNumeric(String str){ 
			   Pattern pattern = Pattern.compile("[0-9]*"); 
			   Matcher isNum = pattern.matcher(str);
			   if( !isNum.matches() ){
			       return false; 
			   } 
			   return true; 
			}
	}

	public static class ClickMapClass extends Mapper<LongWritable, Text, Text, Text> {
		private Text outkey = new Text(), outval = new Text();

		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String[] info = StringUtils.splitPreserveAllTokens(value.toString(), "\t");
			String uid = info[0];
			outkey.set(uid);
			for (int i = 1; i < info.length; i++) {
				String[] data = StringUtils.splitPreserveAllTokens(info[i], ",");
				if (data.length<2) {
					return;
				}
				String[] temp = StringUtils.splitPreserveAllTokens(data[1], ":");
				String showid = data[0], date = temp[0];
				try {
					Integer.valueOf(date);
					outval.set("click" + showid + "," + date);
					context.write(outkey, outval);
				} catch (Exception e) {
					continue;
				}
			}
		}
	}

	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
		private Text outval = new Text();

		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException {
			HashMap<String, String> vvshow = new HashMap<String,String>();
			HashMap<String, Integer> clickshow = new HashMap<String,Integer>();
			for (Text text : values) {
				String info = text.toString();
				if (info.startsWith("vv")) {
					String[] data = StringUtils.split(info.substring(2), ",");
					if (data.length!=5) {
						return;
					}
					String showid = data[0];
					String epi = data[1];
					if (!isInteger(data[2])) {
						return;
					}
					long vidlen = Long.valueOf(data[2]);
					if (!isInteger(data[3])) {
						return;
					}
					long ts = Long.valueOf(data[3]);
					if (!isInteger(data[4])) {
						return;
					}
					int date = Integer.valueOf(data[4]);
					if (vvshow.containsKey(showid)) {
						String[] temp = StringUtils.splitPreserveAllTokens(vvshow.get(showid), ":");
						epi = epi + "\2" + temp[0];
						vidlen += Long.valueOf(temp[1]);
						ts += Long.valueOf(temp[2]);
						if (date < Integer.valueOf(temp[3])) {
							date = Integer.valueOf(temp[3]);
						}
					}
					epi = MergeEpisode(epi, "");
					vvshow.put(showid, epi + ":" + vidlen + ":" + ts + ":" + date);

				} else {
					String[] data = StringUtils.splitPreserveAllTokens(info.substring(5), ",");
					String showid = data[0];
					int date = Integer.valueOf(data[1]);
					if (clickshow.containsKey(showid)) {
						if (date < clickshow.get(showid)) {
							date = clickshow.get(showid);
						}
					}
					clickshow.put(showid, date);
				}
			}

			StringBuffer sBuffer = new StringBuffer();
			Object[] objs = vvshow.keySet().toArray();
			for (int i = 0; i < objs.length; i++) {
				sBuffer.append(",");
				sBuffer.append(objs[i].toString()).append(":")
						.append(vvshow.get(objs[i].toString()));
			}

			StringBuffer sBufferA = new StringBuffer();
			objs = clickshow.keySet().toArray();
			for (int i = 0; i < objs.length; i++) {
				sBufferA.append(",");
				sBufferA.append(objs[i].toString()).append(":")
						.append(clickshow.get(objs[i].toString()));
			}

			String val = "vv:";
			if (sBuffer.length() > 1) {
				val = val + sBuffer.substring(1);
			}
			val = val + "\tclick:";
			if (sBufferA.length() > 1) {
				val = val + sBufferA.substring(1);
			}
			outval.set(val);
			context.write(key, outval);
		}

		private String MergeEpisode(String epi1, String epi2) {
			String[] episode;
			ArrayList<Integer> list = new ArrayList<Integer>();
			int n=0;
			if (epi1.length() > 0) {
				episode = StringUtils.splitPreserveAllTokens(epi1, "\2");
				for (int j = 0; j < episode.length; j++) {
					n=Integer.valueOf(episode[j]);
					if (!list.contains(n)) {
						list.add(n);
					}					
				}
			}

			if (epi2.length() > 0) {
				episode = StringUtils.splitPreserveAllTokens(epi2, "\2");
				for (int j = 0; j < episode.length; j++) {
					n=Integer.valueOf(episode[j]);
					if (!list.contains(n)) {
						list.add(n);
					}					
				}
			}

			Collections.sort(list);
			StringBuffer sBuffer = new StringBuffer();
			for (int j = 0; j < list.size(); j++) {
				sBuffer.append("\2").append(list.get(j));
			}
			return sBuffer.substring(1);
		}
		public static boolean isInteger(String value) {
			try {
				Double.parseDouble(value);
				return true;
			} catch (NumberFormatException e) {
				return false;
			}
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException, ParseException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "tv show:GetDatafromLog");
		job.setJarByClass(GetDatafromLog.class);
        String clickPath=otherArgs[1];
        String vvPath=otherArgs[0];
        String date=otherArgs[2];
        String period=otherArgs[3];
        Calendar start=Calendar.getInstance();
        Calendar end=Calendar.getInstance();
        SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd");
        //格式化当前时间的样式
        start.setTime(sdf.parse(date));
        start.add(Calendar.DAY_OF_MONTH, 0);
        end.setTime(sdf.parse(date));
        end.add(Calendar.DAY_OF_MONTH, -Integer.parseInt(period));
        while (start.after(end)) {
        	MultipleInputs.addInputPath(job, new Path(vvPath+sdf.format(start.getTime())), TextInputFormat.class,
    				VVMapClass.class);
    		MultipleInputs.addInputPath(job, new Path(clickPath+sdf.format(start.getTime())), TextInputFormat.class,
    				ClickMapClass.class);
    		start.add(Calendar.DAY_OF_MONTH, -1);
		}
		

		job.setNumReduceTasks(1);
		job.setReducerClass(ReducerClass.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length-1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
