package com.qiguo.tv.movie.statistics;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PersonalLikesCutoffSts {
	public static class MapClass extends Mapper<LongWritable, Text, Text, NullWritable>{
		private double threshold = 0.0;
		protected void map(LongWritable key, Text val, Context context)throws IOException,
		InterruptedException{
			String likestr = val.toString();
			String[] likeArr = likestr.split("\t", -1);
			String label = likeArr[2].substring(5);
			String guid = likeArr[0];
			StringTokenizer stk = new StringTokenizer(label, ",");
			int tot = stk.countTokens();
			int cnt = 0;
			double max = 0.0;
			while(stk.hasMoreTokens()){
				String pair = stk.nextToken();
				int idx = pair.lastIndexOf(":");
				double sc = Double.parseDouble(pair.substring(idx+1).trim()); 
				if(sc > max){max = sc;}
				if(sc > threshold){cnt++;}				
			}
			String out = guid + "\t" + max + "\t" + cnt + "\t"+tot;
			context.write(new Text(out), NullWritable.get());
		}
		public void setup(Context context)throws IOException, InterruptedException{
			threshold = Double.parseDouble(context.getConfiguration().get("threshold"));
		}
	}
	
	public static void main(String[] args)throws IOException,
	InterruptedException,ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "30");
		
		String[] othargs = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("threshold", othargs[2]);
		Job job = Job.getInstance(conf, "analysis");
		job.setJarByClass(PersonalLikesCutoffSts.class);
		MultipleInputs.addInputPath(job, new Path(othargs[0]), TextInputFormat.class, 
				MapClass.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(othargs[1]));
		System.exit(job.waitForCompletion(true)? 0:1);
		
	}
}
