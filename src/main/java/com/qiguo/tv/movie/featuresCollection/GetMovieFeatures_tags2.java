package com.qiguo.tv.movie.featuresCollection;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 *@info: 收集电影的tags词，做key用（已剔除tags含时间年份性的词） 
 **/
public class GetMovieFeatures_tags2 {
	public static class MapClass extends Mapper<LongWritable, Text, Text, NullWritable>{
		protected void map(LongWritable key, Text val, Context context) throws IOException,
		InterruptedException{
			String infoStr = val.toString();
			StringTokenizer stk = new StringTokenizer(infoStr, "\t");
			stk.nextToken();
			while (stk.hasMoreTokens()) {
				String str = stk.nextToken().trim();
				if(str.startsWith("tags")){
					if(str.length() > 5){
						String tmp = str.substring(5);
						StringTokenizer subStk = new StringTokenizer(tmp, ",");
						while(subStk.hasMoreTokens()){
							String subStr = subStk.nextToken().trim(); 
							if( !isNumeric(subStr)){     // 剔除含年份/年代的词
								context.write(new Text(subStr), NullWritable.get());
							}
						}
					}
				}
			}
		}
		public boolean isNumeric(String str){   
	        Pattern pattern = Pattern.compile("^[0-9]+.");
	        Matcher isNum = pattern.matcher(str);
	        if( !isNum.matches() ){
	            return false;
	        }
	        return true;
	    }
	}
	
	public static class ReduceClass extends Reducer<Text, NullWritable, Text, NullWritable>{
		protected void reduce(Text key, Iterable<NullWritable> val, Context context) throws IOException,
		InterruptedException{
			context.write(key, NullWritable.get());
		}
	}
	
	public static void main(String[] args)throws IOException,InterruptedException,
	ClassNotFoundException{
		Configuration conf =  new Configuration();
		conf.set("mapred.reduce.parallel.copies", "30");
		Job job = Job.getInstance(conf, "tags");
		String[] argStrs = new GenericOptionsParser(conf, args).getRemainingArgs();
		MultipleInputs.addInputPath(job, new Path(argStrs[0]), TextInputFormat.class,
				MapClass.class);
		job.setJarByClass(GetMovieFeatures_tags2.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setReducerClass(ReduceClass.class);
		FileOutputFormat.setOutputPath(job, new Path(argStrs[1]));
		System.exit(job.waitForCompletion(true)? 0 : 1);
	}
}
