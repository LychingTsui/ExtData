package com.qiguo.tv.movie.featCollection;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

//step3 
/** 把guid 对应的likes特征和所浏览（观看）的item特征拼接
 * 未用!
 */
public class GetJoinFeatures {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{	
		protected void map(LongWritable key, Text val, Context context) throws IOException,
		InterruptedException{
			String strs = val.toString();
			StringTokenizer strinfo = new StringTokenizer(strs, "\t");
			String guid = strinfo.nextToken().trim();
			while(strinfo.hasMoreTokens()){
				String fetStr = strinfo.nextToken().trim();
				if(fetStr.startsWith("[")){
					fetStr = "item"+fetStr;
					context.write(new Text(guid), new Text(fetStr));
				}else if (fetStr.startsWith("1:[") || fetStr.startsWith("0:[")) {
					fetStr = "like" + fetStr;
					context.write(new Text(guid), new Text(fetStr));
				}
			}			
		}
	}
	
	public static class ReduceClass extends Reducer<Text, Text, Text, Text>{
		protected void recude(Text key, Iterable<Text> vals, Context context)throws IOException,
		InterruptedException{
			String likes = null;
			
			for(Text st : vals){
				String tmp = st.toString().trim();
				if(tmp.startsWith("like")){
					likes = tmp.substring(4);
					break;
				}	
			}	
			if(!likes.equals("")){
				for(Text val: vals){
					String tmp = val.toString();
					if(tmp.startsWith("item")){
						tmp = tmp.substring(4);
						String str = tmp + " " + likes;
						context.write(key, new Text(str));
					}
				}
			}	
		}
	}
	
	public static void main(String[] args)throws IOException,
	InterruptedException,ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies","25");
		String[] othArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		Job job = Job.getInstance(conf, "join");
		job.setJarByClass(GetJoinFeatures.class);

		MultipleInputs.addInputPath(job, new Path(othArgs[0]), TextInputFormat.class,
				MapClass.class);
		
		job.setNumReduceTasks(1);
		job.setReducerClass(ReduceClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(othArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	
	
}
