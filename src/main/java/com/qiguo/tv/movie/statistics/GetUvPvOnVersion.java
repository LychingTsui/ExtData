package com.qiguo.tv.movie.statistics;

//import com.qiguo.Utils.LogUtils;
import com.youku.tv.json.JSONObject;
import com.youku.tv.movie.reclist20151228.GetTagsTimes;
import com.youku.tv.movie.reclist20151228.GetTagsTimes.MapClass;
import com.youku.tv.movie.reclist20151228.GetTagsTimes.ReducerClass;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONTokener;

public class GetUvPvOnVersion {
	public static class Mymap extends Mapper<LongWritable,Text,Text,IntWritable>{
		//private static Text outKey = new Text();
		//static String ver = "";
		private final static IntWritable one = new IntWritable(1);
		public void map(LongWritable key, Text value, Context context) 
				throws IOException,InterruptedException{
			//Counter counter = (Counter)context.getCounter("counter","all");
			
			try{
				String[] info = value.toString().split("\t");
				//System.out.println(info.length);
				for(String str : info){
					if(str.startsWith("{") && 
							str.contains("guid") &&
							str.contains("category")){
						JSONObject json = new JSONObject(str);
						String ctg = json.getString("category");
						if(ctg.startsWith("movie")){
							String guid = json.getString("guid");
							context.write(new Text(guid), one);
						}
					}
				}
			} catch (com.youku.tv.json.JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
		}	
		//protected void setup(Context context)throws IOException,InterruptedException{
			//ver = String.valueOf(context.getConfiguration().get("ver"));
			//super.setup(context);
		//}
	}
	
	
	public static class MyReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
		public void reduce(Text text,Iterable<IntWritable> value,Context context)
				throws IOException,InterruptedException {
			int sum = 0;
			for(IntWritable val : value){
				sum += val.get();
			}
			context.write(text, new IntWritable(sum));
			
		}
	}
	
	public static void main(String[] args)throws IOException,InterruptedException,
	ClassNotFoundException  {
		Configuration conf = new Configuration();
		
		conf.set("mapred.reduce.parallel.copies", "25");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		//conf.set("ver", otherArgs[2]);
		
		Job job = Job.getInstance(conf, "tv movie:getUv&PvOnVersion");
		job.setJarByClass(GetUvPvOnVersion.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,Mymap.class);
		job.setNumReduceTasks(1);
		//job.setCombinerClass(MyReduce.class);
		job.setReducerClass(MyReduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}
