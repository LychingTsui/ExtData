package com.qiguo.tv.movie.statistics;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.youku.tv.json.JSONException;
import com.youku.tv.json.JSONObject;

public class CardPersonalRecClickUvSts {
	public static class MapClass extends Mapper<LongWritable, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		public void map(LongWritable key, Text val, Context context)throws IOException,
		InterruptedException{
			String info[] = val.toString().split("\t", -1);
			if(info.length < 27)
				return;
			
			if(info[25].endsWith("listClick")){
				if(info[26].equals("cardDetail")){
					if(isJson(info[27])){
						JSONObject js = null;
						try {
							js = new JSONObject(info[27]);
							String guid = js.getString("guid");
							if(js.has("cardId") && js.getString("cardId").equals("personalRec")){
								String cardId  = js.getString("cardId");
								if(js.has("group")){
									String group = js.getString("group");
									String keyStr = group+" "+ guid+" "+cardId;
									context.write(new Text(keyStr), one);		
								}
							}
						} catch (JSONException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}	
					}
				}else if (info[26].equals("cardList")) {
					if(isJson(info[27])){
						JSONObject js = null;
						try {
							js = new JSONObject(info[27]);
							String guid = js.getString("guid");
							boolean cardidflag = js.has("cardId") && js.getString("cardId").equals("personalRec");
							//boolean idflag = js.has("id") && js.getString("id").equals("personalRec");
							if(cardidflag){
								String cardId  = js.getString("cardId");
								if(js.has("group")){
									String group = js.getString("group");
									String keyStr = group+" "+ guid+" "+cardId;
									context.write(new Text(keyStr), one);		
								}
							}
						} catch (JSONException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}	
					}
				}	
			}	
		}
		public boolean isJson(String str){
			
			if(str.startsWith("{")){
				try {
					JSONObject jsonObject = new JSONObject(str);
					if(jsonObject.has("category")){
						String ctg = jsonObject.get("category").toString();
						if(ctg.startsWith("movie") ){
							return true;
						}
					}
					else if(jsonObject.has("ctg")){
						String ctg = jsonObject.get("ctg").toString();
						if(ctg.startsWith("movie") ){
							return true;
						}
					}
				} catch (com.youku.tv.json.JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}				
			}
			else {
				String strTmp = "{" + str + "}";
				try {
					JSONObject jsonObject = new JSONObject(strTmp);
					if(jsonObject.has("ctg")){
						String ctg = jsonObject.get("ctg").toString();
						if(ctg.startsWith("movie") ){
							return true;
						}
					}
					else if(jsonObject.has("category")){
						String ctg = jsonObject.get("category").toString();
						if(ctg.startsWith("movie")){
							return true;
						}
					}
				} catch (com.youku.tv.json.JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}	
			}
			return false;
		}
		
	}
	public static class ReduceClass extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		public void reduce(Text key, Iterable<IntWritable> val, Context context)throws IOException,
		InterruptedException{
			int sum = 0;
			for(IntWritable i : val){
				sum += i.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	public static void main(String[] args)throws IOException,
	InterruptedException,ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		Job job = Job.getInstance(conf, "cardClick:uv");
		job.setJarByClass(CardPersonalRecClickUvSts.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, MapClass.class);
		job.setNumReduceTasks(1);
	
		job.setReducerClass(ReduceClass.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
	
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
