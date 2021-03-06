package com.qiguo.tv.movie.statistics;

import java.io.IOException;

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

import com.youku.tv.json.JSONArray;
import com.youku.tv.json.JSONException;
import com.youku.tv.json.JSONObject;

public class CardPersonalRecDisplayuvSts {
	public static class MapClass extends Mapper<LongWritable, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		public void map(LongWritable key, Text val, Context context)throws IOException,
		InterruptedException{
			String info[] = val.toString().split("\t", -1);
			if(info.length < 27)
				return;
			if(info[25].endsWith("listDisplay")){
				if(info[26].equals("cardDetail")){
					if(isJson(info[27])){
						JSONObject js = null;
						try {
							js = new JSONObject(info[27]);
							String guid = js.getString("guid").toString();
							if(js.has("cardId")){
								if(js.getString("cardId").equals("personalRec")){
									if(js.has("list")){
										JSONArray jsarr = getArray(js.getString("list"));
										String group = jsarr.getJSONObject(0).get("group").toString();
										String outstr = group+"\t"+ guid + "\t" + js.getString("cardId");
										context.write(new Text(outstr), one);
									}
								}
							}/*else if (js.has("id") && js.getString("id").equals("personalRec")){
								JSONArray jsarr = getArray(js.getString("list"));
								String group = jsarr.getJSONObject(0).get("group").toString();
								String outstr = group+"\t"+ guid + "\t" + js.getString("cardId");
								context.write(new Text(outstr), one);
							}
							*/
						} catch (JSONException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}	
					}
				}else if(info[26].equals("cardList")){
					if(isJson(info[27])){
						JSONObject  js = null;
						try {
							js = new JSONObject(info[27]);
							String guid = js.getString("guid");
							if(js.has("list")){
								if(new JSONObject(js.getString("list")).has("personalRec")){
									String psnRec = new JSONObject(js.getString("list")).get("personalRec").toString();
									
									String group = getArray(psnRec).getJSONObject(0).get("group").toString();
									String outStr = group + "\t" + guid + "\t" + "personalRec";
									context.write(new Text(outStr), one);
								}
							}
						} catch (JSONException e) {
							e.printStackTrace();
						}
					}				
				}
			}	
		}
		public JSONArray getArray(String str) throws JSONException{
			JSONArray jsa = new JSONArray(str);
			return jsa;
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
		job.setJarByClass(CardPersonalRecDisplayuvSts.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, MapClass.class);
		job.setNumReduceTasks(1);
	
		job.setReducerClass(ReduceClass.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
	
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
