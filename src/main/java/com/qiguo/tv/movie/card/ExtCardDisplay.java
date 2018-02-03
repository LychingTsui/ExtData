package com.qiguo.tv.movie.card;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.youku.tv.json.JSONArray;
import com.youku.tv.json.JSONException;
import com.youku.tv.json.JSONObject;

public class ExtCardDisplay {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{
		protected void map(LongWritable key, Text val, Context context)throws IOException,
		InterruptedException{
			String[] info = val.toString().split("\t", -1);
			if(info.length < 27) return;
			
			if(info[25].endsWith("listDisplay")){
				if(info[26].equals("cardList")){
					if(info[27].startsWith("{")){
						try {
							JSONObject json = new JSONObject(info[27]);
							String guid = json.getString("guid");
							if(json.has("list")){
								JSONObject subjson = new JSONObject(json.getString("list"));
								@SuppressWarnings("unchecked")
								Iterator<String> iter =  subjson.keys();
								while(iter.hasNext()){
									String id = iter.next();
									if(!id.equals("personalRec")){
										String out = "0:" + id;
										context.write(new Text(guid), new Text(out));
									}
								}
							}
						} catch (JSONException e) {
							e.printStackTrace();
						}
					}
				}
				else if (info[26].equals("cardDetail")) {
					if(info[27].startsWith("{")){
						try {
							JSONObject json = new JSONObject(info[27]);
							String guid = json.getString("guid");
							if(json.has("cardId") && !json.getString("cardId").equals("personalRec")){
								String out = "0:" + json.getString("cardId");
								context.write(new Text(guid), new Text(out));
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
	}
	public static void main(String[] args)throws IOException,
	InterruptedException, ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		Job job = Job.getInstance(conf,"ExtCardNeg");
		job.setJarByClass(ExtCardDisplay.class);
		job.setNumReduceTasks(1);
		
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
				MapClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
