package com.qiguo.tv.movie.statistics;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.youku.tv.json.JSONException;
import com.youku.tv.json.JSONObject;

public class RelatedCMvSts2new {
	public static class MapClass extends Mapper<LongWritable, Text, Text, IntWritable>{
		static final IntWritable one = new IntWritable(1);
		HashSet<String>newMvSet = new HashSet<String>();
		protected void map(LongWritable key, Text val, Context context)throws IOException,
		InterruptedException{
			String info[] = val.toString().split("\t", -1);
			if(info.length < 27)
				return;
			if(info[25].endsWith("listClick")){
				if(info[26].equals("relatedRec")){
					if(info[27].startsWith("{")){
						JSONObject js = null;
						try {
							js = new JSONObject(info[27]);
							boolean flag = false;
							if(js.has("category") && js.getString("category").startsWith("movie")){
								flag = true;
							}else if (js.has("ctg") && js.getString("ctg").startsWith("movie")) {
								flag = true;
							}
							if(flag){
								if(js.has("algGroup")){
									String group = js.get("algGroup").toString();														
									String guid = js.getString("guid");
									JSONObject subjs = new JSONObject(js.getString("videoInfo"));
									String clkMVid = subjs.getString("id");
									//JSONObject subjs2 = new JSONObject(js.getString("refVideoInfo"));
									//String pageMvid = subjs2.getString("id");
									if(newMvSet.contains(clkMVid)){
										if(guid.isEmpty()){guid = info[15];}
										String outStr = group + " " + guid + " " + clkMVid;
										context.write(new Text(outStr), one);
									}
								}
							}
						} catch (JSONException e) {
							e.printStackTrace();
						}	
					}
				}		
			}
		}
		public void setup(Context context)throws IOException, InterruptedException{
			Path[] cachePath = DistributedCache.getLocalCacheFiles(context.getConfiguration()); 
			for(Path path : cachePath){
				loadData(path.toString(), context);
			}
		}
		public void loadData(String path, Context context)throws IOException,
		InterruptedException{
			BufferedReader bfr = new BufferedReader(new FileReader(path));
			String line = "";
			while((line = bfr.readLine()) != null){
				newMvSet.add(line.trim());
			}
			bfr.close();
		}
	}
	public static class ReduceClass extends Reducer<Text, IntWritable, Text, IntWritable>{
		protected void reduce(Text key, Iterable<IntWritable>vals, Context context) throws IOException,
		InterruptedException{
			int sum = 0;
			for(IntWritable v : vals){
				sum += v.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	
	public static void main(String[] args)throws IOException,
	InterruptedException, ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "relatedRecClk:uv");
		job.setJarByClass(RelatedCMvSts2new.class);
		
		Path cachePath = new Path(otherArgs[2]);
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] arr = fs.listStatus(cachePath);
		for (FileStatus fstatus : arr) {
			Path p = fstatus.getPath();
			if (fs.isFile(p)) {
				job.addCacheFile(p.toUri());
			}
		}
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), 
				TextInputFormat.class, MapClass.class);
		job.setNumReduceTasks(1);
	
		job.setReducerClass(ReduceClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
	
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
