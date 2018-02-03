package com.qiguo.tv.movie.statistics;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.youku.tv.json.JSONException;
import com.youku.tv.json.JSONObject;

public class LogException {
	public static class MapClass extends Mapper<LongWritable, Text, Text, NullWritable>{
		HashSet<String>guidSet = new HashSet<String>();
		protected void map(LongWritable key, Text val, Context context)throws IOException,
		InterruptedException{
			String[] infoStrs = val.toString().split("\t", -1);
			if(infoStrs.length < 27){
				return;
			}
			String[] stk = val.toString().split("\t", -1);
			String ver = "";
			String com ="";
			String pre ="" ;
			int len = stk.length;
			for(int i = len -1; i >= 0; i--){
				if(stk[i].trim().equals("android")){
					for(int j = i-1; j>=0; j--){
						if(stk[j].trim().length()>0){
							com = stk[j];break;
						}
					}
				}
			}
			for(int k = 0; k< len; k++){
				if(stk[k].startsWith("电视家")){
					for(int i = k+1; i< len;i++ ){
						if(stk[i].trim().length() > 0){
							ver = stk[i];break;
						}
					}
				}
			}
			String guid = infoStrs[15];
			if(guidSet.contains(guid)){
				if(infoStrs[25].endsWith("listClick")){
					if(infoStrs[26].equals("cardDetail") || infoStrs[26].equals("cardList")){
						if(isJson(infoStrs[27])){
							JSONObject js = null;
							try {
								js = new JSONObject(infoStrs[27]);
								String cardId = js.getString("cardId");
								if(cardId.equals("personalRec")){
									String timestp = infoStrs[2];
									long time = Long.parseLong(timestp);
							        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
							        String sd = sdf.format(new Date(Long.parseLong(String.valueOf(time))));
							        
									if(js.has("sysver")){
										String sysver = js.getString("sysver");
										String keyStr = guid+"\t"+sd+"\t"+sysver+"\t"+ver+"\t"+com;
										context.write(new Text(keyStr), NullWritable.get());		
									}//else {
									//	String keyStr =  guid+"\t"+sd+"\t"+ver+"\t"+com;
									//	context.write(new Text(keyStr), NullWritable.get());	
									//} 
									
								}	
							} catch (JSONException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}	
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
		public void setup(Context context)throws IOException,InterruptedException{
			Path[] filePaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for(Path path : filePaths){
				loadData(path.toString(), context);
			}
			super.setup(context);
		}
		public void loadData(String path, Context context)throws IOException,
		InterruptedException{
			BufferedReader bfr = new BufferedReader(new FileReader(path));
			String line = "";
			while((line = bfr.readLine()) != null){
				String guid = line.trim();
				guidSet.add(guid);
			}
			bfr.close();
		}
	}
	/*
	public static class ReduceClass extends Reducer<Text, IntWritable, Text, IntWritable>{
		protected void reduce(Text key, Iterable<IntWritable>vals, Context context)throws IOException,
		InterruptedException{
			int sum = 0;
			for(IntWritable i : vals){
				sum += i.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	*/
	public static void main(String[] args)throws IOException,InterruptedException,
	ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		
		Job job = Job.getInstance(conf, "comicClickRelated:uv");
		job.setJarByClass(LogException.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, MapClass.class);
		Path cachePath = new Path(otherArgs[2]);
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] arr = fs.listStatus(cachePath);
		for (FileStatus fstatus : arr) {
			Path p = fstatus.getPath();
			if (fs.isFile(p)) {
				job.addCacheFile(p.toUri());
			}
		}	
		job.setNumReduceTasks(1);
		//job.setReducerClass(ReduceClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
	
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
