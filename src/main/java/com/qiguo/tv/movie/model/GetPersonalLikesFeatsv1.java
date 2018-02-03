package com.qiguo.tv.movie.model;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.qiguo.tv.movie.featCollection.Tup2;

/*
 * @info：偏好截取长度为25，偏好从初始的1.0递减值0.1；
 */
public class GetPersonalLikesFeatsv1 {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{
		HashMap<String, Integer>guidLikeskeysMap = new HashMap<String, Integer>();
		HashMap<String, Integer>movietagsmap = new HashMap<String, Integer>();
		static int joinLikeTagsIdStart = 0;
		static int personalLikeIdStart = 0;
		static int likesCutOff = 25;
		protected void map(LongWritable key, Text val, Context context) throws IOException,
		InterruptedException{
		
			StringTokenizer stk = new StringTokenizer(val.toString(), "\t");
			String uid = stk.nextToken();
			ArrayList<Tup2>personalList = new ArrayList<Tup2>();
			while(stk.hasMoreTokens()){	
				String likes = stk.nextToken().trim();
				
				if(likes.startsWith("label")){
					if(likes.length() > 5){
						String subStr = likes.substring(5);
						StringTokenizer substk = new StringTokenizer(subStr, ",");
						int cutoff = likesCutOff;
						int cnt = 0;
						while (substk.hasMoreTokens() && cnt < cutoff) {
							String p = substk.nextToken().trim();
							int loc = p.lastIndexOf(":");
							String keyStr = p.substring(0,loc);		
							if(movietagsmap.containsKey(keyStr)){
								Tup2 tup2 = new Tup2(movietagsmap.get(keyStr), 1.0);
								personalList.add(tup2);
								
							}
							if(guidLikeskeysMap.containsKey(keyStr)){
								double sc = 1.0 - 0.0375 * cnt;  // 递减梯度，使得最后一个偏好key降为为0.1
								BigDecimal bgd = new BigDecimal(sc);
								sc = Double.parseDouble(bgd.setScale(2,BigDecimal.ROUND_HALF_UP).toString());
								Tup2 tup2 = new Tup2(guidLikeskeysMap.get(keyStr), sc);
								personalList.add(tup2);
								cnt++;
							}															
						}						
					}	
				}
				
			}	
			context.write(new Text(uid), new Text(personalList.toString()));
		}
		public boolean isNumeric(String str){ 
			   Pattern pattern = Pattern.compile("^[0-9]*"); 
			   Matcher isNum = pattern.matcher(str);
			   if( !isNum.matches() ){
			       return false; 
			   } 
			   return true; 
		}
		public void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException,
		InterruptedException{
			joinLikeTagsIdStart = Integer.parseInt(context.getConfiguration().get("joinLikeTagsIdStart"));
			personalLikeIdStart = Integer.parseInt(context.getConfiguration().get("personalLikeIdStart"));
			likesCutOff = Integer.parseInt(context.getConfiguration().get("likesCutOff"));
			Path[] filePaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for(Path path : filePaths){
				loadData(path.toString(), context);
			}
			
			super.setup(context);		
		}
		public void loadData(String path, Context context) throws IOException,
		InterruptedException{
			BufferedReader bs = new BufferedReader(new FileReader(path));			
			String line = null;
			
			int idx1 = joinLikeTagsIdStart;
			int idx2 = personalLikeIdStart;
			while((line = bs.readLine()) != null){
				String fetStr = line.trim();
				if(fetStr.startsWith("t3")){
					String tmp = fetStr.substring(2);
					movietagsmap.put(tmp, idx1++);
				}
				else {
					guidLikeskeysMap.put(fetStr, idx2++);
				}
			}
			bs.close();
		}
				
	}
	
	public static void main(String[] args)throws IOException,
	InterruptedException,ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies","25");
		String[] othArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		
		conf.set("joinLikeTagsIdStart", othArgs[3]);
		conf.set("personalLikeIdStart", othArgs[4]);
		conf.set("likesCutOff", othArgs[5]);
		Job job = Job.getInstance(conf, "guidlikesFeature");
		job.setJarByClass(GetPersonalLikesFeatsv1.class);

		 Path cachePath = new Path(othArgs[2]);
		 FileSystem fs = FileSystem.get(conf);
		 FileStatus[] arr = fs.listStatus(cachePath);
		 for (FileStatus fstatus : arr) {
			 Path p = fstatus.getPath();
			 if (fs.isFile(p)) {
				 job.addCacheFile(p.toUri());
			 }
		 }	
		MultipleInputs.addInputPath(job, new Path(othArgs[0]), TextInputFormat.class,
				MapClass.class);

		job.setNumReduceTasks(1);
		//job.setReducerClass(cls);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(othArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
