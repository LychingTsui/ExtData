package com.qiguo.tv.movie.relatedRec;

import java.beans.IntrospectionException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

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

public class ExtMovieItemFeatures {
	public static class Mapclass extends Mapper<LongWritable, Text, Text, Text>{	
		HashMap<String, Integer>actMp = new HashMap<String, Integer>();	
		HashMap<String, Integer>directMap = new HashMap<String, Integer>();
		HashMap<String, Integer>tagsMap = new HashMap<String, Integer>();
		int[] featIdx = {0,0,0}; //featIdx[0] directIdx , featidx[1] tagsIdxstart, featidx[2] join
		static int directIdxStart = 0;
		static int mvTagsIdxStart = 0;
		static int mvTagsIdxEnd = 0;
		protected void map(LongWritable key, Text val, Context context) throws 
		InterruptedException, IOException{
			ArrayList<Tup2> list = new ArrayList<Tup2>();
			StringTokenizer stk = new StringTokenizer(val.toString(), "\t");
			
			String movieId = stk.nextToken();
			while(stk.hasMoreTokens()){
				String infoStr = stk.nextToken();  //uid
				if(infoStr.startsWith("actor")){
					if(infoStr.length() > 6){
						String subStr = infoStr.substring(6);
						StringTokenizer subStk = new StringTokenizer(subStr, ",");
						HashMap<String,Integer> actSet = new HashMap<String, Integer>();
						int cnt = 1;
						while(actSet.size() < 3 && subStk.hasMoreTokens()){       
							String str = subStk.nextToken().trim();
							actSet.put(str, cnt++);
						}						
						double tar = 1.0;	
						for(Map.Entry<String, Integer> entry : actSet.entrySet()){
							if(actMp.containsKey(entry.getKey())){
								double score = tar - (entry.getValue()-1)*0.2;
								BigDecimal sc = new BigDecimal(score);
								score = Double.parseDouble(sc.setScale(2, BigDecimal.ROUND_HALF_UP).toString());
								list.add(new Tup2(actMp.get(entry.getKey()), score));
							}
							
						}								
					}
				}
				else if(infoStr.startsWith("diretor")){
					if(infoStr.length() > 8){
						String dirStr = infoStr.substring(8);
						String direct = dirStr.split(",")[0].trim(); 				
						if(directMap.containsKey(direct)){
							list.add(new Tup2(directMap.get(direct), 1.0));						
						}					
					}
				}
				else if (infoStr.startsWith("tags")) {
					if(infoStr.length() > 5){
						double sc = 0.5;
						int start = 0;
						int cut = 8;
						String tagStr = infoStr.substring(5);
						StringTokenizer subStk = new StringTokenizer(tagStr,",");
						int lenth = subStk.countTokens();
						while (subStk.hasMoreTokens() && start < cut) {     //
							String tagstr = subStk.nextToken().trim();
							if(tagsMap.containsKey(tagstr)){
								BigDecimal bgd = new BigDecimal(sc + 0.05*(lenth - start++));
								double sctmp = Double.parseDouble(bgd.setScale(2, BigDecimal.ROUND_HALF_UP).toString());
								list.add(new Tup2(tagsMap.get(tagstr), sctmp));
							}
						}
					}
				}
				else if(infoStr.startsWith("rating")){
					int idx = mvTagsIdxEnd + 1;
					if(infoStr.length() > 7){
						String ratStr = infoStr.substring(7).trim();
						int score = Integer.parseInt(ratStr);
						double rat = rateStep(score);						
						list.add(new Tup2(idx, rat));
					}else {
						list.add(new Tup2(idx, 0.4));   //均值处
					}
				}
				else if (infoStr.startsWith("date")) {
					int idx = mvTagsIdxEnd + 2;
					if(infoStr.length() > 5){
						String dateStr = infoStr.substring(5).trim();
						if(!dateStr.equals("0")){
							SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
							int start = Integer.parseInt(sdf.format(new Date()).substring(0, 4));;
							int diff = start - Integer.parseInt(infoStr.substring(5));
							double res = yrStep(diff);
							list.add(new Tup2(idx, res));
						}else {
							list.add(new Tup2(idx, 0.5));
						}
					}else {
						list.add(new Tup2(idx, 0.5));   ///  修改
					}
				}				
			}		
			context.write(new Text(movieId), new Text(list.toString()));
		}
		public double yrStep(int diff){
			double res = 0.0;
			if(diff >= 0 && diff < 1){
				res = 1.0;
			}
			else if (diff < 5 && diff >= 2) {
				res = 0.8;
			}
			else if (diff < 10 && diff >= 5) {
				res = 0.6;
			}
			else if(diff < 20 && diff >= 10){
				res = 0.4;
			}else if (diff < 30 && diff >= 20) {
				res = 0.2;
			}else  {
				res = 0.1;
			}
			return res;
		}
		public double rateStep(int score){
			double res = 0.0;
			if(score >= 85){
				res = 1.0;
			}else if (score >=75 && score < 85) {
				res = 0.8;
			}else if (score >= 65 && score < 75) {
				res = 0.6;
			}else if (score >= 55 && score < 65) {
				res = 0.5;
			}else if (score >= 45 && score < 55) {
				res = 0.4;
			}else {
				res = 0.3;           /// 
			}
			return res;
		}
		public void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException,
		InterruptedException{
			directIdxStart = Integer.parseInt(context.getConfiguration().get("directIdxStart"));
			mvTagsIdxStart = Integer.parseInt(context.getConfiguration().get("mvTagsIdxStart"));
			mvTagsIdxEnd = Integer.parseInt(context.getConfiguration().get("mvTagsIdxEnd"));
			Path[] filePaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for(Path path : filePaths){
				loadData(path.toString(), context);
			}
			
			super.setup(context);		
		}
		public void loadData(String path, Context context) throws IOException,
		InterruptedException{
			FileReader fr = new FileReader(path);
			BufferedReader bs = new BufferedReader(fr);			
			String line = null;
			int actIdx = 1;
			int directIdx = directIdxStart;
			int tagsMapIdx = mvTagsIdxStart;
			while((line = bs.readLine()) != null){
				String lintmp = line.trim();
				if(lintmp.startsWith("t1")){
					String strtmp = lintmp.substring(2).trim();
					actMp.put(strtmp, actIdx++);
				}else if(lintmp.startsWith("t2")){
					String strtmp = lintmp.substring(2).trim();
					directMap.put(strtmp, directIdx++);
				}else if (lintmp.startsWith("t3")) {
					String strtmp = lintmp.substring(2).trim();
					tagsMap.put(strtmp, tagsMapIdx++);
				}
			}
			bs.close();
		}
	}
	
	public static void main(String[] args)throws IOException,
	IntrospectionException, ClassNotFoundException, InterruptedException{
		
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies","25");
		String[] othArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		
		conf.set("directIdxStart", othArgs[3]);
		conf.set("mvTagsIdxStart", othArgs[4]);
		conf.set("mvTagsIdxEnd", othArgs[5]);
		Job job = Job.getInstance(conf, "movieItemFeature");
		job.setJarByClass(ExtMovieItemFeatures.class);

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
				Mapclass.class);

		job.setNumReduceTasks(1);
		//job.setReducerClass(cls);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(othArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
