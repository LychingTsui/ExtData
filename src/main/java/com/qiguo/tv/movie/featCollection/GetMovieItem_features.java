package com.qiguo.tv.movie.featCollection;

import java.beans.IntrospectionException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
/**
 *(version-1.0)
 *从电影元数据获得onehot码，
 *输入：电影元数据
 *缓存：特征key的文件
 *输出格式movieItemfeatures : [id0:val1, id1:val2.....](格式)
 **/
 
public class GetMovieItem_features {
	public static class Mapclass extends Mapper<LongWritable, Text, Text, Text>{	
		HashMap<String, Integer>featCordMp = new HashMap<String, Integer>();		
		protected void map(LongWritable key, Text val, Context context) throws 
		InterruptedException, IOException{
			
			ArrayList<Tup2> list = new ArrayList<Tup2>();
			StringTokenizer stk = new StringTokenizer(val.toString(), "\t");
			
			String movieId = stk.nextToken();
			while(stk.hasMoreTokens()){
				String infoStr = stk.nextToken();
				if(infoStr.startsWith("actor")){
					if(infoStr.length() > 6){
						StringTokenizer subStk = new StringTokenizer(infoStr.substring(6), ",");
						double score = 1.0;
						
						while(subStk.hasMoreTokens() && score > 0.0){
							String str = subStk.nextToken().trim();
							if(featCordMp.containsKey(str)){
								list.add(new Tup2(featCordMp.get(str), score));
								score -= 0.2;	
								BigDecimal sc = new BigDecimal(score);
								score = Double.parseDouble(sc.setScale(2,BigDecimal.ROUND_HALF_UP).toString());
							}
						}		
					}
				}
				else if(infoStr.startsWith("diretor")){
					if(infoStr.length() > 8){
						String dirStr = infoStr.substring(8);
						StringTokenizer subStk = new StringTokenizer(dirStr, ",");
						while(subStk.hasMoreTokens()){
							String str = subStk.nextToken().trim();
							if(featCordMp.containsKey(str)){
								list.add(new Tup2(featCordMp.get(str), 1.0));
							}
						}
					}
				}
				else if(infoStr.startsWith("area")){
					if(infoStr.length() > 5){
						String areaStr = infoStr.substring(5);
						StringTokenizer subStk = new StringTokenizer(areaStr,",");
						double sc = 1.0;
						while(subStk.hasMoreTokens() && sc > 0.0){
							String arstr = subStk.nextToken().trim();
							if(featCordMp.containsKey(arstr)){
								list.add(new Tup2(featCordMp.get(arstr), sc));
								sc -= 0.3;
								BigDecimal scbd = new BigDecimal(sc);
								sc = Double.parseDouble(scbd.setScale(2,BigDecimal.ROUND_HALF_UP).toString());
							}
						}
					}
				}
				else if (infoStr.startsWith("date")) {
					if(infoStr.length() > 5){
						SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
						int start = Integer.parseInt(sdf.format(new Date()).substring(0, 4));;
						int diff = start - Integer.parseInt(infoStr.substring(5));
						double res = yrStep(diff);
						list.add(new Tup2(17479, res));
					}
				}else if (infoStr.startsWith("type")) {
					if(infoStr.length() > 5){
						String typeStr = infoStr.substring(5);
						StringTokenizer substk = new StringTokenizer(typeStr, ",");
						Double sc = 1.0;
						while(substk.hasMoreTokens() && sc > 0.0){
							String ty = substk.nextToken().trim();
							if(featCordMp.containsKey(ty)){
								list.add(new Tup2(featCordMp.get(ty), sc));
								sc = sc - 0.2;
								BigDecimal scbd = new BigDecimal(sc);
								sc = Double.parseDouble(scbd.setScale(2,BigDecimal.ROUND_HALF_UP).toString());
							}
						}
					}
				}
			}		
			context.write(new Text(movieId), new Text(list.toString()));					
		}
		public double yrStep(int diff){
			double res = 0.0;
			if(diff >= 0 && diff <5){
				res = 1.0;
			}
			else if (diff < 10 && diff >= 5) {
				res = 0.8;
			}
			else if (diff < 20 && diff >= 10) {
				res = 0.6;
			}
			else if(diff < 30 && diff >= 20){
				res = 0.4;
			}else if (diff < 50 && diff >= 30) {
				res = 0.1;
			}
			else {
				res = 0.05;
			}
			return res;
		}
		public void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException,
		InterruptedException{
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
			int idx = 1;
			while((line = bs.readLine()) != null){
				String fetStr = line.trim();
				featCordMp.put(fetStr, idx++);
			}
			bs.close();
		}
	}
	
	public static void main(String[] args)throws IOException,
	IntrospectionException, ClassNotFoundException, InterruptedException{
		
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies","25");
		String[] othArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		Job job = Job.getInstance(conf, "movieItemFeature");
		job.setJarByClass(GetMovieItem_features.class);

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
