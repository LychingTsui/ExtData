package com.qiguo.tv.movie.featCollection;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
/**
 * @info: 未使用，
 */
public class GetPersonalLikesFeatures2 {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{
		
		HashMap<String, Integer>guidLikeskeysMap = new HashMap<String, Integer>();
		HashMap<String, Integer>movietagsmap = new HashMap<String, Integer>();
		
		protected void map(LongWritable key, Text val, Context context) throws IOException,
		InterruptedException{
		
			StringTokenizer stk = new StringTokenizer(val.toString(), "\t");
			String uid = stk.nextToken();
			ArrayList<Tup2>personalList = new ArrayList<Tup2>();
			int topCutOff = 50;
			int downCutOff = 10;
			while(stk.hasMoreTokens()){	
				String likes = stk.nextToken().trim();
				
				if(likes.startsWith("label")){
					if(likes.length() > 5){
						String subStr = likes.substring(5);
						StringTokenizer substk = new StringTokenizer(subStr, ",");
						int total = substk.countTokens();					
						int cnt = 0;
						if(total <= downCutOff || total > topCutOff){
						
							while (substk.hasMoreTokens() && (cnt <= downCutOff || cnt <= topCutOff)) {
								String p = substk.nextToken().trim();
								int loc = p.lastIndexOf(":");
								String keyStr = p.substring(0, loc);
								if(movietagsmap.containsKey(keyStr)){
									Tup2 tup2 = new Tup2(movietagsmap.get(keyStr), 1.0);
									personalList.add(tup2);
								}
								if(guidLikeskeysMap.containsKey(keyStr)){
									double sc = 1.0 - 0.01*cnt;
									BigDecimal bd = new BigDecimal(sc);
									sc = Double.parseDouble(bd.setScale(2, BigDecimal.ROUND_HALF_UP).toString());
									Tup2 tup2 = new Tup2(guidLikeskeysMap.get(keyStr), sc);
									personalList.add(tup2);
									cnt++;
								}
							}
						}else {
							int cutoff = new Double(Math.ceil((total - 10) * 0.8)).intValue() ;
							cutoff += 10;
							while (substk.hasMoreTokens() && cnt <= cutoff) {
								String p = substk.nextToken().trim();
								int loc = p.lastIndexOf(":");
								String keyStr = p.substring(0, loc);
								if(movietagsmap.containsKey(keyStr)){
									Tup2 tup2 = new Tup2(movietagsmap.get(keyStr), 1.0);
									personalList.add(tup2);
								}
								if(guidLikeskeysMap.containsKey(keyStr)){
									double sc = 1.0 - cnt*0.01;
									BigDecimal bgd = new BigDecimal(sc);
									sc = Double.parseDouble(bgd.setScale(2, BigDecimal.ROUND_HALF_UP).toString());
									Tup2 tup2 = new Tup2(guidLikeskeysMap.get(keyStr), sc);
									personalList.add(tup2);
									cnt++;
								}
							}
						}
											
					}	
				}				
			}	
			context.write(new Text(uid), new Text(personalList.toString()));
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
			BufferedReader bs = new BufferedReader(new FileReader(path));			
			String line = null;
			int offset1 = 25070; //   前为电影item特征
			int offset2 = 33327;
			int idx1 = offset1 + 1;
			int idx2 = offset2 + 1;
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
	public static void main(String[] args)throws IOException,InterruptedException,
	ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies","30");
		String[] argsStr = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "likesFeaturesCollectV2");
		job.setJarByClass(GetPersonalLikesFeatures2.class);
		
		Path cachePath = new Path(argsStr[2]);
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] fstas = fs.listStatus(cachePath);
		for(FileStatus fst : fstas){
			Path path = fst.getPath();
			if(fs.isFile(path)){
				job.addCacheFile(path.toUri());
			}
		}
		MultipleInputs.addInputPath(job, new Path(argsStr[0]), TextInputFormat.class, MapClass.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(argsStr[1]));
		
		System.exit(job.waitForCompletion(true)? 0: 1);
		
	}
}
