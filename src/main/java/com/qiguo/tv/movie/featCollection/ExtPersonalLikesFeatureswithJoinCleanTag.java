package com.qiguo.tv.movie.featCollection;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
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


public class ExtPersonalLikesFeatureswithJoinCleanTag {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{
		HashMap<String, Integer>guidLikeskeysMap = new HashMap<String, Integer>();
		HashMap<String, Integer>movietagsmap = new HashMap<String, Integer>();
		HashMap<String, String> cleanMp = new HashMap<String, String>();
		private int joinLikeTagsIdStart = 0;
		private int personalLikeIdStart = 0;
		private double cutRate = 0.1;
		protected void map(LongWritable key, Text val, Context context) throws IOException,
		InterruptedException{
			String[] likeArr = val.toString().split("\t", -1);
			String guid = likeArr[0];
			
			StringTokenizer stk = new StringTokenizer(likeArr[2].substring(5), ",");
			if(stk.countTokens() > 0){
				ArrayList<Tup2>personalList = new ArrayList<Tup2>();
				HashMap<Integer, Double> featMp = new HashMap<Integer, Double>();
				String firStr = stk.nextToken().trim();
				int idx = firStr.lastIndexOf(":");
				String firtkey = firStr.substring(0, idx);
				double maxval = Double.parseDouble(firStr.substring(idx+1).trim());
				
				if(movietagsmap.containsKey(firtkey)){ //判断label第一个key是否为movietag内的Key，是，则写入label对应的值作为交叉特征的值
					//personalList.add(new Tup2(movietagsmap.get(firtkey), maxval));
					featMp.put(movietagsmap.get(firtkey), maxval);
				}else if (cleanMp.containsKey(firtkey) && movietagsmap.containsKey(cleanMp.get(firtkey))) {
					//personalList.add(new Tup2(movietagsmap.get(cleanMp.get(firtkey)), maxval));
					featMp.put(movietagsmap.get(cleanMp.get(firtkey)), maxval);
				}
				
				if(guidLikeskeysMap.containsKey(firtkey)){
					//personalList.add(new Tup2(guidLikeskeysMap.get(firtkey), maxval));
					featMp.put(guidLikeskeysMap.get(firtkey), maxval);
				}else if(guidLikeskeysMap.containsKey(cleanMp.get(firtkey))){
					featMp.put(guidLikeskeysMap.get(cleanMp.get(firtkey)), maxval);
					//personalList.add(new Tup2(guidLikeskeysMap.get(cleanMp.get(firtkey)), maxval));
				}
					
				double cutoff = maxval * cutRate; //截断阈值
				while(stk.hasMoreTokens()){
					String p2 = stk.nextToken().trim();
					int splitidx = p2.lastIndexOf(":");
					double v = Double.parseDouble(p2.substring(splitidx+1).trim());
					String featkey = p2.substring(0, splitidx);
					if(v >= cutoff && guidLikeskeysMap.containsKey(featkey)){
						if(!featMp.containsKey(guidLikeskeysMap.get(featkey))){
							// 对已存在的(清洗Tags后会产生不同的原始key映射为同一个key)
							featMp.put(guidLikeskeysMap.get(featkey), v);
						}
						//personalList.add(new Tup2(guidLikeskeysMap.get(featkey), v));
					}else if(v >= cutoff && guidLikeskeysMap.containsKey(cleanMp.get(featkey))){
						if(!featMp.containsKey(guidLikeskeysMap.get(cleanMp.get(featkey)))){
							featMp.put(guidLikeskeysMap.get(cleanMp.get(featkey)), v);
						}
					}
					// 交叉特征占位，
					if(cleanMp.containsKey(featkey) && movietagsmap.containsKey(cleanMp.get(featkey)) 
							&& !cleanMp.get(featkey).equals("exp") && v >= cutoff ){
						if(!featMp.containsKey(movietagsmap.get(cleanMp.get(featkey)))){
							featMp.put(movietagsmap.get(cleanMp.get(featkey)),  v);
						}
						//personalList.add(new Tup2(movietagsmap.get(cleanMp.get(featkey)), v));
					}else if (movietagsmap.containsKey(featkey) && v >= cutoff) {
						if(!featMp.containsKey(movietagsmap.get(featkey))){
							featMp.put(movietagsmap.get(featkey),  v);
						}
						//personalList.add(new Tup2(movietagsmap.get(featkey), v));
					}
				}
				for(Map.Entry<Integer, Double>entry : featMp.entrySet()){
					personalList.add(new Tup2(entry.getKey(), entry.getValue()));
				}
				context.write(new Text(guid), new Text(personalList.toString()));
			}	
			
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
			cutRate = Double.parseDouble(context.getConfiguration().get("cutrate"));
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
				}else if (fetStr.startsWith("tx")) {
					cleanMp.put(fetStr.substring(2), "exp");
				}else if (fetStr.split(",").length > 2) {
					cleanMp.put(fetStr.split(",")[0], fetStr.split(",")[1]);
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
		conf.set("cutrate", othArgs[5]);
		Job job = Job.getInstance(conf, "guidlikesFeature_cutrate");
		job.setJarByClass(ExtPersonalLikesFeatureswithJoinCleanTag.class);

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
