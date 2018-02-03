package com.qiguo.tv.movie.featCollection;

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
import org.apache.hadoop.io.NullWritable;
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
 * @info：对应的键抽取过程：GetPersonalLikesKeyCutOff，
 * 按label第一个最大值的1/10做最后的截断，特征key对应的值作为特征值
 * @param 
 **/
public class GetPersonalLikesFeatures3 {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{
		HashMap<String, Integer>guidLikeskeysMap = new HashMap<String, Integer>();
		HashMap<String, Integer>movietagsmap = new HashMap<String, Integer>();
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
				String firStr = stk.nextToken().trim();
				int idx = firStr.lastIndexOf(":");
				String firkey = firStr.substring(0, idx);
				if(movietagsmap.containsKey(firkey)){
					personalList.add(new Tup2(movietagsmap.get(firkey), 1.0));
				}
				double maxval = Double.parseDouble(firStr.substring(idx+1).trim());
				if(guidLikeskeysMap.containsKey(firkey)){
					personalList.add(new Tup2(guidLikeskeysMap.get(firkey), maxval));
				}		
				double cutoff = maxval * cutRate; //阈值
				while(stk.hasMoreTokens()){
					String p2 = stk.nextToken().trim();
					int splitidx = p2.lastIndexOf(":");
					double v = Double.parseDouble(p2.substring(splitidx+1).trim());
					String featkey = p2.substring(0, splitidx);
					if(v >= cutoff && guidLikeskeysMap.containsKey(featkey)){
						personalList.add(new Tup2(guidLikeskeysMap.get(featkey), v));
					}
					// 交叉特征占位，
					if(movietagsmap.containsKey(featkey) && v >= cutoff){
						personalList.add(new Tup2(movietagsmap.get(featkey), 1.0));
					}
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
		job.setJarByClass(GetPersonalLikesFeatures3.class);

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
