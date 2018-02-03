package com.qiguo.tv.movie.relatedRecXgModel;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeSet;

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

import com.qiguo.tv.movie.featCollection.Tup2;
import com.qiguo.tv.movie.featuresCollection.Pair2;
/**
 * 加相关电影的全局统计信息特征
 **/

public class ConvertRelatedItem2FeaturesClkStsFeats {
	public static class MapClass extends Mapper<LongWritable, Text, Text, NullWritable>{
		HashMap<String, String> movieMp = new HashMap<String, String>();
		HashMap<String, String> mvClkStsFtsMp = new HashMap<String, String>();
		int[] featIdxArr = {0};
		//int[] minMaxArr = new int[10];
		protected void map(LongWritable key, Text val, Context context) throws IOException,
		InterruptedException{
			StringTokenizer stk = new StringTokenizer(val.toString(), "\t");
			String rateStr = stk.nextToken().trim();
			String pair = stk.nextToken();
			int idx = pair.indexOf(":");
			String mv1 = pair.substring(0, idx);
			String mv2 = pair.substring(idx+1);
			/*
			ArrayList<int[]> tuplis = new ArrayList<int[]>();
			int clkTot = Integer.parseInt(stk.nextToken());
			int shwTot = Integer.parseInt(stk.nextToken());
			int[] tup = {clkTot, shwTot};
			tuplis.add(tup);
			*/
			int offset = featIdxArr[0] -1; //featIdxArr[0]: join交叉起始id
			int totlen = 2 * offset -1;
			int otherStart = offset * 3;
			if(movieMp.containsKey(mv1) && movieMp.containsKey(mv2)){
				HashMap<Integer, Double>mv1Mp = getMovieMp(movieMp.get(mv1));
				StringTokenizer subStk = new StringTokenizer(movieMp.get(mv2), ", ");
				while (subStk.hasMoreTokens()) {
					Pair2 p2 = new Pair2(subStk.nextToken().trim());
					if(p2.getIdx() < featIdxArr[0]- 2       //上映和时间
							&& mv1Mp.containsKey(p2.getIdx())){ //&& entry.getKey() >= featIdx[0]
						mv1Mp.put(p2.getIdx() + offset, 1.0);
					}else if (p2.getIdx() == featIdxArr[0] - 1  //把 上映时间 也做交叉特征处理
							&& mv1Mp.containsKey(p2.getIdx())) {
						mv1Mp.put(totlen, getDateDiff(p2.getScore(), mv1Mp.get(p2.getIdx())));
					}
					mv1Mp.put(p2.getIdx() + totlen, p2.getScore());
				}
				
				if(mvClkStsFtsMp.containsKey(mv2)){ //  加上该电影的统计特征
					//int stsStart = otherStart; // + tuplis.size() * 2;
					int i = 0;
					for(String v : mvClkStsFtsMp.get(mv2).split("\t")){
						mv1Mp.put(otherStart + i++ , Double.parseDouble(v.trim()));
					}
				}
				String featStr = getSortedFeatures(mv1Mp);
				context.write(new Text(rateStr +" " +featStr), NullWritable.get());
			}
		}
		
		public String getSortedFeatures(HashMap<Integer, Double>totalMap){
			
			TreeSet<Tup2> pairSet = new TreeSet<Tup2>(new Comparator<Tup2>() {
				public int compare(Tup2 t1, Tup2 t2){
					int res = t1.getIdx() < t2.getIdx() ? -1: 1;     //升序
					return res;
				}
			});
			
			for(Map.Entry<Integer, Double>entry : totalMap.entrySet()){
				Tup2 p2 = new Tup2(entry.getKey(), entry.getValue());
				pairSet.add(p2);
			}
			String out = "";
			for(Tup2 t2 : pairSet){
				out += t2.toString() + " ";
			}
			return out;
		}
		public double getDateDiff(double date1, double date2){
			
			double diff = Math.abs(date1 - date2);
			double res = 0.6;
			if(diff <= 0.2){
				res = 1.0;
			}else if (diff < 0.5 && diff > 0.2) {
				res = 0.6;
			}else if (diff >= 0.5 && diff < 0.7) {
				res = 0.3;
			}else if(diff >= 0.7) {
				res = 0.1;
			}
			return res;
		}
		public HashMap<Integer, Double> getMovieMp(String str){
			HashMap<Integer, Double> mvMp = new HashMap<Integer, Double>();
			StringTokenizer stk = new StringTokenizer(str, ", ");
			while (stk.hasMoreTokens()) {
				String p = stk.nextToken().trim();
				Pair2 p2 = new Pair2(p);
				mvMp.put(p2.getIdx(), p2.getScore());
			}
			return mvMp;
		}
		
		public void setup(Context context)throws IOException,InterruptedException{
			featIdxArr[0] = Integer.parseInt(context.getConfiguration().get("featIdx1"));
			//featIdxArr[1] = Integer.parseInt(context.getConfiguration().get("featIdx2"));
			//featIdxArr[2] = Integer.parseInt(context.getConfiguration().get("featIdx3"));
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
			int i = 0;
			while((line = bfr.readLine())!= null){
				if (line.split("\t").length == 2) {
					String mvid = line.split("\t")[0];
					String mvFeatStr = line.split("\t")[1].substring(1);
					mvFeatStr = mvFeatStr.substring(0, mvFeatStr.length()-1);
					movieMp.put(mvid, mvFeatStr);
				}else{
					int idx = line.indexOf("\t");
					mvClkStsFtsMp.put(line.substring(0,idx), line.substring(idx+1));
				}
			}
			bfr.close();
		}
	}
	public static void main(String[] args)throws IOException,
	InterruptedException, ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		conf.set("featIdx1", otherArgs[3]);
		//conf.set("featIdx2", otherArgs[4]);
		//conf.set("featIdx3", otherArgs[5]);
		Job job = Job.getInstance(conf, "ext");
		
		Path cachePath = new Path(otherArgs[2]);
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] arr = fs.listStatus(cachePath);
		for (FileStatus fstatus : arr) {
			Path p = fstatus.getPath();
			if (fs.isFile(p)) {
				job.addCacheFile(p.toUri());
			}
		}
	
		job.setJarByClass(ConvertRelatedItem2FeaturesClkStsFeats.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
				MapClass.class);
		
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
