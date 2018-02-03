package com.qiguo.tv.movie.featCollection;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/*
 *  输入1： guid	0/1:[arr1:v2 arr2:v2 ....] 
 *  输入2： guid	[id1:v1 id2:v2 ...] //   偏好
 *  输出:   0.0/1.0	id1:v1 id2:v2 ... (liblinear 输入格式)
 * 
 * */

public class GetCombineMovieAndLikesFeatures {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{
		
		protected void map(LongWritable key, Text val, Context context)throws IOException,
		InterruptedException{
			String rowStr = val.toString();
			StringTokenizer stk = new StringTokenizer(rowStr,"\t");
			String uidStr = stk.nextToken();
			String featStr = stk.nextToken();
			context.write(new Text(uidStr), new Text(featStr));
		}
	}
	public static class ReduceClass extends Reducer<Text, Text, Text, NullWritable>{
		static int movTagsIdStart = 0;
		static int joinTagsLikesIdStart = 0;
		static int personalLikesIdStart = 0;
		protected void reduce(Text key, Iterable<Text> vals, Context context) throws IOException,
		InterruptedException{
			String likes = "";
			for(Text val : vals){
				String vastr = val.toString().trim();
				if(vastr.startsWith("[")){
					likes = vastr.substring(1);
					likes = likes.substring(0, likes.length() - 1);
					break;
				}
			}
			if(!likes.equals("")){
				for(Text v : vals){
					String vStr = v.toString();
					if(!vStr.equals(likes)){
						double lab = Double.parseDouble(vStr.substring(0,1));
						String feat = vStr.substring(3);
						feat = feat.substring(0, feat.length() - 1);
						feat = lab + " " + feat + "," + likes;
						String featValStr = sortFeatures(feat);
						context.write(new Text(featValStr), NullWritable.get());
					}
				}
			}
		}
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			movTagsIdStart = Integer.parseInt(context.getConfiguration().get("movieTagsIdStart"));
			joinTagsLikesIdStart = Integer.parseInt(context.getConfiguration().get("joinTagsLikesIdStart"));
			personalLikesIdStart = Integer.parseInt(context.getConfiguration().get("personalLikesIdStart"));
			super.setup(context);
		}
		
		public String sortFeatures(String featStr){
			int likesMovieTagsStart = joinTagsLikesIdStart;
			int likeMovieTagsEnd = personalLikesIdStart-1;
			String libDaformatStr = "";
			StringTokenizer stk = new StringTokenizer(featStr, " ,");
			String lab = stk.nextToken();
			double lael = Double.parseDouble(lab);
			libDaformatStr += lael +" ";
			TreeSet<Tup2> pairSet = new TreeSet<Tup2>(new Comparator<Tup2>() {
				public int compare(Tup2 t1, Tup2 t2){
					int res = t1.getIdx() < t2.getIdx() ? -1: 1;
					return res;
				}
			});
			
			HashMap<Integer, Double> midMap = joinMap(featStr);
			
			while (stk.hasMoreTokens()) {				
				String p2 = stk.nextToken().trim();
				Tup2 tup2 = new Tup2(p2);
				if(tup2.getIdx() >= likesMovieTagsStart && tup2.getIdx() <= likeMovieTagsEnd){
					if(midMap.containsKey(tup2.idx)){
						pairSet.add(new Tup2(tup2.getIdx(), midMap.get(tup2.getIdx())));  
					}					
				}else {
					pairSet.add(tup2);
				}
			}
			for(Tup2 t2 : pairSet){
				libDaformatStr += t2.toString() +" ";
			}
			return libDaformatStr;
		}
		/**处理交叉特征**/
		public HashMap<Integer, Double> joinMap(String featStr){
			StringTokenizer stk = new StringTokenizer(featStr, " ,");
			stk.nextToken(); //lab
			int movieTagStartIdx = movTagsIdStart;
	        int personlikesMovieEndIdx = personalLikesIdStart-1;
			HashMap<Integer, Double>tmpMap = new HashMap<Integer, Double>();
			while (stk.hasMoreTokens()) {				
				String pair = stk.nextToken().trim();
				Tup2 tup = new Tup2(pair);
				if(tup.getIdx() >= movieTagStartIdx && tup.getIdx() <= personlikesMovieEndIdx){
					tmpMap.put(tup.getIdx(), tup.getVal());
				}
			}
			int likesMovieTagsStart = joinTagsLikesIdStart;
	        int likesMovieTagsEnd = personalLikesIdStart-1;
	        int offset = joinTagsLikesIdStart - movTagsIdStart;
	        Iterator<Map.Entry<Integer, Double>> iter = tmpMap.entrySet().iterator();
	        while (iter.hasNext()) {
				Map.Entry<Integer, Double>entry = iter.next();
				if(entry.getKey() >= likesMovieTagsStart && entry.getKey() <= likesMovieTagsEnd){
	                int keyId = entry.getKey() - offset;
	                if(tmpMap.containsKey(keyId)){
	                    tmpMap.put(entry.getKey(), tmpMap.get(keyId));
	                }else {
	                    iter.remove();
	                }
	            }				
			}
	        /*
	        for(Map.Entry<Integer, Double> enty : tmpMap.entrySet()){
	            if(enty.getKey() >= likesMovieTagsStart && enty.getKey() <= likesMovieTagsEnd){
	                int keyId = enty.getKey() - offset;
	                if(tmpMap.containsKey(keyId)){
	                    tmpMap.put(enty.getKey(), tmpMap.get(keyId));
	                }else {
	                    //tmpMap.put(enty.getKey(), 0.0);
	                    tmpMap.remove(enty.getKey());    ///   不可用的删除元素方法！
	                }
	            }
	        }
	        */
	        return tmpMap;
		}
	}
	public static void main(String[] args) throws IOException,
	InterruptedException,ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		conf.set("movieTagsIdStart", otherArgs[2]);
		conf.set("joinTagsLikesIdStart", otherArgs[3]);
		conf.set("personalLikesIdStart", otherArgs[4]);
		Job job = Job.getInstance(conf,"Combinefeatures");
		job.setJarByClass(GetCombineMovieAndLikesFeatures.class);
		
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
				MapClass.class);
		job.setReducerClass(ReduceClass.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true)? 0:1);
	}
	
}
