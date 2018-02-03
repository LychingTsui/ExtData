package com.qiguo.tv.movie.relatedRec;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
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

import com.qiguo.tv.movie.featuresCollection.Pair2;

import PersonalRecommend.T2;
/**
 * 与TopkPredByRelatedRecModelFullJoin 区别在于，把上映时间也做交叉特征处理，
 * 其余处理相同
 **/

public class TopkPredByRelatedRecModelFullJoinwithDateDiff {
	public static class MapClass extends Mapper<LongWritable, Text, Text, NullWritable>{
		HashMap<String, String>movieItemFeatsMp = new HashMap<String, String>();
		private int[] featIdx = {0,0};  // featidx[0]: tags起始ID，featidx[1]:join 起始id
		HashMap<Integer, Double>weiMp = new HashMap<Integer, Double>();
		private int flag = -1;
		private String group = "";
		private int topK = 30;
		protected void map(LongWritable key, Text val, Context context) throws IOException,
		InterruptedException{
			StringTokenizer stk = new StringTokenizer(val.toString(), "\t");
			String mvid = stk.nextToken();
			String mvFeatStr = stk.nextToken();
			mvFeatStr = mvFeatStr.substring(1);
			mvFeatStr = mvFeatStr.substring(0, mvFeatStr.length()-1);
			
			TreeSet<T2> topKSet = new TreeSet<T2>(new Comparator<T2>() {
		           public int compare(T2 o1, T2 o2) {
		               int res = o1.getVal() > o2.getVal() ? -1 : 1;            //降序
		               return res;
		           }
		       });
			
			for(Map.Entry<String, String>entry : movieItemFeatsMp.entrySet()){
				if(!entry.getKey().equals(mvid)){
					Pair2[] feats = getFeatures(mvFeatStr, entry.getValue());
					double res = predScore(feats, weiMp);
					res = flag == 0 ? -res : res;
					if(topKSet.size() >= topK){
	                       if(res > topKSet.last().getVal()){
	                           topKSet.pollLast();
	                           topKSet.add(new T2(entry.getKey(), res));
	                       }
	                   }else {
	                       topKSet.add(new T2(entry.getKey(), res));
	                   }	
				}
			}
			String  outStr = mvid+"\t";
			int size = topKSet.size();
			int cnt = 1;
			for(T2 p2: topKSet){
				if(cnt == size){
					outStr += p2.toString() + group;
				}else {
					outStr += p2.toString() + group + "," ;
					cnt++;
				}
			}
			context.write(new Text(outStr), NullWritable.get());
		}
		public double predScore(Pair2[] feats, HashMap<Integer, Double>weiMp){
			
			double res = 0.0;
			for(Pair2 p2 : feats){
				res += weiMp.get(p2.getIdx()) * p2.getScore();
			}
			BigDecimal bd = new BigDecimal(res);
	        res = Double.parseDouble(bd.setScale(2, BigDecimal.ROUND_HALF_UP).toString());
			return res;
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
			}else {
				res = 0.1;
			}
			return res;
		}
		public Pair2[] getFeatures(String str1, String str2){

	        HashMap<Integer, Double>ftmp1 = mvId2FeaturesMp(str1);
	        HashMap<Integer, Double>ftmp2 = mvId2FeaturesMp(str2);
	       
	        int offset = featIdx[1] - 1;
	        int firstMvTotLen = offset*2 - 1;   //交叉 无打分特征，有date特征
	        // 交叉特征处理
	        for(Map.Entry<Integer,Double> entry : ftmp2.entrySet()){
	            if(entry.getKey() < featIdx[1] - 2 
	                    && ftmp1.containsKey(entry.getKey())){
	                ftmp1.put(entry.getKey() + offset, 1.0);
	            }else if (entry.getKey() == featIdx[1] - 1 // date 特征
	            		&& ftmp1.containsKey(entry.getKey())) {
					ftmp1.put(firstMvTotLen, getDateDiff(entry.getValue(), ftmp1.get(entry.getKey())));
				}
	            ftmp1.put(entry.getKey() + firstMvTotLen, entry.getValue());
	        }

	        Pair2[] feats = new Pair2[ftmp1.size()];
	        TreeSet<Pair2> pairset = new TreeSet<Pair2>(new Comparator<Pair2>() {
	            public int compare(Pair2 o1, Pair2 o2) {
	                int res = o1.getIdx() < o2.getIdx() ? -1 : 1;   //升序
	                return res;
	            }
	        });

	        for(Map.Entry<Integer, Double> entry : ftmp1.entrySet()){
	            pairset.add(new Pair2(entry.getKey(), entry.getValue()));
	        }

	        int i = 0;
	        for( Pair2 p2 : pairset){
	            feats[i++] = new Pair2(p2.getIdx(), p2.getScore());
	        }
	        return feats;
	    }
		public HashMap<Integer, Double> mvId2FeaturesMp(String mvFtStr){
	        HashMap<Integer, Double> mvFtsMp = new HashMap<Integer, Double>();
	        StringTokenizer stk = new StringTokenizer(mvFtStr, ", ");
	        while(stk.hasMoreTokens()){
	            Pair2 p2 = new Pair2(stk.nextToken());
	            mvFtsMp.put(p2.getIdx(), p2.getScore());
	        }
	        return mvFtsMp;
	    }
		
		public void setup(Context context)throws IOException,
		InterruptedException{
			featIdx[0] = Integer.parseInt(context.getConfiguration().get("featIdx1"));
			featIdx[1] = Integer.parseInt(context.getConfiguration().get("featIdx2"));
			flag = Integer.parseInt(context.getConfiguration().get("flag"));
			group = context.getConfiguration().get("group");
			topK = Integer.parseInt(context.getConfiguration().get("topK"));
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
			while((line = bfr.readLine())!= null){
				if (line.split("\t")[0].length() == 32) {
					String mvid = line.split("\t")[0];
					String mvFeatStr = line.split("\t")[1].substring(1);
					mvFeatStr = mvFeatStr.substring(0, mvFeatStr.length()-1);
					movieItemFeatsMp.put(mvid, mvFeatStr);
				}else if(!line.split(" ")[0].trim().isEmpty() && line.split(" ")[0].length() < 8) {
					StringTokenizer stk = new StringTokenizer(line);
					int idx = Integer.parseInt(stk.nextToken());
					double val = Double.parseDouble(stk.nextToken());
					weiMp.put(idx, val);
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
		conf.set("flag", otherArgs[3]);
		conf.set("featIdx1", otherArgs[4]);
		conf.set("featIdx2", otherArgs[5]);
		conf.set("group", otherArgs[6]);
		conf.set("topK", otherArgs[7]);
		Job job = Job.getInstance(conf, "topK");
		
		Path cachePath = new Path(otherArgs[2]);
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] arr = fs.listStatus(cachePath);
		for (FileStatus fstatus : arr) {
			Path p = fstatus.getPath();
			if (fs.isFile(p)) {
				job.addCacheFile(p.toUri());
			}
		}
	
		job.setJarByClass(TopkPredByRelatedRecModelFullJoinwithDateDiff.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
				MapClass.class);
		
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	
	}
}
