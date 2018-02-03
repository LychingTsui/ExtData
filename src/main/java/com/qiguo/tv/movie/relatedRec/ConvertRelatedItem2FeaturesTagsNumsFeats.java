package com.qiguo.tv.movie.relatedRec;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
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

public class ConvertRelatedItem2FeaturesTagsNumsFeats {
	public static class MapClass extends Mapper<LongWritable, Text, Text, NullWritable>{
		HashMap<String, String>movieItemFeatsMp = new HashMap<String, String>();
		int[] featIdx = {0,0};
		protected void map(LongWritable key, Text val, Context context)throws IOException,
		InterruptedException{
			StringTokenizer stk = new StringTokenizer(val.toString(), " ");
			String firstStr = stk.nextToken();
			double lab = Double.parseDouble(firstStr.substring(0, 1));
			String mvstr1 = firstStr.substring(2);
			String mvstr2 = stk.nextToken();
			if(movieItemFeatsMp.containsKey(mvstr1) &&
					movieItemFeatsMp.containsKey(mvstr2)){
				HashMap<Integer, Double>ftmp1 = getmvFeatsMp(movieItemFeatsMp.get(mvstr1));
				HashMap<Integer, Double>ftmp2 = getmvFeatsMp(movieItemFeatsMp.get(mvstr2));
				//featIdx[0] tagsStartId; featIdx[1] joinTagsStarId
				int offset = featIdx[1] - featIdx[0]; // offset为tags 键的长度,含上映和打分 
				int totlen = featIdx[1] + offset - 3; //?????????
				//交叉特征处理
				HashSet<Pair2> set = new HashSet<Pair2>();
				for(Map.Entry<Integer, Double> entry : ftmp2.entrySet()){
					if(entry.getKey() < featIdx[1] - 2       //2:上映和时间
							&& entry.getKey() >= featIdx[0]
							&& ftmp1.containsKey(entry.getKey())){
						set.add(new Pair2(entry.getKey(), entry.getValue()));
						//ftmp1.put(entry.getKey() + offset, 1.0);
					}
					ftmp1.put(entry.getKey() + totlen, entry.getValue());
				}
				
				if(set.size() > 0){
					double res = setPosTagsLevel(set.size());
					for(Pair2 p2 : set){
						ftmp1.put(p2.getIdx() + offset, res);
					}
				}else {
					HashSet<Pair2> settmp = new HashSet<Pair2>();
					for(Map.Entry<Integer, Double> entry : ftmp1.entrySet()){
						if(entry.getKey() < featIdx[1] - 2       //2:上映和时间
								&& entry.getKey() >= featIdx[0]){
							settmp.add(new Pair2(entry.getKey()+offset, -1.0));
						}
					}
					for(Map.Entry<Integer, Double> entry : ftmp2.entrySet()){
						if(entry.getKey() < featIdx[1] - 2       
								&& entry.getKey() >= featIdx[0]){
							settmp.add(new Pair2(entry.getKey() + offset, -10.0));
						}
					}
					for(Pair2 p2 : settmp){
						ftmp1.put(p2.getIdx(), p2.getScore());
					}
				}
				
				String out = getSortedFeatures(ftmp1);
				out = lab + " " + out;
				context.write(new Text(out), NullWritable.get());
			}
		}
		public double setPosTagsLevel(int size){
			double res = 0.0;
			if(size < 9 && size > 6){
				res = 20.0;
			}else if (size <=6 && size > 4) {
				res = 15.0;
			}else if (size >2 && size <= 4) {
				res = 10.0;
			}else if (size >=1 && size < 3) {
				res = 5.0;
			}
			return res;
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
		public HashMap<Integer, Double>getmvFeatsMp(String featStr){
			HashMap<Integer, Double> mvftsMp = new HashMap<Integer, Double>();
			StringTokenizer stk = new StringTokenizer(featStr, ", ");
			while (stk.hasMoreTokens()) {
				Pair2 p2 = new Pair2(stk.nextToken());
				mvftsMp.put(p2.getIdx(), p2.getScore());
			}
			return mvftsMp;
		}
		
		public void setup(Context context)throws IOException,
		InterruptedException{
			//featIdx[0] = Integer.parseInt(context.getConfiguration().getTrimmed("directIdStart"));
			featIdx[0] = Integer.parseInt(context.getConfiguration().getTrimmed("tagsIdStart"));
			featIdx[1] = Integer.parseInt(context.getConfiguration().getTrimmed("joinTagsIdStart"));
			Path[] cachePath = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for(Path path : cachePath){
				loadData(path.toString(), context);
			}
			super.setup(context);
		}
		public void loadData(String path, Context context)throws IOException,
		InterruptedException{
			BufferedReader bfr = new BufferedReader(new FileReader(path));
			String line = "";
			while ((line = bfr.readLine()) != null) {
				StringTokenizer stk = new StringTokenizer(line, "\t");
				String mvid = stk.nextToken();
				String mvFeatStr = stk.nextToken();
				mvFeatStr = mvFeatStr.substring(1);
				mvFeatStr = mvFeatStr.substring(0, mvFeatStr.length()-1);
				movieItemFeatsMp.put(mvid, mvFeatStr);
			}
			bfr.close();
		}
		
	}
	public static void main(String[] args)throws IOException,
	InterruptedException,ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		
		String[] argsStrs = new GenericOptionsParser(conf, args).getRemainingArgs();
		//conf.set("directIdStart", argsStrs[3]);
		conf.set("tagsIdStart", argsStrs[3]);
		conf.set("joinTagsIdStart", argsStrs[4]);
		
		Job job = Job.getInstance(conf, "extTrainData");
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] fsts = fs.listStatus(new Path(argsStrs[2]));
		for(FileStatus sts : fsts){
			Path path = sts.getPath();
			if(fs.isFile(path)){
				job.addCacheFile(path.toUri());
			}
		}
		MultipleInputs.addInputPath(job, new Path(argsStrs[0]), TextInputFormat.class,
				MapClass.class);
		job.setJarByClass(ConvertRelatedItem2FeaturesTagsNumsFeats.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileOutputFormat.setOutputPath(job, new Path(argsStrs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
