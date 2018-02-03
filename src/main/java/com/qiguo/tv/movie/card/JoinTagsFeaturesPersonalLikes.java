package com.qiguo.tv.movie.card;

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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.qiguo.tv.movie.featCollection.Tup2;;

public class JoinTagsFeaturesPersonalLikes {
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
		private int[] featId = {0,0,0};
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
						String feat = vStr.substring(2);
						//feat = feat.substring(0, feat.length() - 1);
						feat = lab + " " + feat + "," + likes;
						String featValStr = sortFeatures(feat, featId);
						context.write(new Text(featValStr), NullWritable.get());
					}
				}
			}
		}
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			 featId[0]= Integer.parseInt(context.getConfiguration().get("CardTagsIdStart"));
			 featId[1]= Integer.parseInt(context.getConfiguration().get("joinTagsLikesIdStart"));
			 featId[2] = Integer.parseInt(context.getConfiguration().get("personalLikesIdStart"));
			 super.setup(context);
		}
		
		public String sortFeatures(String featStr, int[] featId){
			int likesMovieTagsStart = featId[1];    // 交叉特征起始id位置
			int likeMovieTagsEnd = featId[2]-1;     // 交叉特征结束id位置
			int movieTagsIdStart = featId[0];
			String libDaformatStr = "";
			StringTokenizer stk = new StringTokenizer(featStr, " ,");
			String lab = stk.nextToken();
			double lael = Double.parseDouble(lab);
			libDaformatStr += lael +" ";
			TreeSet<Tup2> pairSet = new TreeSet<Tup2>(new Comparator<Tup2>() {
				public int compare(Tup2 t1, Tup2 t2){
					int res = t1.getIdx() < t2.getIdx() ? -1: 1;     //升序
					return res;
				}
			});
			HashMap<Integer, Double> midMap = new HashMap<Integer, Double>();//(featStr);
			while (stk.hasMoreTokens()) {				
				String p2 = stk.nextToken().trim();
				Tup2 tup2 = new Tup2(p2);
				if(tup2.getIdx() >= featId[0] && tup2.getIdx() < featId[2]){
					midMap.put(tup2.getIdx(), tup2.getVal());
				}
				pairSet.add(tup2);
			}
			int offset = likesMovieTagsStart - movieTagsIdStart;
	        Iterator<Map.Entry<Integer, Double>> iter = midMap.entrySet().iterator();
	        while(iter.hasNext()){
	            Map.Entry<Integer, Double>entry = iter.next();
	            if(entry.getKey() >= featId[1] && entry.getKey() < featId[2] ){
	                int keyId = entry.getKey() - offset;
	                if(!midMap.containsKey(keyId)){    //设置交叉特征，若当前电影tags特征与用户偏好占位特征id同在则保留；否则删除
	                		iter.remove();              
	                }
	            }
	        }
	        for(Tup2 p : pairSet){
	            if(p.getIdx() >= featId[1] && p.getIdx() < featId[2]){
	                if(midMap.containsKey(p.getIdx())){
	                    libDaformatStr += new Tup2(p.getIdx(), midMap.get(p.getIdx())).toString() +" ";
	                }
	            }else{
	                libDaformatStr += p.toString() +" ";
	            }
	        }
			return libDaformatStr;
		}
	}
	public static void main(String[] args)throws IOException,InterruptedException,
	ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("CardTagsIdStart", otherArgs[2]);
		conf.set("joinTagsLikesIdStart", otherArgs[3]);
		conf.set("personalLikesIdStart", otherArgs[4]);
		Job job = Job.getInstance(conf,"join");
		job.setJarByClass(JoinTagsFeaturesPersonalLikes.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
				MapClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true)? 0:1);
	}
}
