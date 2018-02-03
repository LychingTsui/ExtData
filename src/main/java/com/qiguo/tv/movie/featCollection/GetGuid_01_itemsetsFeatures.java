package com.qiguo.tv.movie.featCollection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.qiguo.tv.movie.featuresCollection.Pair2;
/***
 * @author Tsui
 * 
 */
public class GetGuid_01_itemsetsFeatures {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{
		protected void map(LongWritable key, Text val, Context context)throws IOException,
		InterruptedException{
			StringTokenizer sToken = new StringTokenizer(val.toString(),"\t");
			String guid = sToken.nextToken().trim();
			String record = sToken.nextToken().trim();
			context.write(new Text(guid), new Text(record));
		}
	}
	public static class ReduceClass extends Reducer<Text, Text, Text, Text>{
		protected void reduce(Text key, Iterable<Text>vals, Context context)throws IOException,
		InterruptedException{
			String posStr = "";
			String negStr = "";
			HashMap<Integer, Double> posMap = null ; 
			HashMap<Integer, Double>negMap = null;
			for(Text txt : vals){
				String txtstr = txt.toString().trim();
				if(txtstr.startsWith("1:[")){				
					posStr = txtstr.substring(3);
					posStr = posStr.substring(0, posStr.length()-1);
					if(posMap == null){
						posMap = getUidItemFeatMap(posStr);
					}else {
						StringTokenizer skon = new StringTokenizer(posStr,",");
						while(skon.hasMoreTokens()){
							String tmp = skon.nextToken();
							Pair2 p = new Pair2(tmp.trim());
							if(posMap.containsKey(p.getIdx())){
								double val = posMap.get(p.getIdx())+1.0;
								posMap.put(p.getIdx(), val);
							}
							else {
								posMap.put(p.getIdx(), p.getScore());
							}
						}
					}	
				}
				else if (txt.toString().startsWith("0:[")) {				
					negStr = txtstr.substring(3);
					negStr = negStr.substring(0, negStr.length()-1);
					if(negMap == null){
						negMap = getUidItemFeatMap(negStr);
					}else {
						StringTokenizer skon = new StringTokenizer(negStr, ",");
						while(skon.hasMoreTokens()){
							String tmp = skon.nextToken();
							Pair2 p = new Pair2(tmp.trim());
							if(negMap.containsKey(p.getIdx())){
								double val = negMap.get(p.getIdx())+1.0;
								negMap.put(p.getIdx(), val);
							}
							else {
								negMap.put(p.getIdx(), p.getScore());
							}
						}
					}
				}
			}
			////////////////////////////////
			if(posMap != null){		
				ArrayList<Pair2> p2list = new ArrayList<Pair2>();
				for(Map.Entry<Integer, Double>entry : posMap.entrySet()){
					p2list.add(new Pair2(entry.getKey(),entry.getValue()));
				}
				context.write(key, new Text("1:" + p2list.toString()));
			}
			if(negMap != null){
				ArrayList<Pair2>p2lis = new ArrayList<Pair2>();
				for(Map.Entry<Integer, Double>entry : negMap.entrySet()){
					p2lis.add(new Pair2(entry.getKey(),entry.getValue()));
				}
				context.write(key, new Text("0:" + p2lis.toString()));
			}
		}
		public HashMap<Integer, Double>getUidItemFeatMap(String str){
			HashMap<Integer, Double>map = new HashMap<Integer, Double>();
			StringTokenizer sTok = new StringTokenizer(str,",");
			while(sTok.hasMoreTokens()){
				String tmp = sTok.nextToken().trim();
				Pair2 p2 = new Pair2(tmp);
				map.put(p2.getIdx(), p2.getScore());
			}
			return map;
		}
	}
	
	public static void main(String[] args)throws IOException,
	InterruptedException,ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies","25");
		String[] othArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		Job job = Job.getInstance(conf, "featcollect");
		job.setJarByClass(GetGuid_01_itemsetsFeatures.class);

		MultipleInputs.addInputPath(job, new Path(othArgs[0]), TextInputFormat.class,
				MapClass.class);
		
		job.setNumReduceTasks(1);
		job.setReducerClass(ReduceClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(othArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
