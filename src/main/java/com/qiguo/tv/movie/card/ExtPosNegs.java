package com.qiguo.tv.movie.card;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
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
 * 在浏览纪录(neg)中剔除正样本(pos)，其余全为负样本写入
 **/
public class ExtPosNegs {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{
		HashMap<String, HashSet<String>> personalClkHis = new HashMap<String, HashSet<String>>();
		protected void map(LongWritable key, Text val, Context context) throws IOException,
		InterruptedException{
			StringTokenizer stk = new StringTokenizer(val.toString(), "\t");
			String guid = stk.nextToken();
			String labCardid = stk.nextToken();
			int idx = labCardid.indexOf(":");
			if(personalClkHis.containsKey(guid)){
				String cardid = labCardid.substring(idx+1);
				if(!personalClkHis.get(guid).contains(cardid)){
					context.write(new Text(guid), new Text(labCardid));
				}
			}else {
				context.write(new Text(guid), new Text(labCardid));
			}
		}
		protected void setup(Context context)throws IOException,
		InterruptedException {
			Path[] cachePth = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for(Path path : cachePth){
				loadData(context, path.toString());
			}
			super.setup(context);
		}
		protected void loadData(Context context, String path)throws IOException,
		InterruptedException {
			BufferedReader bfr = new BufferedReader(new FileReader(path));
			String line = "";
			while((line = bfr.readLine()) != null){
				StringTokenizer stk = new StringTokenizer(line, "\t");
				String guid = stk.nextToken();
				String posStr = stk.nextToken();
				int idx = posStr.indexOf(":");
				String cardid = posStr.substring(idx+1);
				if(personalClkHis.containsKey(guid)){
					personalClkHis.get(guid).add(cardid);
				}else {
					HashSet<String> set = new HashSet<String>();
					set.add(cardid);
					personalClkHis.put(guid, set);
				}
			}
			bfr.close();
		}
	}
	public static void main(String[] args)throws IOException,
	InterruptedException,ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies","25");
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "extCardPosNeg");
		job.setJarByClass(ExtPosNegs.class);
		
		Path cachePth = new Path(otherArgs[2]);
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] fsts = fs.listStatus(cachePth);
		for(FileStatus fileStatus : fsts){
			Path path = fileStatus.getPath();
			if(fs.isFile(path)){
				job.addCacheFile(path.toUri());
			}
		}
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, 
				MapClass.class);
		job.setNumReduceTasks(1);
		//job.setReducerClass(ReduceClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
