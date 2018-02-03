package com.qiguo.tv.movie.relatedRec;

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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ExtRelatedRecNegs {
	public static class MapClass extends Mapper<LongWritable, Text, Text, NullWritable>{
		HashMap<String, HashSet<String>>clkRecMp = new HashMap<String, HashSet<String>>();
		protected void map(LongWritable key, Text val, Context context)throws IOException,
		InterruptedException{
			StringTokenizer stk = new StringTokenizer(val.toString(), ",\t");
			String mvkey = stk.nextToken();
			int cnt = 0;
			if(clkRecMp.containsKey(mvkey)){
				while(stk.hasMoreTokens() && cnt < 20){
					String tmp = stk.nextToken().trim();
					int idx = tmp.indexOf(":");
					String mvid = tmp.substring(0, idx);
					cnt++;
					if(!clkRecMp.get(mvkey).contains(mvid)){
						String out = "0:" + mvkey +" "+ mvid;
						context.write(new Text(out), NullWritable.get());
					}
				}
			} else {
				while (stk.hasMoreTokens() && cnt < 20) {   //20
					cnt++;
					String mvid = stk.nextToken().trim();
					int idx = mvid.indexOf(":");
					mvid = mvid.substring(0, idx);
					String out = "0:" + mvkey + " "+ mvid;
					context.write(new Text(out), NullWritable.get());
				}
			} 
		}
		public void setup(Context context)throws IOException,
		InterruptedException{
			Path[] cachePaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for(Path path : cachePaths){
				loadData(path.toString(), context);
			}
			super.setup(context);
		}
		public void loadData(String path, Context context)throws IOException,
		InterruptedException{
			BufferedReader bfr= new BufferedReader(new FileReader(path));
			String line = "";
			while((line = bfr.readLine()) != null){
				StringTokenizer stk = new StringTokenizer(line, ",\t");
				String keyItem = stk.nextToken();
				while (stk.hasMoreTokens()) {
					String item = stk.nextToken();
					if(clkRecMp.containsKey(keyItem)){
						clkRecMp.get(keyItem).add(item);
					}else {
						HashSet<String> set = new HashSet<String>();
						set.add(item);
						clkRecMp.put(keyItem, set);
					}
				}
			}
			bfr.close();
		}
	}
	public static void main(String[] args)throws IOException,
	InterruptedException,ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "30");
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf,"extNeg");
		job.setJarByClass(ExtRelatedRecNegs.class);
		job.setNumReduceTasks(1);
		
		Path cachePath = new Path(otherArgs[2]);
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] filests = 	fs.listStatus(cachePath);
		for(FileStatus fsts : filests){
			Path pt = fsts.getPath();
			if(fs.isFile(pt)){
				job.addCacheFile(pt.toUri());
			}
		}
		
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, 
				MapClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0:1);
	}
}
