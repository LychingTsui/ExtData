package com.qiguo.tv.movie.featCollection;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class ExtPersonalLikesKeyCutOffwithCleanTags {
	public static class MapClass extends Mapper<LongWritable, Text, Text, NullWritable>{
		private double cutRate = 0.1;
		HashMap<String, String>cleanTagsMp = new HashMap<String, String>();
		protected void map(LongWritable key, Text val, Context context)throws IOException,
		InterruptedException{
			String[] likeArr = val.toString().split("\t", -1);
			StringTokenizer stk = new StringTokenizer(likeArr[2].substring(5), ",");
			if(stk.countTokens() > 0){
				String firStr = stk.nextToken().trim();
				int idx = firStr.lastIndexOf(":");
				String firkey = firStr.substring(0,idx);
				double maxval = Double.parseDouble(firStr.substring(idx+1).trim());
				int cutoff = (int) (maxval * cutRate);
				double tmp = maxval;
				if(cleanTagsMp.containsKey(firkey) 
						&& !cleanTagsMp.get(firkey).equals("exp")){
					context.write(new Text(cleanTagsMp.get(firkey)), NullWritable.get());
				}else {
					context.write(new Text(firkey), NullWritable.get());
				}
				
				while(stk.hasMoreTokens() && tmp > cutoff){
					String p2 = stk.nextToken().trim();
					int splitidx = p2.lastIndexOf(":");
					tmp = Double.parseDouble(p2.substring(splitidx+1).trim());
					String featkey = p2.substring(0, splitidx);
					if(cleanTagsMp.containsKey(featkey) 
							&& !cleanTagsMp.get(featkey).equals("exp")){
						context.write(new Text(cleanTagsMp.get(featkey)), NullWritable.get());
					}else {
						context.write(new Text(featkey), NullWritable.get());
					}
				}
			}
		}
		public void setup(Context context)throws IOException, 
		InterruptedException{
			cutRate = Double.parseDouble(context.getConfiguration().get("cutRate"));
			Path[] filePaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for(Path path : filePaths){
				loadData(path.toString(), context);
			}
			super.setup(context);
		}
		public void loadData(String path, Context context)throws IOException, InterruptedException{
			String line = "";
			BufferedReader bfr = new BufferedReader(new FileReader(path));
			while ((line = bfr.readLine()) != null) {
				if(line.startsWith("tx")){
					cleanTagsMp.put(line.substring(2), "exp");
				}else {
					StringTokenizer stk = new StringTokenizer(line, ",");
					String tag0 = stk.nextToken();
					cleanTagsMp.put(tag0, stk.nextToken());
				}
			}
			bfr.close();
		}
	}
	
	public static class ReduceClass extends Reducer<Text, NullWritable, Text, NullWritable>{
		protected void reduce(Text key,Iterable<NullWritable> val, Context context)throws IOException,
		InterruptedException{
			context.write(key, NullWritable.get());
		}
	}
	public static void main(String[] args)throws IOException,
	InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "30");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("cutRate", otherArgs[3]);
		Job job = Job.getInstance(conf, "personalLikesKey");
		job.setJarByClass(ExtPersonalLikesKeyCutOffwithCleanTags.class);
		Path cachePath = new Path(otherArgs[2]);
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] fsts = fs.listStatus(cachePath); 
		for (FileStatus fstus : fsts) {
			Path p = fstus.getPath();
			 if (fs.isFile(p)) {
				 job.addCacheFile(p.toUri());
			 }
		}
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, 
				MapClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
