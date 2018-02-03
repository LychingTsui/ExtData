package com.qiguo.tv.movie.featuresCollection;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class CollectMovieFeaturesTagsCleaning {
	public static class MapClass extends Mapper<LongWritable, Text, Text, NullWritable>{
		HashMap<String, String>cleanTagsMp = new HashMap<String, String>();
		protected void map(LongWritable key, Text val, Context context) throws IOException,
		InterruptedException{
			String infoStr = val.toString();
			StringTokenizer stk = new StringTokenizer(infoStr, "\t");
			stk.nextToken();
			while (stk.hasMoreTokens()) {
				String str = stk.nextToken().trim();
				if(str.startsWith("tags")){
					if(str.length() > 5){
						String tmp = str.substring(5);
						StringTokenizer subStk = new StringTokenizer(tmp, ",");
						while(subStk.hasMoreTokens() ){
							String subStr = subStk.nextToken().trim(); 
							if( !isNumeric(subStr)){     // 剔除含年份/年代的词
								if(cleanTagsMp.containsKey(subStr)){
									if(!cleanTagsMp.get(subStr).equals("exp")){
										context.write(new Text(cleanTagsMp.get(subStr)), NullWritable.get());
									}
								}else {
									context.write(new Text(subStr), NullWritable.get());
								}
							}
						}
					}
				}
			}
		}
		public boolean isNumeric(String str){   
	        Pattern pattern = Pattern.compile("^[0-9]+.");
	        Matcher isNum = pattern.matcher(str);
	        if( !isNum.matches() ){
	            return false;
	        }
	        return true;
	    }
		
		public void setup(Context context) throws IOException, InterruptedException{
			Path[] cachePath = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for(Path p : cachePath){
				loadData(p.toString(), context);
			}
			super.setup(context);
		}
		public void loadData(String path, Context context)throws IOException,InterruptedException{
			BufferedReader bfr = new BufferedReader(new FileReader(path));
			String line = "";
			while ((line = bfr.readLine()) != null) {
				StringTokenizer stk = new StringTokenizer(line, ",");
				if(stk.countTokens() > 2){  //label.txt 清洗的key ','分割大于2列
					String tag = stk.nextToken();
					String cleanedTag = stk.nextToken();
					cleanTagsMp.put(tag, cleanedTag);
				}else {
					String exptag = line.substring(2); //前两个字符标记字符 tx
					cleanTagsMp.put(exptag, "exp");
				}
			}
			bfr.close();
		}
	}
	
	public static class ReduceClass extends Reducer<Text, NullWritable, Text, NullWritable>{
		protected void reduce(Text key, Iterable<NullWritable> val, Context context) throws IOException,
		InterruptedException{
			String tag = "t3"+ key.toString();
			context.write(new Text(tag), NullWritable.get());
		}
	}
	
	public static void main(String[] args)throws IOException,InterruptedException,
	ClassNotFoundException{
		Configuration conf =  new Configuration();
		conf.set("mapred.reduce.parallel.copies", "30");
		Job job = Job.getInstance(conf, "tags");
		String[] argStrs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Path cache = new Path(argStrs[2]);
		
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] fsts = fs.listStatus(cache);
		for(FileStatus fst : fsts){
			Path p = fst.getPath();
			if(fs.isFile(p)){
				job.addCacheFile(p.toUri());
			}
		}
		
		MultipleInputs.addInputPath(job, new Path(argStrs[0]), TextInputFormat.class,
				MapClass.class);
		job.setJarByClass(CollectMovieFeaturesTagsCleaning.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setReducerClass(ReduceClass.class);
		FileOutputFormat.setOutputPath(job, new Path(argStrs[1]));
		System.exit(job.waitForCompletion(true)? 0 : 1);
	}
}
