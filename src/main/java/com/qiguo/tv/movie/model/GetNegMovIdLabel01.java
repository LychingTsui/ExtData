package com.qiguo.tv.movie.model;

import java.io.BufferedReader;
import java.io.File;
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

/*
 * @param input:vvhistory，
 * 		  cache: 某天的show数据（有vv纪录的用户的show，已剔除无vv用户的show数据）
 * @info: 从show中剔除某user在vv中含有的movie，其余show数据为纯净的负样本数据，标记0:movieId
 */
public class GetNegMovIdLabel01 {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{
		HashMap<String, HashSet<String>> uidshowMp = new HashMap<String, HashSet<String>>();
		
		protected void map(LongWritable key, Text val, Context context)throws IOException,
		InterruptedException{
			/*读历史观看记录，剔除已经观看的电影在show里记录*/
			StringTokenizer stk = new StringTokenizer(val.toString()," \t");
			String guid = stk.nextToken(); 
			if(uidshowMp.containsKey(guid)){
				//String[] vvMovStrs = stk.nextToken().split(" "); ///?
				HashSet<String> vvset = new HashSet<String>();
				while(stk.hasMoreTokens()){
					vvset.add(stk.nextToken());
				}
				for(String mvid : uidshowMp.get(guid)){
					if(!vvset.contains(mvid)){
						String mvidlab = "0:"+mvid;
						context.write(new Text(guid), new Text(mvidlab));
					}
				}
			}			
		}
		public void setup(Context context)throws IOException,InterruptedException{
			Path[] filePaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for(Path path : filePaths){
				loadData(path.toString(), context);
			}
			super.setup(context);
		}
		public void loadData(String path, Context context)throws IOException,InterruptedException{
			
			BufferedReader bfr = new BufferedReader(new FileReader(path));
			String line = "";
			while((line = bfr.readLine()) != null){
				HashSet<String> mvSet = new HashSet<String>();
				StringTokenizer stk = new StringTokenizer(line);
				String guid = stk.nextToken();
				while (stk.hasMoreTokens()) {
					mvSet.add(stk.nextToken());
				}
				uidshowMp.put(guid, mvSet);
			}
			bfr.close();
		}
	}
	public static void main(String[] args)throws IOException, InterruptedException,
	ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies","25");
		String[] othArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		Job job = Job.getInstance(conf, "neg");
		job.setJarByClass(GetNegMovIdLabel01.class);

		 Path cachePath = new Path(othArgs[2]);
		 FileSystem fs = FileSystem.get(conf);
		 FileStatus[] arr = fs.listStatus(cachePath);
		 for (FileStatus fstatus : arr) {
			 Path p = fstatus.getPath();
			 if (fs.isFile(p)) {
				 job.addCacheFile(p.toUri());
			 }
		 }	
		MultipleInputs.addInputPath(job, new Path(othArgs[0]), TextInputFormat.class,
				MapClass.class);
		job.setNumReduceTasks(1);
		//job.setReducerClass(cls);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(othArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
