package com.qiguo.tv.movie.featCollection;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

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

/***
 * step 1  guid-0/1:movieid
 * @author Tsui
 */
public class GetGuidItemLabel01 {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{
		HashMap<String, Set<String>> GuidVvSets = new HashMap<String, Set<String>>();
		protected void map(LongWritable key, Text val, Context context)throws IOException,
		InterruptedException{
			String[] showStr = val.toString().split("\t",-1);
			String guid = showStr[0];
			if(showStr.length > 1){
				String exgd = "00:00:00:00:00:00";
				if(!guid.equals(exgd)){
					if(GuidVvSets.containsKey(guid)){
						for(String item : GuidVvSets.get(guid)){
							context.write(new Text(guid), new Text("1:"+item));
						}
					}
					for(String movieId: showStr){
						if(movieId.length() == 32){
							if(GuidVvSets.containsKey(guid)){
								if(!GuidVvSets.get(guid).contains(movieId)){
									context.write(new Text(guid), new Text("0:"+movieId));
								}
							}						
							
						}
					}
				}		
			}
		}
		public void setup(Context context)throws IOException,
		InterruptedException{
			Path[] filePaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for(Path path : filePaths){
				loadData(path.toString(), context);
			}
			
			super.setup(context);	
		}
		//  读取vv数据
		public void loadData(String path, Context context) throws IOException,InterruptedException{
			FileReader fr = new FileReader(path);
			BufferedReader bs = new BufferedReader(fr);			
			String line = null;
			while((line = bs.readLine()) != null){
				Set<String> vvSets = new HashSet<String>();
				String[] str = line.split("\t",-1);
				String guid  = str[0];
				if(str.length > 1){
					for(String s : str){
						String[] mvitem = s.split(",");
						if(mvitem[0].trim().length() == 32){
							vvSets.add(mvitem[0]);
						}					
					}
					GuidVvSets.put(guid, vvSets);
				}			
			}
			bs.close();
		}
	}
	public static void main(String[] args)throws IOException,
	InterruptedException,ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies","25");
		String[] othArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		Job job = Job.getInstance(conf, "movieItemFeature");
		job.setJarByClass(GetGuidItemLabel01.class);

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
