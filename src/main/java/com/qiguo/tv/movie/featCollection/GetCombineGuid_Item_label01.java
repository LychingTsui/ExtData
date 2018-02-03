package com.qiguo.tv.movie.featCollection;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

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
 *  step 2 combine item-feature into guid-0/1:[id:val, id:val....]
 *  @author Tsui
 */

public class GetCombineGuid_Item_label01 {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{
		HashMap<String, String> guidItemFeatMap = new HashMap<String, String>();
		
		protected void map(LongWritable key, Text val, Context context)throws IOException,
		InterruptedException{
			String[] strs = val.toString().split("\t",-1);
			if(strs.length == 2){
				String guid = strs[0];
				String[] movLabel = strs[1].split(":");
				if(guidItemFeatMap.containsKey(movLabel[1])){
					String tmp = movLabel[0]+":"+ guidItemFeatMap.get(movLabel[1]);
					context.write(new Text(guid), new Text(tmp));
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
		// 读取movieItem－features表数据
		public void loadData(String path, Context context) throws IOException,
		InterruptedException{
			FileReader fr = new FileReader(path);
			BufferedReader bs = new BufferedReader(fr);			
			String line = null;
		
			while((line = bs.readLine()) != null){
				String[] movieFeat = line.split("\t",-1);
				if(movieFeat.length == 2){
					guidItemFeatMap.put(movieFeat[0], movieFeat[1]);
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
		job.setJarByClass(GetCombineGuid_Item_label01.class);

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
