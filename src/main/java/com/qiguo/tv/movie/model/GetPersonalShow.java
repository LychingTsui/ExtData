package com.qiguo.tv.movie.model;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
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
 * @param input: 某天的show数据
 * 		  cache：有vv数据的user的guid
 * @info: 保留某天有vv纪录的user的show数据，去除无vv纪录的user的show数据
 */
public class GetPersonalShow {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{
		HashSet<String> vvguidSet = new HashSet<String>();
		protected void map(LongWritable key, Text val, Context context) throws IOException,
		InterruptedException{
			StringTokenizer stk = new StringTokenizer(val.toString(), "\t");
			String guid = stk.nextToken();
			StringBuffer strbuf = new StringBuffer();
			
			if(vvguidSet.contains(guid)){
				while(stk.hasMoreTokens()){
					String mv = stk.nextToken();
					if(mv.length() == 32){
						strbuf.append(mv+" ");
					}
				}
				String outStr = strbuf.toString();
				context.write(new Text(guid), new Text(outStr));
			}
			
		}
		public void setup(Context context)throws IOException, InterruptedException{
			Path[] filePaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for(Path path : filePaths){
				loadData(path.toString(), context);
			}
			
			super.setup(context);	
		}
		public void loadData(String path, Context context)throws IOException, InterruptedException{
			String line = "";
			BufferedReader bfr = new  BufferedReader(new FileReader(path));
			while((line = bfr.readLine()) != null){
				line = line.trim();
				vvguidSet.add(line);
			}
			bfr.close();
		}
	}
	public static void main(String[] args)throws IOException, InterruptedException,
	ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies","25");
		String[] othArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		Job job = Job.getInstance(conf, "guid");
		job.setJarByClass(GetPersonalShow.class);
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
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(othArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
