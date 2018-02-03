package com.qiguo.tv.movie.card;

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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
/**
 * 
 **/
public class ConvertTagsId2CardTagFeatures {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{
		HashMap<String, String>cardid2FeatsMp = new HashMap<String, String>();
		protected void map(LongWritable key, Text val, Context context)throws IOException,
		InterruptedException{
			StringTokenizer stk = new StringTokenizer(val.toString(), "\t");
			String guid = stk.nextToken();
			String cardstr = stk.nextToken();
			int idx = cardstr.indexOf(":");
			String lab = cardstr.substring(0,idx);
			String cardid = cardstr.substring(idx+1);
			if(cardid2FeatsMp.containsKey(cardid)){
				context.write(new Text(guid), new Text(lab +":"+ cardid2FeatsMp.get(cardid)));
			}
		}
		
		public void setup(Context context)throws IOException,
		InterruptedException{
			Path[] cachepth = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for(Path pth: cachepth){
				loadData(context, pth.toString());
			}
			super.setup(context);
		}
		public void loadData(Context context, String path)throws IOException,
		InterruptedException{
			BufferedReader bfr = new BufferedReader(new FileReader(path));
			String line = "";
			while((line = bfr.readLine()) != null){
				StringTokenizer stk = new StringTokenizer(line, "\t");
				while(stk.hasMoreTokens()){
					String id = stk.nextToken();
					String feats = stk.nextToken();
					cardid2FeatsMp.put(id, feats);
				}
			}
			bfr.close();
		}
	}
	public static void main(String[] args)throws IOException,InterruptedException,
	ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies","25");
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "extCardPosNeg");
		job.setJarByClass(ConvertTagsId2CardTagFeatures.class);
		
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
