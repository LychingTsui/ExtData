package com.qiguo.tv.movie.relatedRecXgModel;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class ExtRelatedMovieShowRatePer {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{
		private static final IntWritable one = new IntWritable(1);
		HashSet<String> mvSet = new HashSet<String>();
		protected void map(LongWritable key, Text val, Context context)throws IOException,
		InterruptedException{
			StringTokenizer stk = new StringTokenizer(val.toString(), "\t");
			stk.nextToken();
			//HashMap<String, String>specificMvMp = new HashMap<String, String>();
			while (stk.hasMoreTokens()) {
				String mvid = stk.nextToken();
				String time = stk.nextToken();
				int idx = time.indexOf(":");
				if(mvSet.contains(mvid)){
					String out = one + "\t" + idx;
					context.write(new Text(mvid), new Text(out));
				}
			}
		}
		
		public void setup(Context context)throws IOException,
		InterruptedException{
			Path[] cachepath = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for(Path pth:cachepath){
				upLoad(pth.toString(), context);
			}
		}
		public void upLoad(String path, Context context)throws IOException,
		InterruptedException{
			BufferedReader bfr = new BufferedReader(new FileReader(path));
			String line = "";
			while((line = bfr.readLine()) != null){
				mvSet.add(line.trim());
			}
			bfr.close();
		}
	}
	
	public static class ReduceClass extends Reducer<Text, Text, Text, Text>{
		protected void reduce(Text key, Iterable<Text> vals, Context context)throws IOException, 
		InterruptedException {
			int disUv = 0;
			int disPv = 0;
			for(Text v : vals){
				disUv += Integer.parseInt(v.toString().split("\t")[0]);
				disPv += Integer.parseInt(v.toString().split("\t")[1]);
			}
			double res = disPv / (disUv*1.0);
			BigDecimal bgd = new BigDecimal(res);
			res = Double.parseDouble(bgd.setScale(2, BigDecimal.ROUND_HALF_UP).toString());
			context.write(key, new Text(res+" "));
		}
	}
	
	public static void main(String[] args)throws IOException, InterruptedException,
	ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		Job job = Job.getInstance(conf, "collectDisStsFeats");
		
		String[] argstrs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Path cachePath = new Path(argstrs[2]);
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] arr = fs.listStatus(cachePath);
		for (FileStatus fstatus : arr) {
			Path p = fstatus.getPath();
			if (fs.isFile(p)) {
				job.addCacheFile(p.toUri());
			}
		}
		job.setJarByClass(ExtRelatedMovieShowRatePer.class);
		MultipleInputs.addInputPath(job, new Path(argstrs[0]), TextInputFormat.class,
				MapClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(argstrs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
