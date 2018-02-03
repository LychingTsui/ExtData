package com.qiguo.tv.movie.relatedRecXgModel;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
/** 
 * 与ExtSpecificMoviesStsFeatures 不同的是在多输入是直接采用日期转换为数值作为比较对象
 * 留取指定时间间隔内的数据, 另外有对人均点击率和人均观看时长的调和平均值特征
 **/

public class ExtSpecificMoviesStsFeaturesDate {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{
		HashSet<String>moviesSet = new HashSet<String>();
		private static IntWritable one = new IntWritable(1);
		protected void map(LongWritable key, Text val, Context context)throws IOException,
		InterruptedException {
			StringTokenizer stk = new StringTokenizer(val.toString(), "\t");
			stk.nextToken();
			HashMap<String, String> movieStsMp = new HashMap<String, String>();
			while(stk.hasMoreTokens()){
				String[] movieStr = stk.nextToken().split(",");
				if(moviesSet.contains(movieStr[0])){
					if(movieStsMp.containsKey(movieStr[0])){
						int clkTimes = Integer.parseInt(movieStsMp.get(movieStr[0]).split(":")[1]) + 1;
						int duration = Integer.parseInt(movieStsMp.get(movieStr[0]).split(":")[2]) + (int)(Long.parseLong(movieStr[1])/3600000l);
						String info = movieStsMp.get(movieStr[0]).split(":")[0] + ":" + clkTimes + ":" + duration;
						movieStsMp.put(movieStr[0], info);
					}else {
						int dura = (int)(Long.parseLong(movieStr[1]) / 3600000);
						movieStsMp.put(movieStr[0], one +":" + one + ":" + dura); // uv + clktimes + 时长
					}
				}
			}
			for(Map.Entry<String, String>entry : movieStsMp.entrySet()){
				context.write(new Text(entry.getKey()), new Text(entry.getValue()));
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
			BufferedReader bfr = new BufferedReader(new FileReader(path));
			String line = "";
			while((line = bfr.readLine()) != null){
				moviesSet.add(line.trim());
			}
			bfr.close();
		}
	}
	
	public static class ReduceClass extends Reducer<Text, Text, Text, Text>{
		protected void reduce(Text key, Iterable<Text> val, Context context)throws IOException,
		InterruptedException {
			int totUv = 0;
			int totPv = 0;
			int dura = 0;
			for(Text v : val){
				String[] arr = v.toString().split(":");
				totUv += Integer.parseInt(arr[0]);
				totPv += Integer.parseInt(arr[1]);
				dura += Integer.parseInt(arr[2]);
			}
			float timePsn = totPv / (1.0f*totUv);
			BigDecimal bgd = new BigDecimal(timePsn);
			timePsn = Float.parseFloat(bgd.setScale(2, BigDecimal.ROUND_HALF_UP).toString());
			float duraPsn = dura / (1.0f * totUv);
			BigDecimal bgd2 = new BigDecimal(duraPsn);
			duraPsn = Float.parseFloat(bgd2.setScale(2, BigDecimal.ROUND_HALF_UP).toString());
			float eval = 1/((1.0f / timePsn) + (1.0f / duraPsn));
			BigDecimal bgd3 = new BigDecimal(eval);
			eval = Float.parseFloat(bgd3.setScale(2, BigDecimal.ROUND_HALF_UP).toString());
			String out = timePsn + "\t" + duraPsn;// + "\t"+ eval; //totUv + "\t" + totPv + "\t" +dura + "\t" +
			context.write(key,  new Text(out));
		}
	}
	
	public static void main(String[] args)throws IOException, InterruptedException,
	ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		Job job = Job.getInstance(conf, "collectStsFeats");
		
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
		
		FileSystem dirfs = FileSystem.get(URI.create(argstrs[0]), conf);
		FileStatus fstas = dirfs.getFileStatus(new Path(argstrs[0]));
		//String format = "yyyyMMdd";  //  格式：20170731 
		int startDate = Integer.parseInt(argstrs[3]); 
		int endDate = Integer.parseInt(argstrs[4]);
		
        if(fstas.isDirectory()){
				for(FileStatus subfstus : dirfs.listStatus(new Path(argstrs[0]))){
					String subdir = subfstus.getPath().toString();
					int idx = subdir.lastIndexOf("/");
					String lastStr = subdir.substring(idx+1);
					//long someDayTime = sdf.parse(lastStr).getTime(); 
					int someday = Integer.parseInt(lastStr);
					boolean flag1 = someday - startDate >= 0 ? true : false;
					boolean flag2 = someday - endDate <= 0 ? true : false;
					if(flag1 && flag2){
						FileSystem f = FileSystem.get(URI.create(subdir), conf);
						boolean fb = f.exists(new Path(subdir));
						if(fb){
							for(FileStatus fstu : f.listStatus(new Path(subdir))){
								//System.out.println(fstu.getPath().toString());
								MultipleInputs.addInputPath(job, fstu.getPath(), TextInputFormat.class, MapClass.class);				
							}
						}
					}
				}		
			}
        
		//MultipleInputs.addInputPath(job, new Path(argstrs[0]), TextInputFormat.class, MapClass.class);
		job.setJarByClass(ExtSpecificMoviesStsFeaturesDate.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(ReduceClass.class);
		
		FileOutputFormat.setOutputPath(job, new Path(argstrs[1]));
		System.exit(job.waitForCompletion(true) ? 0:1 );
	}
}
