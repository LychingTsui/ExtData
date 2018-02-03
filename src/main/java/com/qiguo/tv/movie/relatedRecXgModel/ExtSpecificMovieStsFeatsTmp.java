package com.qiguo.tv.movie.relatedRecXgModel;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
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
 * 提取电影的统计特征
 **/
public class ExtSpecificMovieStsFeatsTmp {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{
		HashSet<String>moviesSet = new HashSet<String>();  // 有过观看记录的电影集合
		private static IntWritable one = new IntWritable(1);
		protected void map(LongWritable key, Text val, Context context)throws IOException,
		InterruptedException {
			StringTokenizer stk = new StringTokenizer(val.toString(), "\t");
			stk.nextToken();  //guid
			HashMap<String, String> movieStsMp = new HashMap<String, String>();
			while(stk.hasMoreTokens()){
				String[] movieStr = stk.nextToken().split(",");
				if(moviesSet.contains(movieStr[0])){
					boolean f = movieStr.length > 3 && !movieStr[2].startsWith("-");
					if(movieStsMp.containsKey(movieStr[0])){
						movieStr[2] = f == true ? movieStr[2] : "0";
						double duration = Long.parseLong(movieStr[2])/3600000.0d > 2.5d ? 1.0d :Long.parseLong(movieStr[2])/3600000.0d; 
						duration = Double.parseDouble(movieStsMp.get(movieStr[0]).split(":")[2]) + duration;
						BigDecimal bgD = new BigDecimal(duration);
						duration = Double.parseDouble(bgD.setScale(2, BigDecimal.ROUND_HALF_UP).toString());
						int clkTimes = Integer.parseInt(movieStsMp.get(movieStr[0]).split(":")[1]) + 1;
						String info = movieStsMp.get(movieStr[0]).split(":")[0] + ":" + clkTimes + ":" + duration;
						movieStsMp.put(movieStr[0], info);
					
					}else {
						double dura = f == true ? Long.parseLong(movieStr[2]) / 3600000.0 : 0.0d;
						dura = dura > 2.5d ? 1.0d : dura;
						BigDecimal bgd = new BigDecimal(dura);
						dura = Double.parseDouble(bgd.setScale(2, BigDecimal.ROUND_HALF_UP).toString());
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
			float dura = 0.0f;
			for(Text v : val){
				String[] arr = v.toString().split(":");
				totUv += Integer.parseInt(arr[0]);
				totPv += Integer.parseInt(arr[1]);
				dura += Float.parseFloat(arr[2]);
			}
			BigDecimal bgdura = new BigDecimal(dura);
			dura = Float.parseFloat(bgdura.setScale(2, BigDecimal.ROUND_HALF_UP).toString());
			float timePsn = 0.0f;
			float duraPsn = 0.0f;
			if(totUv > 1){
				timePsn = totPv / (1.0f * totUv);
				BigDecimal bgd = new BigDecimal(timePsn);
				timePsn = Float.parseFloat(bgd.setScale(2, BigDecimal.ROUND_HALF_UP).toString());
				duraPsn = dura / (1.0f * totUv);
				BigDecimal bgd2 = new BigDecimal(duraPsn);
				duraPsn = Float.parseFloat(bgd2.setScale(2, BigDecimal.ROUND_HALF_UP).toString());
			
				//非线性缩减
				
				BigDecimal bgdscale = new BigDecimal(Math.log(timePsn + 1) / Math.log(2));
				timePsn = Float.parseFloat(bgdscale.setScale(0, BigDecimal.ROUND_HALF_UP).toString());
				
				BigDecimal bgdduPsn = new BigDecimal(Math.log(duraPsn + 2) / Math.log(2));
				duraPsn = Float.parseFloat(bgdduPsn.setScale(0, BigDecimal.ROUND_HALF_UP).toString());
				
				totUv = (int)Math.log10(totUv) + 1;
				totPv = (int)Math.log10(totPv) + 1;
				dura = (int)(Math.log10(dura+10));
				 
				String out = timePsn + "\t" + duraPsn + "\t" + totUv + "\t" + totPv + "\t" + dura;
				context.write(key,  new Text(out));
			}
		}
	}
	
	public static void main(String[] args)throws IOException, InterruptedException,
	ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		Job job = Job.getInstance(conf, "collectSts");
		
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
		
		//FileSystem dirfs = FileSystem.get(URI.create(argstrs[0]), conf);
		//FileStatus fstas = dirfs.getFileStatus(new Path(argstrs[0]));
		//String format = "yyyyMMdd";  //  格式：20170731 
		//String startDate = argstrs[3];  
		//String endDate = argstrs[4];
		/*
		try{
            SimpleDateFormat sdf = new SimpleDateFormat(format);
            long startTime = sdf.parse(startDate).getTime();
            long endTime = sdf.parse(endDate).getTime();
            
            if(fstas.isDirectory()){
    				for(FileStatus subfstus : dirfs.listStatus(new Path(argstrs[0]))){
    					String subdir = subfstus.getPath().toString();
    					int idx = subdir.lastIndexOf("/");
    					String lastStr = subdir.substring(idx+1);
    					long someDayTime = sdf.parse(lastStr).getTime(); 
    					
    					if(someDayTime >= startTime && someDayTime <= endTime){
							//System.out.println(lastStr +"\t"+ subdir);
							FileSystem f = FileSystem.get(URI.create(subdir), conf);
    						for(FileStatus fstu : f.listStatus(new Path(subdir))){
    							//System.out.println(fstu.getPath().toString());
    							MultipleInputs.addInputPath(job, fstu.getPath(), TextInputFormat.class, MapClass.class);				
    						}
    					}
    				}		
    			}
        }catch (Exception e){
            e.printStackTrace();
        }
		*/
		MultipleInputs.addInputPath(job, new Path(argstrs[0]), TextInputFormat.class, MapClass.class);
		job.setJarByClass(ExtSpecificMovieStsFeatsTmp.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(ReduceClass.class);
		
		FileOutputFormat.setOutputPath(job, new Path(argstrs[1]));
		System.exit(job.waitForCompletion(true) ? 0:1 );
	}
}
