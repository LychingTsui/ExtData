package PersonalRecommend;

import java.io.IOException;
import java.net.URI;
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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/*
 *@info：获取xxxx年xx月始至今用户的所有vv中movieId 纪录
 *  		此数据作为训练样本的正样本
 */
public class GetUidVvMovieId {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{
		protected void map(LongWritable key, Text val, Context context)throws IOException,
		InterruptedException{
			String vvInfo = val.toString();
			String id = "00:00:00:00:00:00";
			StringTokenizer stk = new StringTokenizer(vvInfo, "\t");
			String guid = stk.nextToken();
			if(!guid.equals(id)){     
				while (stk.hasMoreTokens()) {
					String subInfo = stk.nextToken();
					StringTokenizer substk = new StringTokenizer(subInfo, ",");
					String movieId = substk.nextToken();
					if(movieId.length() == 32){
						context.write(new Text(guid), new Text(movieId));
					}					
				}
			}
			
		}
	}
	public static class ReduceClass extends Reducer<Text, Text, Text, Text>{
		protected void reduce(Text key, Iterable<Text>vals, Context context)throws IOException,
		InterruptedException{
			HashSet<String> movIdSet = new HashSet<String>();
			String movStr = "";
			for(Text v : vals){
				movIdSet.add(v.toString());
			}
			for(String mvid : movIdSet){
				movStr += mvid + "\t";
			}
			context.write(key, new Text(movStr));
		}
	}
	public static void main(String[] args)throws IOException,InterruptedException,
	ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "30");
		Job job = Job.getInstance(conf, "collectVvitems");
		
		String[] argstrs = new GenericOptionsParser(conf, args).getRemainingArgs();
		FileSystem dirfs = FileSystem.get(URI.create(argstrs[0]), conf);
		FileStatus fstas = dirfs.getFileStatus(new Path(argstrs[0]));
		String date = argstrs[2];  // 可以设置收集起始日期（正月计算，未实现截止到日） 格式：201707 
		int start_y = Integer.parseInt(date.substring(0, 4));
		int start_m = Integer.parseInt(date.substring(4,6));
		if(fstas.isDirectory()){
			for(FileStatus subfstus : dirfs.listStatus(new Path(argstrs[0]))){
				String subdir = subfstus.getPath().toString();
				int idx = subdir.lastIndexOf("/");
				String lastStr = subdir.substring(idx+1);
				int monthArv = Integer.parseInt(lastStr.substring(4,6));
				int yearArv = Integer.parseInt(lastStr.substring(0,4));
				if(yearArv - start_y == 0){
					/* 在此可以设置某几个月份内的数据，定制几个月内的vvHistory  */					
					if(monthArv - start_m >= 0){
						FileSystem f = FileSystem.get(URI.create(subdir), conf);
						for(FileStatus fstu : f.listStatus(new Path(subdir))){
							MultipleInputs.addInputPath(job, fstu.getPath(), TextInputFormat.class, MapClass.class);				
						}
					}
				}
				if (yearArv - start_y > 0) {
					FileSystem f = FileSystem.get(URI.create(subdir), conf);
					for(FileStatus fstu : f.listStatus(new Path(subdir))){
						MultipleInputs.addInputPath(job, fstu.getPath(), TextInputFormat.class, MapClass.class);				
					}
				}
			}		
		}
		
		//MultipleInputs.addInputPath(job, new Path(argstrs[0]), TextInputFormat.class, MapClass.class);
		job.setJarByClass(GetUidVvMovieId.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(ReduceClass.class);
		
		FileOutputFormat.setOutputPath(job, new Path(argstrs[1]));
		System.exit(job.waitForCompletion(true)? 0:1 );
	}
	
}
