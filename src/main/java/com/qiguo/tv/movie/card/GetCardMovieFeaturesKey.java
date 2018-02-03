package com.qiguo.tv.movie.card;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class GetCardMovieFeaturesKey {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{
		HashMap<String, String> movieDicts = new HashMap<String, String>();
		protected void map(LongWritable key, Text val, Context context)throws IOException,
		InterruptedException{
			StringTokenizer stk = new StringTokenizer(val.toString(), ",\t");
			String cardId = stk.nextToken();
			int idx = cardId.indexOf(":");
			cardId = cardId.substring(0,idx);
			HashMap<String, Integer>tagsMp = new HashMap<String, Integer>();
			while (stk.hasMoreTokens()) {
				String mvidStr = stk.nextToken();
				int subIdx = mvidStr.indexOf(":");
				mvidStr = mvidStr.substring(0, subIdx);
				if(movieDicts.containsKey(mvidStr)){
					String tagStr = movieDicts.get(mvidStr);
					String[] tagsArr = tagStr.split(",");
					for(String tag : tagsArr){
						tag = tag.trim();
						if(!isNumeric(tag)){
							if(tagsMp.containsKey(tag)){
								tagsMp.put(tag, tagsMp.get(tag)+1);
							}else {
								tagsMp.put(tag, 1);
							}
						}
					}
				}
			}
			for(Map.Entry<String, Integer>entry : tagsMp.entrySet()){
				String out = entry.getKey()+" "+ entry.getValue();
				context.write(new Text(cardId), new Text(out));
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
		public void setup(Context context)throws IOException,InterruptedException{
			Path[] cachePath = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for(Path path : cachePath){
				loadData(context, path.toString());
			}
			super.setup(context);
		}
		
		public void loadData(Context context, String path)throws IOException,
		InterruptedException{
			BufferedReader bfr = new BufferedReader(new FileReader(path));
			String line = "";
			while((line = bfr.readLine()) != null){
				StringTokenizer stk = new StringTokenizer(line, "\t");
				String mvid = stk.nextToken();
				while(stk.hasMoreTokens()){
					String tmp = stk.nextToken();
					if(tmp.startsWith("tags") && tmp.length() > 5){
						String tagsStr = tmp.substring(5);
						movieDicts.put(mvid, tagsStr);
					}
				}
			}
			bfr.close();
		}
	}
	/*
	public static class ReduceClass extends Reducer<Text, Text, Text, Text>{
		protected void reduce(Text key, Iterable<Text>vals, Context context)throws IOException,
		InterruptedException{
			String out = "";
			for(Text  v : vals){
				out += v.toString() + "\t";
			}
			context.write(key, new Text(out));
		}
	}
	*/
	public static void main(String[] args)throws IOException,
	InterruptedException, ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies","25");
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		Job job = Job.getInstance(conf, "cardtagsSts");
		job.setJarByClass(GetCardMovieFeaturesKey.class);
		
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
