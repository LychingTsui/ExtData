package com.qiguo.tv.movie.featuresCollection;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

//import javax.security.auth.login.Configuration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import com.youku.tv.json.JSONException;
import com.youku.tv.json.JSONObject;
// features : uid,movieId, 观长／总长，上映距观看今时长，movie打分，   
public class CollectTrainDataFeatures {
	public static class FeatureMap extends Mapper<LongWritable, Text, Text, NullWritable>{
		Text outKey = new Text();
		Map<String, String> uidMap = new HashMap<String, String>();
		HashMap<String, JSONObject> movieDict= new HashMap<String, JSONObject>();
		protected void map(LongWritable key, Text val, Context context) throws IOException,
		InterruptedException{
			String[] info = val.toString().split("\t",-1);
			int movieNum = info.length-1;   // info[0] 为uid
			String uid = info[0];
			
			StringBuffer strBuf = new StringBuffer();		
			strBuf.append(uidMap.get(uid) +"\t");
			String typePath = "/user/tvapk/run_sh/cuiliqing/featureData/type.txt";		
			String actorPath = "/user/tvapk/run_sh/cuiliqing/featureData/actor1.txt";	
			String direcPath = "/user/tvapk/run_sh/cuiliqing/featureData/director.txt";
			String areaPath = "/user/tvapk/run_sh/cuiliqing/featureData/area.txt";  //路径
	
			for(int idx = 1; idx <= movieNum; idx++){
				String[] movieinfo = info[idx].split(","); 
				String movieId = movieinfo[0];
				double rat = getWatchRate(movieId, movieinfo[1], movieDict);
				strBuf.append(rat +"\t");
				//int interal = getInteral(movieId, movieinfo[2], movieDict);
				//strBuf.append(interal+"\t");
				//int time = getTimes(movieinfo[2]);  //观看次数
				//strBuf.append(time + "\t");
				int score = getScore(movieId, movieDict);
				strBuf.append(score +"\t");
				
				String ftType = "type";
				int[] typeCod = getfetOneHotCode(movieId, ftType, typePath, movieDict);
				strBuf.append(typeCod + "\t");
				
				String ftact = "actor";		
				int[] actCod = getfetOneHotCode(movieId, ftact, actorPath, movieDict);
				strBuf.append(actCod + "\t");
				
				String director = "diretor";			
				int[] directCod = getfetOneHotCode(movieId, director, direcPath, movieDict);
				strBuf.append(directCod + "\t");
				
				String are = "area";		
				int[] areaCod = getfetOneHotCode(movieId, are, areaPath, movieDict);
				strBuf.append(areaCod + "\t");
				
				outKey.set(strBuf.toString());
				context.write(outKey, NullWritable.get());
			}			
		}
		public double getWatchRate(String movieId,String watTime, Map<String, JSONObject>movieDict){
			Double rat = 0.0;
			if(movieDict.containsKey(movieId)){
				try{				
					String dur = movieDict.get(movieId).get("duration").toString();
					if(isNumeric(dur)){  //
						if(isNumeric(watTime)){
							rat = Double.valueOf(watTime)/(1.0+Double.valueOf(dur));
							if(rat>1.0){
								rat = 1.0;
							}
						}
						
					}else{
						rat = 0.0;       // 异常值处理
					}
				}catch (JSONException e) {
					e.printStackTrace();
					// TODO: handle exception
				}
			}
			return rat;
		}
		public int getInteral(String movieId, String movieInfo, Map<String,JSONObject>movieDict){
			int interal = -1;
			if (movieInfo.contains(":")){
				String[] dataAndTime = movieInfo.split(":"); 
				String onDate = "";
				
				try {
					onDate = movieDict.get(movieId).get("date").toString();
					if(isNumeric(onDate)){
						interal = Integer.valueOf(dataAndTime[0])-Integer.valueOf(onDate);
					}
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}	
			}
			return interal;
		}
		public int getTimes(String movieinfo){
			int time = 1;
			if(movieinfo.contains(":")){
				String[] t = movieinfo.split(":");
				if(isNumeric(t[1])){
					time = Integer.valueOf(t[1]);
				}
			}
			return time;
		}
		public int getScore(String movieId, Map<String,JSONObject>movieDict){
			int score = 50;
			try{
				String sc = movieDict.get(movieId).getString("rate").toString();
				if(isNumeric(sc)){
					score = Integer.valueOf(sc);
				}
			}catch (JSONException e) {
				e.printStackTrace();
				// TODO: handle exception
			}
			return score;
		}
		public int[] getfetOneHotCode(String movieId,String pro,String typePath,Map<String,JSONObject>movieDict)
		throws IOException{
			int[]onHcod = null;
			try {
				String val = movieDict.get(movieId).getString(pro);
		
				String featureSet = new FeatureFileReader(typePath).readFeatureFile();
				onHcod = new OneHot(featureSet).oneHotCode(val);
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return onHcod;
		}

		public boolean isNumeric(String str){ 
			   Pattern pattern = Pattern.compile("^[0-9]+"); 
			   Matcher isNum = pattern.matcher(str);
			   if( !isNum.matches() ){
			       return false; 
			   } 
			   return true; 
			}
		public void setup(Context context)throws IOException,
		InterruptedException{
			Path[] filePaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for(Path path : filePaths){
				loadData(path.toString(), context);
			}
			
			this.setup(context); 	
		}
		public void loadData(String file, Context context) throws IOException,
		InterruptedException{
			FileReader fr = new FileReader(file);
			BufferedReader bs = new BufferedReader(fr);
			
			String line = null;
			while((line = bs.readLine()) != null){
				//String[] infoStrings = line.toString().split("\t",-1);
				String[] infoStrings = StringUtils.splitPreserveAllTokens(line, "\t");
				String key = infoStrings[0];   // 
				if(key.contains(":")){
					int i = 0;
					   // movieId
					String uidStr = "u" + i++;
					uidMap.put(key, uidStr);
				}else{
					StringBuffer strBuffer = new StringBuffer();
					strBuffer.append("{");
					for(int i = 1; i < infoStrings.length; i++){
						 if(!infoStrings[i].startsWith("intro")){
							 strBuffer.append(infoStrings[i]+",");
						 }
					}
					strBuffer.append("}");
					try {
						movieDict.put(key, new JSONObject(strBuffer.toString()));
					} catch (JSONException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
			}
			bs.close();
		} 	
		
	}

	public static void main(String[] args) throws IOException,
	 InterruptedException,ClassNotFoundException{
		 Configuration conf = new Configuration();
		 conf.set("mapred.reduce.parallel.copies","25"); // hadoop 有一个600s的时间限制，超时未完成的mr会被终结 
	
		 String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		 Job job = Job.getInstance(conf, "tv movie:collectFeatures");
		 job.setJarByClass(CollectTrainDataFeatures.class);

		 Path cachePath = new Path(otherArgs[2]);
		 FileSystem fs = FileSystem.get(conf);
		 FileStatus[] arr = fs.listStatus(cachePath);
		 for (FileStatus fstatus : arr) {
			 Path p = fstatus.getPath();
			 if (fs.isFile(p)) {
				 job.addCacheFile(p.toUri());
			 }
		 }
		
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
					FeatureMap.class);

		//job.setNumReduceTasks(1);
		//job.setReducerClass(cls);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	 
	 }
	 
}
