package com.qiguo.tv.movie.featCollection;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.youku.tv.json.JSONArray;
import com.youku.tv.json.JSONException;
import com.youku.tv.json.JSONObject;
/*
 * @info：对负样本进行采样，采样原则：user当天观看的最后位置的电影前的show纪录数据保留，作为负样本，其余舍去
 */
public class GetgGuidItemIdSample2 {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{
		HashMap<String, HashMap<String, Integer>> uidMaxlocMp = new HashMap<String, HashMap<String, Integer>>();
		HashMap<String, HashSet<String>> uidVvMap = new HashMap<String, HashSet<String>>();
		protected void map(LongWritable key, Text val, Context context) throws IOException,
		InterruptedException{
			
			String[] info = val.toString().split("\t", -1);
			if(info.length < 27)
				return;			
			if(info[25].endsWith("listDisplay")){				
				if(isJson(info[27])){
					JSONObject js = null;
					try {
						js = new JSONObject(info[27]);					
						String guid = js.getString("guid");
						if(!uidVvMap.containsKey(guid)){
							return;
						}
						int pn = 0;	
						String ctg ="";
						if(js.has("category")){
							ctg = js.getString("category");
						}else if(js.has("ctg")){
							ctg = js.getString("ctg");
						}
						if(uidMaxlocMp.get(guid).containsKey(ctg)){
							pn = js.getInt("pn");
							int maxPn = uidMaxlocMp.get(guid).get(ctg)/10 + 1;
							if(pn <= maxPn){
								if(pn < maxPn){
									JSONArray jsarr = getArray(js.getString("list"));
									
									for(int i=0; i< 10; i++){
										String mvid = jsarr.getJSONObject(i).getString("id");
										if(!uidVvMap.get(guid).contains(mvid)){
											
											String negmv = "0:"+mvid;
											context.write(new Text(guid), new Text(negmv));		
										}//else {
										//	String vvid = "1:"+mvid;
										//	context.write(new Text(guid), new Text(vvid));
										//}					
									}
								}else if(pn == maxPn) {
									int pos = uidMaxlocMp.get(guid).get(ctg) % 10;
									JSONArray jsarr = getArray(js.getString("list"));
									for(int i=0; i< pos; i++){
										String mvid = jsarr.getJSONObject(i).getString("id");
										if(!uidVvMap.get(guid).contains(mvid)){
											
											String negmv = "0:"+mvid;
											context.write(new Text(guid), new Text(negmv));					
										}//else {
										//	String vvid = "1:"+mvid;
										//	context.write(new Text(guid), new Text(vvid));
										//}
									}
								}
								for(String id : uidVvMap.get(guid)){
									String vvid = "1:"+id;
									context.write(new Text(guid), new Text(vvid));
								}
							}
						}
					} catch (JSONException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}	
				}						
			}
			
		}
		public JSONArray getArray(String str) throws JSONException{
			
			JSONArray jsa = new JSONArray(str);
			return jsa;
		}
		public boolean isJson(String str){	
			if(str.startsWith("{")){
				try {
					JSONObject jsonObject = new JSONObject(str);
					if(jsonObject.has("ctg")){
						String ctg = jsonObject.get("ctg").toString();
						if(ctg.startsWith("movie")){
							return true;
						}
					}
					else if(jsonObject.has("category")){
						String ctg = jsonObject.get("category").toString();
						if(ctg.startsWith("movie")){
							return true;
						}
					}
				} catch (com.youku.tv.json.JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}				
			}
			else {
				String strTmp = "{" + str + "}";
				try {
					JSONObject jsonObject = new JSONObject(strTmp);
					if(jsonObject.has("ctg")){
						String ctg = jsonObject.get("ctg").toString();
						if(ctg.startsWith("movie")){
							return true;
						}
					}
					else if(jsonObject.has("category")){
						String ctg = jsonObject.get("category").toString();
						if(ctg.startsWith("movie")){
							return true;
						}
					}
				} catch (com.youku.tv.json.JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}	
			}
			return false;
		}
		public void setup(Context context)throws IOException,InterruptedException{
			Path[] filePaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for(Path path : filePaths){
				loadData(path.toString(), context);
			}
			
			super.setup(context);
		}
		public void loadData(String path, Context context) throws IOException,
		InterruptedException{
			FileReader fr = new FileReader(path);
			BufferedReader bs = new BufferedReader(fr);			
			String line = null;
			
			while((line = bs.readLine()) != null){
				StringTokenizer stk = new StringTokenizer(line, "\t");
				String[] uidctg = stk.nextToken().split(" ");
				if(uidctg.length < 2){
					return;
				}
				String uid = uidctg[0];
				String ctg = uidctg[1];
				String mvStr = stk.nextToken();
				int idx = mvStr.indexOf(" ");
				int maxloc = Integer.parseInt(mvStr.substring(0,idx));
				HashMap<String, Integer>ctgMaxPnMp = new HashMap<String, Integer>();
				ctgMaxPnMp.put(ctg, maxloc);
				uidMaxlocMp.put(uid, ctgMaxPnMp);				
				HashSet<String> mvset = new HashSet<String>();
				StringTokenizer substk = new StringTokenizer(mvStr.substring(idx+1));
				while(substk.hasMoreTokens()){
					String mvid = substk.nextToken();
					mvset.add(mvid);
				}	
				uidVvMap.put(uid, mvset);
			}		
			bs.close();
		}
	}
	public static class ReduceClass extends Reducer<Text, Text, Text, Text>{
		protected void reduce(Text key, Iterable<Text> val, Context context)throws IOException,
		InterruptedException{
			HashSet<String> vvSet = new HashSet<String>();
			for(Text vvStr : val){
				vvSet.add(vvStr.toString());
			}
			for(String vv : vvSet){
				context.write(key, new Text(vv));
			}
		}
	}
	public static void main(String[] args)throws IOException,
	InterruptedException,ClassNotFoundException{
		Configuration conf = new Configuration();
		String otherArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("mapred.reduce.parallel.copies", "25");
		Job job = Job.getInstance(conf, "GetvvSample");
		job.setJarByClass(GetgGuidItemIdSample2.class);
		Path cachePath = new Path(otherArgs[2]);
		 FileSystem fs = FileSystem.get(conf);
		 FileStatus[] arr = fs.listStatus(cachePath);
		 for (FileStatus fstatus : arr) {
			 Path p = fstatus.getPath();
			 if (fs.isFile(p)) {
				 job.addCacheFile(p.toUri());
			 }
		 }
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, MapClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0:1);
	}
}
