package com.qiguo.tv.movie.featCollection;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
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

import com.youku.tv.json.JSONException;
import com.youku.tv.json.JSONObject;

public class GetGuidItemId01Sample {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{
		protected void map(LongWritable key, Text val, Context context)throws IOException,
		InterruptedException{
			String info[] = val.toString().split("\t", -1);
			if(info.length < 27)
				return;
			if(info[25].endsWith("listClick")){
				
				if(isJson(info[27])){
					JSONObject js = null;
					try {
						js = new JSONObject(info[27]);
						int pn = 0;
						int pos = -1;
						if(js.has("pn")){
							pn = js.getInt("pn");
						}
						if(js.has("pos")){
							pos = js.getInt("pos");
						}
						
						int mvloc = (pn-1)*10 + pos;
						if(js.has("videoInfo")){
							String videoInfo = js.get("videoInfo").toString();
							String guid = info[15];//js.getString("guid");
							String catg = "";
							if(js.has("category")){
								catg = js.getString("category");
							}else if(js.has("ctg")){
								catg = js.getString("ctg");
							}
							
							JSONObject subJs = new JSONObject(videoInfo);
							if(subJs.has("id")){
								String mvid = subJs.getString("id");
								if(pn != 0 && pos != -1 ){
									String text = mvloc +" "+ mvid;
									String keyval = guid +" "+catg;
									context.write(new Text(keyval), new Text(text));
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
		
	}
	
	public static class ReduceClass extends Reducer<Text, Text, Text, Text>{
		protected void reduce(Text key, Iterable<Text>vals, Context context)throws IOException,
		InterruptedException{
			int maxloc = 0;
			HashSet<String> mvSet = new HashSet<String>();
			for(Text v : vals){
				String mvStr = v.toString();
				int idx = mvStr.indexOf(" ");
				String mvid = mvStr.substring(idx+1);
				if(mvid.length() == 32){
					mvSet.add(mvid);
					maxloc = Integer.parseInt(mvStr.substring(0,idx));
					//int curPn = loc/10 + 1;
					//maxPn = curPn > maxPn ? curPn : maxPn;
				}				
			}
			if(maxloc > 0){
				String res = maxloc +" ";
				for(String s: mvSet){
					res += s+" "; 
				}
				context.write(key, new Text(res));
			}			
		}
	}

	public static void main(String[] args)throws IOException,
	InterruptedException, ClassNotFoundException{
		Configuration conf = new Configuration();
		String otherArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("mapred.reduce.parallel.copies", "25");
		Job job = Job.getInstance(conf, "Getvv");
		job.setJarByClass(GetGuidItemId01Sample.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, MapClass.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(ReduceClass.class);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0:1);
	}
	
}
