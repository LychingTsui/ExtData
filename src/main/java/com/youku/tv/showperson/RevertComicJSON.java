package com.youku.tv.showperson;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.youku.tv.json.JSONArray;
import com.youku.tv.json.JSONException;
import com.youku.tv.json.JSONObject;

public class RevertComicJSON {
	public static class MapClass extends Mapper<LongWritable, Text, Text, NullWritable> {
		Text outket = new Text();
		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String jsondata = value.toString();
			try {
				if (jsondata != null && jsondata.length() > 1) {
					JSONArray jArray = new JSONArray(jsondata);
					for (int i = 0; i < jArray.length(); i++) {
						StringBuffer buffer=new StringBuffer();
						JSONObject object=jArray.getJSONObject(i);
						if (object.has("status")) {
							if (object.get("status").equals("1")) {
								return;
							} 
						}
						if (object.has("id")) {
							System.out.println(object.get("id"));
							buffer.append("comic-"+object.get("id").toString()+"\t");
						}
						if (object.has("title")) {
							buffer.append(object.get("title").toString()+"\t");
						}
						if (object.has("ctg")) {
							System.out.println(object.get("ctg"));
							buffer.append(object.get("ctg").toString()+"\t");
						}
						if (object.has("finish")) {
							if (object.get("finish").toString().equals("")) {
								buffer.append("0"+"\t");
							}
							else{
								buffer.append(object.get("finish")+"\t");
							}
							System.out.println(object.get("finish")+"finish");
						}
						if (object.has("update")) {
							if (object.get("update").equals("")) {
								buffer.append("0"+"\t");
							}
							else{
								String number=getNumber(object.get("update").toString());
											buffer.append(number+"\t");
							}
						}
						if (object.has("episodes")) {
							Set<String> set=new HashSet<String>();
						    String ssd=object.get("episodes").toString();
						    if (ssd.length()>3) {
						    	 String st[]=ssd.split("\\[")[1].split("\\]")[0].split(",");
									buffer.append(st.length);
									System.out.println(st.length+"size");
							}  
						}
						String bt[]=buffer.toString().split("\t");
						if (bt.length>3) {
							System.out.println(bt.length+"bt");
							
						}		
						outket.set(buffer.toString());
						context.write(outket, NullWritable.get());
					}
				}
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
		public static String getNumber(String str){
			String regEx="[^0-9]";
			Pattern pattern=Pattern.compile(regEx);
			Matcher m=pattern.matcher(str);
			return m.replaceAll("").trim();
		}

		public static void writeFile(String filePathAndName, String fileContent) {
			try {
				File f = new File(filePathAndName);
				if (!f.exists()) {
					f.createNewFile();
				}
				OutputStreamWriter write = new OutputStreamWriter(new FileOutputStream(f, true),
						"UTF-8");
				BufferedWriter writer = new BufferedWriter(write);
				writer.write(fileContent);
				writer.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException {
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "tv movie:GetDataFromMovieSource");
		job.setJarByClass(RevertComicJSON.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
				MapClass.class);

		job.setNumReduceTasks(1);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
