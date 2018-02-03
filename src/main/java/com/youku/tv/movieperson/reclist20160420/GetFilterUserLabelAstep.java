package com.youku.tv.movieperson.reclist20160420;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class GetFilterUserLabelAstep {
	/**过滤用户不合格的标签、导演、演员等依赖标签标注*/
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{
		Text outkey = new Text(),outvalue = new Text();
		Map<String, String> filterTag = new HashMap<String, String>();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String info[] = value.toString().split("\t");
			if (info[1].substring(6).length() > 1 && info[2].substring(5).length() >1) {
				int i =0;
				ArrayList<String> list = new ArrayList<String>();
				String person[] = info[1].substring(6).split(",");
				StringBuffer buffer = new StringBuffer();
				for (String string : person) {
					String temp[] = string.split(":");
					if (temp.length<2) {
						continue;
					}
					if (filterTag.containsKey(temp[0])) {
						String filter = filterTag.get(temp[0]).split(":")[0];
						if (!list.contains(filter) ) {
							i++;
							buffer.append(",").append(filter).append(":").append(temp[1]);
							list.add(filter);
						}
					}
					else {
						if (!list.contains(temp[0])) {
							i++;
							buffer.append(",").append(string);	
							list.add(temp[0]);
						}
					}
				}
				String label[] = info[2].substring(5).split(",");
				StringBuffer bufferLabel = new StringBuffer();
				for (String string : label) {
					String temp[] = string.split(":");
					if (temp.length <2) {
						continue;
					}
					if (filterTag.containsKey(temp[0])) {
						String filters[] = filterTag.get(temp[0]).split(":");
						if (list.contains(filters[0])) {
							continue;
						}
						else {
							if ((filters[1].equals("导演") || filters[1].equals("演员")) && i < 5) {
								list.add(filters[0]);
								bufferLabel.append(",").append(filters[0]).append(":").append(temp[1]);
								i++;
							}
							else {
								list.add(filters[0]);
								bufferLabel.append(",").append(filters[0]).append(":").append(temp[1]);
							}
						}
					}
					else {
						if (!list.contains(temp[0])) {
							list.add(temp[0]);
							bufferLabel.append(",").append(string);
						}	
					}
				}
				outkey.set(info[0]);
				if (buffer.length() >1 && bufferLabel.length() >1) {
					outvalue.set("person" + buffer.substring(1) + "\t" + "label" + bufferLabel.substring(1));
					context.write(outkey, outvalue);
				}
				else if (buffer.length() >1 && bufferLabel.length() <=1) {
					outvalue.set("person" + buffer.substring(1) + "\t" + "label");
					context.write(outkey, outvalue);
				}
				else if (buffer.length() <=1 && bufferLabel.length() >1) {
					outvalue.set("person" + "\t" +"label" + bufferLabel.substring(1));
					context.write(outkey, outvalue);
				}
			}
			if (info[1].substring(6).length() > 1 && info[2].substring(5).length() <= 1) {
				int i = 0;
				ArrayList<String> list = new ArrayList<String>();
				String label[] = info[1].substring(6).split(",");
				StringBuffer buffer = new StringBuffer();
				for (String string : label) {
					String temp[] = string.split(":");
					if (temp.length<2) {
						return;
					}
					if (filterTag.containsKey(temp[0])) {
						String filters[] = filterTag.get(temp[0]).split(":");
						if (!list.contains(filters[0]) && i < 5) {
							buffer.append(",").append(filters[0]).append(":").append(temp[1]);
							list.add(filters[0]);
							i++;
						}
					}
					else {
						if (!list.contains(temp[0]) && i < 5) {
							buffer.append(",").append(string);
							list.add(temp[0]);
							i++;
						}
					}
				}
				outkey.set(info[0]);
				if (buffer.length()>1) {
					outvalue.set("person"+buffer.substring(1)+"\t"+"label");
					context.write(outkey, outvalue);
				}

			}
			if (info[1].substring(6).length() <= 1 && info[2].substring(5).length() > 1) {
				ArrayList<String> list = new ArrayList<String>();
				int i = 0;
				String label[] = info[2].substring(5).split(",");
				StringBuffer buffer = new StringBuffer();
				for (String string : label) {
					String temp[] = string.split(":");
					if (temp.length<2) {
						return;
					}
					if (filterTag.containsKey(temp[0])) {
						String filters[] = filterTag.get(temp[0]).split(":");
						if (!list.contains(filters[0])) {
							if ((filters[1].equals("导演") || filters[1].equals("演员")) && i < 5) {
								buffer.append(",").append(filters[0]).append(":").append(temp[1]);
								list.add(filters[0]);
								i++;
							}
							else {
								buffer.append(",").append(filters[0]).append(":").append(temp[1]);
								list.add(filters[0]);
							}
						}
					}
					else {
						if (!list.contains(temp[0])) {
							buffer.append(",").append(string);
							list.add(temp[0]);
						}
					}
				}
				outkey.set(info[0]);
				if (buffer.length()>1) {
					outvalue.set("person" + "\t" + "label" + buffer.substring(1));
					context.write(outkey, outvalue);
				}
			}
		}
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Path[] files=DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for (Path path : files) {
				loadIndex(path.toString(), context);
			}
		}
		private void loadIndex(String file, Context context) throws IOException, FileNotFoundException{
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(file)), "UTF-8"));
			String line = "";
			while ((line=reader.readLine())!=null) {
				String temp[] = line.split(",");
				filterTag.put(temp[1], temp[1]+":"+temp[2]);
			}
		}	
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		String otherArgs[] = new GenericOptionsParser(configuration, args).getRemainingArgs();
		Job job = Job.getInstance(configuration, "filterUserLabelAstep");
		job.setJarByClass(GetFilterUserLabelAstep.class);
		Path cachePath = new Path(otherArgs[2]);
		FileSystem fSystem = FileSystem.get(configuration);
		FileStatus files[] = fSystem.listStatus(cachePath);
		for (FileStatus fileStatus : files) {
			Path path = fileStatus.getPath();
			if (fSystem.isFile(path)) {
				job.addCacheFile(path.toUri());
			}
		}
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, MapClass.class);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0:1);
	}
}
