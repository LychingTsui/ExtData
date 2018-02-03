package com.youku.tv.movieperson.reclist20160420;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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

public class GetcomplentTags {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{
		HashMap<String, String> map = new HashMap<String, String>();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			MovieDataMeta meta = new MovieDataMeta(value.toString());
			String tags[] = meta.Gettags().split(",");
			StringBuffer buffer = new StringBuffer();
			ArrayList<String> list = new ArrayList<String>();
			for (String string : tags) {
				if (map.containsKey(string)) {
					if ( !list.contains(map.get(string))) {
						buffer.append(",").append(map.get(string));
						list.add(map.get(string));
					}
				}
				else{
					if (!list.contains(string)) {
						buffer.append(",").append(string);
						list.add(string);
					}
				}
			}
			meta.Settags(buffer.substring(1));
			context.write(new Text(meta.Getid()), new Text(meta.ToValueString()));
		}
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Path pathList[] = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for (Path path : pathList) {
				loadIndex(path.toString(), context);
			}
		}
		private void loadIndex(String file, Context context) throws IOException,
		InterruptedException {
			FileReader fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr);
			String line = null;
			while ((line = br.readLine()) != null) {
				String temp[] = line.split(",");
				if (temp.length >= 3) {
					/*添加用户的id 以及用户对时间、分数维度的喜好*/
					map.put(temp[0], temp[1]);
				}
			}
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		String otherArgs[] = new GenericOptionsParser(configuration, args).getRemainingArgs();
		Job job = Job.getInstance(configuration, "GetcomplentTags");
		job.setJarByClass(GetCompletionTags.class);
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
