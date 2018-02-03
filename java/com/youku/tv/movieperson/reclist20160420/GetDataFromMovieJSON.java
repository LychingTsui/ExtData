package com.youku.tv.movieperson.reclist20160420;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import com.youku.tv.json.JSONArray;
import com.youku.tv.json.JSONException;
public class GetDataFromMovieJSON {
	public static class MapClass extends Mapper<LongWritable, Text, Text, NullWritable> {
		Text outket = new Text();
		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String jsondata = value.toString();
			try {
				if (jsondata != null && jsondata.length() > 1) {
					JSONArray jArray = new JSONArray(jsondata);
					System.out.println(jArray.length());
					for (int i = 0; i < jArray.length(); i++) {
						outket.set(jArray.get(i).toString());
						context.write(outket, NullWritable.get());
					}
				}
			} catch (JSONException e) {
				e.printStackTrace();
			}
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
		job.setJarByClass(GetDataFromMovieJSON.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
				MapClass.class);

		job.setNumReduceTasks(1);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
