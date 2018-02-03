package com.qiguo.tv.movie.featCollection;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @info: 功能在GetMovieItem_features2完成后，＋ 观看时间特征（在脚本中完成计算，传值给args）
 */

public class GetMovieItem_featuresWithDate {
	public static class MapClass extends Mapper<LongWritable, Text, Text, NullWritable>{
		static double dateFeat = 0.0;
		static int dateIdStart = 0;
		public void map(LongWritable key, Text val, Context context)throws IOException,
		InterruptedException{
			String valStr = val.toString();
			String dateftPair = dateIdStart+":"+dateFeat;
			String ftStr =valStr + dateftPair;
			context.write(new Text(ftStr), NullWritable.get());
		}
		public void setup(Context context)throws IOException,InterruptedException{
			dateIdStart = Integer.parseInt(context.getConfiguration().get("dateIdxStart"));
			dateFeat = Double.parseDouble(context.getConfiguration().get("dateFeatureValue"));
			super.setup(context);
		}
	}
	
	public static void main(String[] args)throws IOException,InterruptedException,
	ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies","30");
		String[] othArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		
		conf.set("dateIdxStart", othArgs[2]);
		conf.set("dateFeatureValue", othArgs[3]);
		Job job = Job.getInstance(conf,"addDataFeature");
		job.setJarByClass(GetMovieItem_featuresWithDate.class);
		MultipleInputs.addInputPath(job, new Path(othArgs[0]), TextInputFormat.class,
				MapClass.class);

		job.setNumReduceTasks(1);
		//job.setReducerClass(cls);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileOutputFormat.setOutputPath(job, new Path(othArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
 }
