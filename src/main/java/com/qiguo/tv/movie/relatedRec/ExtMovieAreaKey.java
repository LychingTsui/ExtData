package com.qiguo.tv.movie.relatedRec;

import java.io.IOException;
import java.util.StringTokenizer;

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

import com.qiguo.tv.movie.relatedRec.ExtMoviesActorKey.MapClass;
import com.qiguo.tv.movie.relatedRec.ExtMoviesActorKey.ReduceClass;

public class ExtMovieAreaKey {
	public static class MapClass extends Mapper<LongWritable, Text, Text, NullWritable>{
		protected void map(LongWritable key, Text val, Context context) throws IOException,
		InterruptedException{
			StringTokenizer stk = new StringTokenizer(val.toString(), "\t");
			while (stk.hasMoreTokens()) {
				String infield = stk.nextToken();
				if(infield.startsWith("area") && infield.length() > 5){  // area 
					StringTokenizer substk = new StringTokenizer(infield.substring(5), ",");
					context.write(new Text(substk.nextToken()), NullWritable.get());
				}
			}
		}
	}
	public static class ReduceClass extends Reducer<Text, NullWritable, Text, NullWritable>{
		protected void reduce(Text key, Iterable<NullWritable>vals, Context context) throws IOException,
		InterruptedException{
			String out = "t4" + key.toString();
			context.write(new Text(out), NullWritable.get());
		}
	}
	public static void main(String[] args)throws IOException, 
	InterruptedException, ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "30");
		
		Job job = Job.getInstance(conf,"area");
		String[] argsStr = new GenericOptionsParser(conf, args).getRemainingArgs();
		MultipleInputs.addInputPath(job, new Path(argsStr[0]), TextInputFormat.class,
				MapClass.class);
		
		job.setJarByClass(ExtMovieAreaKey.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setReducerClass(ReduceClass.class);
		FileOutputFormat.setOutputPath(job, new Path(argsStr[1]));
		System.exit(job.waitForCompletion(true)? 0 : 1);
	}
}
