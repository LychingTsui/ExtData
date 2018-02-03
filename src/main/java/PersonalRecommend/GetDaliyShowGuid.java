package PersonalRecommend;

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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/*
 * @info: 从当天的show中获取当天的用户guid
 */
public class GetDaliyShowGuid {
	public static class MapClass extends Mapper<LongWritable, Text, Text, NullWritable>{
		public void map(LongWritable key, Text val, Context context) throws IOException,
		InterruptedException{
			String rowStr = val.toString();
			StringTokenizer stk = new StringTokenizer(rowStr, "\t");
			String guid = stk.nextToken();
			String str = "00:00:00:00:00:00";
			if(!guid.equals(str)){
				context.write(new Text(guid), NullWritable.get());
			}						
		}
	}
	
	public static void main(String[] args)throws IOException,InterruptedException,
	ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "30");
		Job job = Job.getInstance(conf,"showGuid");
		String[] argsStr = new GenericOptionsParser(conf, args).getRemainingArgs();
		MultipleInputs.addInputPath(job, new Path(argsStr[0]), TextInputFormat.class,
				MapClass.class);

		job.setJarByClass(GetDaliyShowGuid.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		//job.setReducerClass(ReduceClass.class);
		FileOutputFormat.setOutputPath(job, new Path(argsStr[1]));
		System.exit(job.waitForCompletion(true)? 0 : 1);
	}
}
