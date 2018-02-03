package PersonalRecommend;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

/*
 * @info: 从当天的vv数据里获取当天的guid
 * 		与GetDaliyShowGuid获取的guid合并去重（解决有些vv里的guid在show的数据中没有的问题）后为当天的在线用户
 * 		作为当天预测推荐的用户集
 */
public class GetDayVvGuid {
	public static class MapClass extends Mapper<LongWritable, Text, Text, IntWritable>{
		private final IntWritable One = new IntWritable(1);
		protected void map(LongWritable key, Text val, Context context)throws IOException,
		InterruptedException{
			StringTokenizer stk = new StringTokenizer(val.toString(), "\t");
			String guid = stk.nextToken();
			if(!guid.equals("00:00:00:00:00:00")){
				context.write(new Text(guid), One);
			}
		}
	}
	public static class ReduceClass extends Reducer<Text, IntWritable, Text, NullWritable>{
		protected void reduce(Text key, Iterable<IntWritable> val, Context context)throws IOException,
		InterruptedException{
			context.write(key, NullWritable.get());
		}
	}
	public static void main(String[] args)throws IOException,
	InterruptedException,ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "30");
		Job job = Job.getInstance(conf,"vvGuid");
		String[] argsStr = new GenericOptionsParser(conf, args).getRemainingArgs();
		MultipleInputs.addInputPath(job, new Path(argsStr[0]), TextInputFormat.class,
				MapClass.class);

		job.setJarByClass(GetDayVvGuid.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setReducerClass(ReduceClass.class);
		FileOutputFormat.setOutputPath(job, new Path(argsStr[1]));
		System.exit(job.waitForCompletion(true)? 0 : 1);
	}
}
