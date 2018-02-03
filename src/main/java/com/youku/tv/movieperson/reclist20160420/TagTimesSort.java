package com.youku.tv.movieperson.reclist20160420;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class TagTimesSort {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "fenlei job2");
		job2.setJarByClass(TagTimesSort.class);
		job2.setMapperClass(Topmap.class);
		job2.setReducerClass(Topreduce.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		job2.waitForCompletion(true);

	}
 
	public static class Topmap extends Mapper<LongWritable, Text, Text, Text>
	{
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException
		{
			String linestr = value.toString();
			String[] linelist = linestr.split("\t");
			if (linelist.length == 2) {
					context.write(new Text(linelist[0]), new Text(linelist[1]));
			}
		}
	}
	public static class Topreduce extends Reducer<Text, Text, Text, Text>
	{
		Comparator<tags> compare;
		PriorityQueue<tags> top;

		protected void setup(Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException
				//在setup中构造一个按照要求排序的队列，初始化该队列
				{
			//完善比较器，确定排序（倒序）
			Comparator<tags> compare = new Comparator<tags>()
			{
				public int compare(tags a, tags b) {
					double x = a.getValue();
					double y = b.getValue();
					if (x == y) return 0;
					return x > y ? -1 : 1;
				}
			};//加载比较器
			this.top = new PriorityQueue<tags>(9000, compare);
			
		}

		protected void reduce(Text key, Iterable<Text> values, Reducer<Text,Text, Text, Text>.Context context)
				throws IOException, InterruptedException
		{
			for (Text tt : values){
				Double sb=Double.valueOf(tt.toString());
		   tags t = new tags(key.toString(),sb);
			this.top.add(t);
			}
			 
		}

		protected void cleanup(Reducer<Text,Text, Text, Text>.Context context)
				throws IOException, InterruptedException
		{  

			int i = 0;

			while ((!this.top.isEmpty()) && (i < 9000)) {
				tags t = (tags)this.top.poll();
				i++;
				if (t.getValue()>=1) {
					context.write(new Text(t.getKey()), new Text(t.getValue()+""));
				}
			}
		}
	}

}
class  tags{
	public Double value;
	public String key;
	public tags(String key,Double value){
		this.key=key;
		this.value=value;
	}
	public Double getValue() {
		return value;
	}
	public void setValue(Double value) {
		this.value = value;
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	
}