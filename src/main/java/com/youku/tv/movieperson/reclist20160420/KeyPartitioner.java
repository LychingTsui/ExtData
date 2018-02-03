package com.youku.tv.movieperson.reclist20160420;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class KeyPartitioner extends Partitioner<TextPair, Text> {

	@Override
	public int getPartition(TextPair key, Text value, int numPartitions) {
		// TODO Auto-generated method stub
		return (key.getText().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

}
