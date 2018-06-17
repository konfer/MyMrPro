package com.Hadoop.SecondarySort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import com.Hadoop.Bean.WritableBean.IntPairWritable;

public class FirstPartitioner extends Partitioner<IntPairWritable, IntWritable>
{

	@Override
	public int getPartition(IntPairWritable key, IntWritable value, int numPartitions)
	{
		// TODO Auto-generated method stub
		return Math.abs(key.getFirst()*3)%numPartitions;
	}

}
