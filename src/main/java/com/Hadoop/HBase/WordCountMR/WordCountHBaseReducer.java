package com.Hadoop.HBase.WordCountMR;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import util.StringTools.MyStringUtil;

public class WordCountHBaseReducer extends TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> 
{

	@Override
	protected void reduce(ImmutableBytesWritable key, Iterable<IntWritable> values,
			Reducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable, Mutation>.Context context)
			throws IOException, InterruptedException 
	{
		int sum=0;
		for(IntWritable val:values)
		{
			sum+=val.get();
		}
		
		Put put=new Put(key.get());
		put.addColumn(MyStringUtil.getBytes("content"), MyStringUtil.getBytes("count"), MyStringUtil.getBytes(String.valueOf(sum)));
		context.write(key, put);
	}
	
}
