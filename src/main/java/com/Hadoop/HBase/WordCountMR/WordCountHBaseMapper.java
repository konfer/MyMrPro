package com.Hadoop.HBase.WordCountMR;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountHBaseMapper extends Mapper<Object, Text, ImmutableBytesWritable, IntWritable> 
{
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException 
	{
		// TODO Auto-generated method stub
		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) 
		{
			word.set(itr.nextToken());
			//输出到hbase的key类型为ImmutableBytesWritable
			context.write(new ImmutableBytesWritable(Bytes.toBytes(word.toString())), one);
		}
	}

}
