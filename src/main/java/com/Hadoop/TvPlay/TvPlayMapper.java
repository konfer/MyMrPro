package com.Hadoop.TvPlay;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.Hadoop.Bean.WritableBean.TvplayWritable;

public class TvPlayMapper extends Mapper<Text, TvplayWritable, Text, TvplayWritable>
{

	@Override
	protected void map(Text key, TvplayWritable value, Mapper<Text, TvplayWritable, Text, TvplayWritable>.Context context)
			throws IOException, InterruptedException
	{
		context.write(key, value);
	}

}
