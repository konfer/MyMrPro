package com.Hadoop.StarStat;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StarStatMapper extends Mapper<LongWritable, Text, Text, Text>
{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException
	{
		// 李易峰  male    32670
		// TODO Auto-generated method stub
		String[] tokens=value.toString().split("\t");
		String gender=tokens[1].trim();
		String actorName=tokens[0]+"@"+tokens[2];
		context.write(new Text(gender), new Text(actorName));
				
	}
	
}
