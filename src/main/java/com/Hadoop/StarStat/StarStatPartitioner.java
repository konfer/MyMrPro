package com.Hadoop.StarStat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class StarStatPartitioner extends Partitioner<Text, Text>
{

	@Override
	public int getPartition(Text key, Text value, int numReduceTasks)
	{
		// TODO Auto-generated method stub
		String gender=key.toString();
		if(numReduceTasks==0)
			return 0;
		//性别为male 选择分区0
		if(gender.equals("male"))             
			return 0;
		//性别为female 选择分区1
		if(gender.equals("female"))
			return 1 % numReduceTasks;
		//其他性别 选择分区2
		else
			return 2 % numReduceTasks;
	}

}
