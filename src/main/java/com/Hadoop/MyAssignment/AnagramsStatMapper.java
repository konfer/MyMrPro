package com.Hadoop.MyAssignment;

import util.StringTools.StringInnerSort;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AnagramsStatMapper extends Mapper<LongWritable, Text, Text, Text>
{
	private Text sortedText = new Text();
	private Text originText=new Text();

	@Override protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String AnagramString = value.toString();
		String AnagramSortedString= StringInnerSort.StringSort(AnagramString);
		sortedText.set(AnagramSortedString);
		originText.set(AnagramString);
		context.write(sortedText,originText);


		//	sortedText=AnagramString;
	}

	public static void main(String args[])
	{
		String str = "fjdkshd";
		System.out.println(StringInnerSort.StringSort(str));
	}


}