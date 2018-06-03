package com.WeatherPro;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Temperature
{

	public static class Temperaturemapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		public void map(LongWritable key,Text text,Context context)
		{
			String line=text.toString();
		}
	}

}
