package com.Hadoop.SecondarySort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.Hadoop.Bean.WritableBean.IntPairWritable;


public class SecondarySortReducer extends Reducer<IntPairWritable, IntWritable, Text, IntWritable>
{
	private final Text left = new Text();  

	@Override
	protected void reduce(IntPairWritable key, Iterable<IntWritable> values, Reducer<IntPairWritable, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException
	{
		
		
		left.set(Integer.toString(key.getFirst()));
        for (IntWritable val : values)
        {
        	System.out.println(key.getFirst()+"|"+val);
        	context.write(left, val);
        	
        }
        System.out.println("-------------------");
	}

	
	
}
