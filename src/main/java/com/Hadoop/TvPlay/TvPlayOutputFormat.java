package com.Hadoop.TvPlay;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.Hadoop.Bean.WritableBean.TvplayWritable;

public class TvPlayOutputFormat extends FileInputFormat<Text, TvplayWritable>
{

	@Override
	public RecordReader<Text, TvplayWritable> createRecordReader(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException
	{
		// TODO Auto-generated method stub
		return null;
	}

}
