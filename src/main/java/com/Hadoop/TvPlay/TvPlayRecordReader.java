package com.Hadoop.TvPlay;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import com.Hadoop.Bean.WritableBean.TvplayWritable;

import util.StringTools.AppendString;
import util.StringTools.StringSplitter;

public class TvPlayRecordReader extends RecordReader<Text, TvplayWritable>
{
	public LineReader in;
	public Text lineKey;
	public TvplayWritable tvPlayValue;
	public Text line;

	@Override
	public void close() throws IOException
	{
		// TODO Auto-generated method stub
		if(in !=null){
            in.close();
        }
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException
	{
		return lineKey;
	}

	@Override
	public TvplayWritable getCurrentValue() throws IOException, InterruptedException
	{
		// TODO Auto-generated method stub
		return tvPlayValue;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void initialize(InputSplit input, TaskAttemptContext context) throws IOException, InterruptedException
	{
		// TODO Auto-generated method stub
		FileSplit split=(FileSplit)input;
        Configuration job=context.getConfiguration();
        Path file=split.getPath();
        FileSystem fs=file.getFileSystem(job);
        
        FSDataInputStream filein=fs.open(file);
        in=new LineReader(filein,job);
        line=new Text();
        lineKey=new Text();
        tvPlayValue=new TvplayWritable();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException
	{
		// TODO Auto-generated method stub
		int linesize=in.readLine(line);
		if(linesize==0) 
		{
			return false;
		}
		String[] records=StringSplitter.tabSplitter(line.toString());
		if(records.length==7)
		{
			lineKey.set(AppendString.AppendByAt(records[0], records[1]));
			tvPlayValue.set(Integer.parseInt(records[2]),Integer.parseInt(records[3]),Integer.parseInt(records[4])
	        		,Integer.parseInt(records[5]),Integer.parseInt(records[6]));
			return true;
		}
		else
		{
			return true;
		}
		
	}

}
