package com.Hadoop.TvPlay;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import com.Hadoop.Bean.WritableBean.TvplayWritable;
import com.Hadoop.MyAssignment.AnagramConf;
import com.Hadoop.MyAssignment.AnagramsStatMapper;

public class TvPlayConf extends Configured implements Tool
{

	@Override
	public int run(String[] args) throws Exception
	{
		// TODO Auto-generated method stub
		FileSystem hdfs = null;
		Job job = null;
		int result = 0;

		Configuration conf = new Configuration();
		Path outPutPath = new Path(args[1]);
		try
		{
			hdfs = outPutPath.getFileSystem(conf);
			if (hdfs.isDirectory(outPutPath))
			{
				hdfs.delete(outPutPath, true);

			}

			job = Job.getInstance(conf, "TvPlayStat");

			job.setJarByClass(TvPlayConf.class);
			job.setMapperClass(TvPlayMapper.class);
			job.setReducerClass(TvPlayReducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(TvplayWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			job.setInputFormatClass(TvPlayInputFormat.class);
			
			MultipleOutputs.addNamedOutput(job, "Youku", TextOutputFormat.class,Text.class, Text.class);
			MultipleOutputs.addNamedOutput(job, "Souhu", TextOutputFormat.class,Text.class, Text.class);
			MultipleOutputs.addNamedOutput(job, "Tudou", TextOutputFormat.class,Text.class, Text.class);
			MultipleOutputs.addNamedOutput(job, "Iqiyi", TextOutputFormat.class,Text.class, Text.class);
			MultipleOutputs.addNamedOutput(job, "Xunlei", TextOutputFormat.class,Text.class, Text.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			

			result = job.waitForCompletion(true) ? 0 : 1;

		} catch (Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
		}
		return 0;
	}

}
