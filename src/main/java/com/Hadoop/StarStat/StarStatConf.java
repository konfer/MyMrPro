package com.Hadoop.StarStat;

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


public class StarStatConf extends Configured implements Tool
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

			job = new Job(conf, "StarStat");//新建一个任务
			job.setJarByClass(StarStatConf.class);//主类
			
			job.setNumReduceTasks(2);//reduce的个数设置为2
			job.setPartitionerClass(StarStatPartitioner.class);//设置Partitioner类
			
			
			
			job.setMapperClass(StarStatMapper.class);//Mapper
			job.setMapOutputKeyClass(Text.class);//map 输出key类型
			job.setMapOutputValueClass(Text.class);//map 输出value类型
					
			job.setCombinerClass(StarStatCombiner.class);//设置Combiner类
			
			job.setReducerClass(StarStatReducer.class);//Reducer
			job.setOutputKeyClass(Text.class);//输出结果 key类型
			job.setOutputValueClass(Text.class);//输出结果 value类型
			
			FileInputFormat.addInputPath(job, new Path(args[0]));// 输入路径
			FileOutputFormat.setOutputPath(job, new Path(args[1]));// 输出路径
			job.waitForCompletion(true);//提交任务
			return 0;

		} catch (Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
		}
		return 0;
		
	}

}
