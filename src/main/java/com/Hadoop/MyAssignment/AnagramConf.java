package com.Hadoop.MyAssignment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AnagramConf extends Configured implements Tool
{

	public int run(String[] args)
	{
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

			job = Job.getInstance(conf, "AnagramString");

			job.setJarByClass(AnagramConf.class);
			job.setMapperClass(AnagramsStatMapper.class);
			job.setReducerClass(AnagramsStatReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, outPutPath);

			result = job.waitForCompletion(true) ? 0 : 1;

		} catch (Exception e)
		{
			e.printStackTrace();
		}

		return result;

	}


	public static void main(String[] args) throws Exception
	{
		String[] args0 = new String[]{"hdfs://MyHadoopMasterMachine:9000/data/input/anagram/anagram.txt", "hdfs://MyHadoopMasterMachine:9000/data/output/20/"};
		int ec = ToolRunner.run(new Configuration(), new AnagramConf(), args0);
		System.exit(ec);
	}

}

