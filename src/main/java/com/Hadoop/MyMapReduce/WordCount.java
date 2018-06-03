package com.Hadoop.MyMapReduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * 第一个MapReduce程序
 *
 * @author dajiangtai
 */
public class WordCount
{

	public static void main(String[] args) throws Exception
	{
		//String[] args0 = {"hdfs://MyHadoopMaster:9000/data/in/weather/",
		// "hdfs://MyHadoopMaster:9000/data/out/weatherData/"};
		//
		//Configuration conf = new Configuration();
		Configuration conf=new Configuration();
		System.out.println(conf);
//		Job job = Job.getInstance(conf, "word count");
//		job.setJarByClass(WordCount.class);
//		job.setMapperClass(TokenizerMapper.class);
//		job.setCombinerClass(IntSumReducer.class);
//		job.setReducerClass(IntSumReducer.class);
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(IntWritable.class);
//		job.setInputFormatClass(NLineInputFormat.class);
//		// 输入文件路径
//		FileInputFormat.addInputPath(job, new Path("hdfs://HadoopMasterTencentCloud:9000/data/input/"));
//		// 输出文件路径
//		FileOutputFormat.setOutputPath(job, new Path("hdfs://HadoopMasterTencentCloud:9000/data/out/wordcount/"));
//		System.exit(job.waitForCompletion(true) ? 0 : 1);1
	}

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
	{

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens())
			{
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			for (IntWritable val : values)
			{
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
}
