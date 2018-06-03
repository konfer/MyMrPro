package com.Hadoop.MyMapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class WeatherTest extends Configured implements Tool
{


	public static void main(String[] args) throws Exception
	{
		String[] args0 = {"hdfs://HadoopMasterTencentCloud:9000/data/in/weather/", "hdfs://HadoopMasterTencentCloud:9000/data/out/weatherData/"};
		int ec = ToolRunner.run(new Configuration(), new WeatherTest(), args0);
		System.exit(ec);
	}

	public int run(String[] strings) throws Exception
	{
		//1 read the conf files
		Configuration conf = new Configuration();

		//2 delete the output path if it  exist
		Path myPath = new Path(strings[1]);
		FileSystem hdfs = myPath.getFileSystem(conf);
		if (hdfs.isDirectory(myPath))
		{
			hdfs.delete(myPath, true);
		}

		//3 construct the job object
		//Job job = new Job(conf, "temperature");//新建一个任务
		Job job = Job.getInstance(conf, "WeatherTest");
		job.setJarByClass(WeatherTest.class);// 设置主类


		FileInputFormat.addInputPath(job, new Path(strings[0]));// 输入路径
		System.out.println("Here: " + strings[0]);

		FileOutputFormat.setOutputPath(job, new Path(strings[1]));// 输出路径

		job.setMapperClass(TempratureMapper.class);// Mapper
		job.setReducerClass(TempratureReduce.class);// Reducer

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		int result = job.waitForCompletion(true) ? 0 : 1;//提交任务
		System.out.println("Here: " + result);
		return result;

	}

	public static class TempratureMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			//step 1 : format the waather value
			String line = value.toString();
			//step 2: get the data
			int temprature = Integer.parseInt(line.substring(14, 19).trim());

			//step3: get the stationid

			if (temprature != -9999)
			{
				FileSplit fileSplit = (FileSplit) context.getInputSplit();
				String weatherStationid = fileSplit.getPath().getName().substring(5, 10);

				//step4:output the data
				context.write(new Text(weatherStationid), new IntWritable(temprature));
			}

		}

	}

	public static class TempratureReduce extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		private IntWritable result = new IntWritable();

		public void reduce(Text mykey, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			int count = 0;
			for (IntWritable val : values)
			{
				sum += val.get();
				count++;
			}

			result.set(sum / count);

			context.write(mykey, result);
		}
	}

}
