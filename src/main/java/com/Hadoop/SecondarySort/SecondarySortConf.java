package com.Hadoop.SecondarySort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import com.Hadoop.Bean.WritableBean.IntPairWritable;

public class SecondarySortConf extends Configured implements Tool
{
	
	
	
	@Override
	public int run(String[] args) throws Exception
	{
		int result=0;
		FileSystem hdfs=null;
		
		Configuration conf = new Configuration();
		Path outPutPath = new Path(args[1]);
		hdfs = outPutPath.getFileSystem(conf);
		if (hdfs.isDirectory(outPutPath))
		{
			hdfs.delete(outPutPath, true);

		}
		// TODO Auto-generated method stub
		

		Job job=Job.getInstance(conf, "SecondarySort");
        job.setJarByClass(SecondarySortConf.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
//        FileInputFormat.setInputPaths(job, new Path(args[0]));//输入路径
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));//输出路径

        job.setMapperClass(SecondarySortMapper.class);// Mapper
        job.setReducerClass(SecondarySortReducer.class);// Reducer
        
        job.setPartitionerClass(FirstPartitioner.class);// 分区函数
        //job.setSortComparatorClass(KeyComparator.Class);//本课程并没有自定义SortComparator，而是使用IntPair自带的排序
        job.setGroupingComparatorClass(SecondarySortGrouping.class);// 分组函数


        job.setMapOutputKeyClass(IntPairWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
       
        result=job.waitForCompletion(true) ? 0 : 1;
        return result;
	}

}
