package com.Hadoop.HBase.WordCountMR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableMap;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;

import com.Hadoop.MyAssignment.AnagramConf;
import com.Hadoop.MyAssignment.AnagramsStatMapper;
import com.Hadoop.MyAssignment.AnagramsStatReducer;
import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

import util.StringTools.MyStringUtil;

public class WordCountHBaseMRDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception 
	{
		// TODO Auto-generated method stub
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "MyHadoopMasterMachine,MyHadoopSlaveMachine1,MyHadoopSlaveMachine2,MyHadoopSlaveMachine3,MyHadoopSlaveMachine4");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.master", "MyHadoopMasterMachine:16000");
		
		Job job = Job.getInstance(conf, "WordCountHBase");
		job.setJarByClass(WordCountHBaseMRDriver.class);
		job.setMapperClass(WordCountHBaseMapper.class);
		
		
		
		int result=0;
		
   	    try
   	    {
   	    		Connection connection=ConnectionFactory.createConnection(conf);
   			
   			TableName tableName=TableName.valueOf("wordCount");
   			HTable hTable=(HTable) connection.getTable(tableName);;
   			HBaseAdmin hBaseAdmin=(HBaseAdmin) connection.getAdmin();
   			
   			
   			
   			if(hBaseAdmin.tableExists(tableName))
   			{
   				hBaseAdmin.disableTable(tableName);
   				hBaseAdmin.deleteTable(tableName);
   	   	    }
   	   	    
   	   	    HTableDescriptor htd = new HTableDescriptor(tableName);
   	   	    HColumnDescriptor hcd = new HColumnDescriptor("content");
   	   	    htd.addFamily(hcd);//创建列簇
   	   	    hBaseAdmin.createTable(htd);//创建表
   	    		
   			
   			Scan scan = new Scan();
   			scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
   			scan.setCacheBlocks(false);
   			
   			TableMapReduceUtil.initTableReducerJob("wordCount", WordCountHBaseReducer.class, job, null, null, null, null, false);
   		
   			job.setMapOutputKeyClass(ImmutableBytesWritable.class);  
   	        job.setMapOutputValueClass(IntWritable.class);  
   	        
   	        job.setOutputKeyClass(ImmutableBytesWritable.class);  
   	        job.setOutputValueClass(Put.class);
   	        
   	        FileInputFormat.addInputPath(job, new Path(args[0]));
   			
   	        result = job.waitForCompletion(true) ? 0 : 1;
   	        connection.close();
   	        hBaseAdmin.close();
   	        hTable.close();
   	    }catch(Exception e)
   	    {
   	    		e.printStackTrace();
   	    }
		
		return result;
	}

}
