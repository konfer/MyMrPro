package com.Hadoop.HBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import com.Hadoop.HBase.WordCountMR.WordCountHBaseMRDriver;
import com.Hadoop.TvPlay.TvPlayConf;

public class HBaseMrTest 
{

	@Test
	public void testHBaseMr() throws Exception
	{
		String[] args= {
				"hdfs://MyHadoopMasterMachine:9000/data/input/wordcount"
			};
			int result=ToolRunner.run(new Configuration(), new WordCountHBaseMRDriver(), args);
			
	}
	
}
