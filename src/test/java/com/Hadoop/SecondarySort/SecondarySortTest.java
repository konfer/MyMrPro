package com.Hadoop.SecondarySort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;


public class SecondarySortTest
{

	@Test
	public void testSecondarySort() throws Exception
	{
		String[] args= {
				"hdfs://MyHadoopMasterMachine:9000/data/input/SecondarySort/secondarySort1.txt",
				"hdfs://MyHadoopMasterMachine:9000/data/output/SecondarySort/"
			};
		int result=ToolRunner.run(new Configuration(), new SecondarySortConf(), args);
	}
	
}
