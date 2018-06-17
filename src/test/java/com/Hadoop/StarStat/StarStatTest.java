package com.Hadoop.StarStat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;


public class StarStatTest
{

	@Test
	public void testStarStat() throws Exception
	{
		String[] args= {
				"hdfs://MyHadoopMasterMachine:9000/data/input/actor/actor.txt",
				"hdfs://MyHadoopMasterMachine:9000/data/output/actor/"
			};
		int result=ToolRunner.run(new Configuration(), new StarStatConf(), args);
	}
	
}
	