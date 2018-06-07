package com.Hadoop.MyAssignment;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import util.StringTools.StringInnerSort;

public class MyAnagramMapperTest
{
	private Mapper anagramsMapper;
	private MapDriver driver;
	
	@Before
	public void init()
	{
		anagramsMapper=new AnagramsStatMapper();
		driver=new MapDriver<>(anagramsMapper);
		
	}
	
	@Test
	public void test() throws IOException
	{
		//line example: illst	---- tills , still , lilts
		String line="tills";
		driver.withInput(new LongWritable(), new Text(line))
			  .withOutput(new Text(StringInnerSort.StringSort(line)), new Text(line))
			  .runTest();
		
		
	}
	
	
}
