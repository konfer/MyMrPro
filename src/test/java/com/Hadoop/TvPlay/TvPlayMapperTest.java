package com.Hadoop.TvPlay;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;

public class TvPlayMapperTest
{
	private Mapper tvplayMapper;
	private MapDriver driver;
	
	public void init()
	{
		tvplayMapper=new TvPlayMapper();
		driver=new MapDriver<>(tvplayMapper);
	}
	
	public void test()
	{
			
	}
	
}
