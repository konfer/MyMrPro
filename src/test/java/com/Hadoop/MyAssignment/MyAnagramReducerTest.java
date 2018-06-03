package com.Hadoop.MyAssignment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.Util.StringTools.IterableSplit;
import com.sun.tools.internal.xjc.Driver;

public class MyAnagramReducerTest
{
	private Reducer anagramReducer;
	private ReduceDriver driver;
	
	@Before
	public void init()
	{
		anagramReducer=new AnagramsStatReducer();
		driver=new ReduceDriver<>(anagramReducer);
		
	}
	
	@Test
	public void test() throws IOException
	{
		//line example: illst	---- tills , still , lilts
		String key="illst";
		List<Text> values=new ArrayList<Text>();
		values.add(new Text("tills"));
		values.add(new Text("still"));
		values.add(new Text("lilts"));
		driver.withInput(new Text(key),values)
		  .withOutput(new Text(key),new Text(IterableSplit.IterableSplittedByComma(values))).runTest();
		
	}
	
}
