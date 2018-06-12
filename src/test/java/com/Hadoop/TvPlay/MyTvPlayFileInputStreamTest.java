package com.Hadoop.TvPlay;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.Test;

import util.StringTools.StringSplitter;

public class MyTvPlayFileInputStreamTest 
{
	private Configuration conf;
	private FileSystem fs;
	private FileSystem localFiles;
	
	private FSDataInputStream in;
	private FSDataOutputStream out;
	
	@Before
	public void init() throws IOException
	{
		conf=new Configuration();
		fs=FileSystem.get(URI.create("hdfs://MyHadoopMasterMachine:9000"),conf);
		localFiles=FileSystem.getLocal(conf);
	}
	
	

	//@Test
	public void TvPlayJobRun() throws Exception
	{
		String[] args= {
			"hdfs://MyHadoopMasterMachine:9000/data/TvPlay/tvplay.txt",
			"hdfs://MyHadoopMasterMachine:9000/data/output/TvPlay/"
		};
		int result=ToolRunner.run(new Configuration(), new TvPlayConf(), args);
	}
	
	@Test
	public void testTvPlayFileSplitted() 
	{
		String TvPlayFileDir="/data/TvPlay/tvplay.txt";
		StringBuffer buff=new StringBuffer();
        BufferedReader bufferedReader = null;  
        FSDataInputStream fsr = null;  
        String lineTxt = null;
        try
        {
        	fsr=fs.open(new Path(TvPlayFileDir));
        	bufferedReader=new BufferedReader(new InputStreamReader(fsr));
        	lineTxt=bufferedReader.readLine();
        }catch (Exception e)  
        {  
            e.printStackTrace();  
        } finally  
        {  
            if (bufferedReader != null)  
            {  
                try  
                {  
                    bufferedReader.close();  
                } catch (IOException e)  
                {  
                    e.printStackTrace();  
                }  
            }  
        }  
        String[] pieces=StringSplitter.tabSplitter(lineTxt);
        for(String str:pieces)
        {
        	System.out.println(str);
        }
	}
	
	
}
