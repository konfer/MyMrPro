package com.Hadoop.Hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HdfsTest
{
	private Configuration conf;
	private FileSystem fs;
	
	@Before
	public void init() throws IOException
	{
		conf=new Configuration();
		fs=FileSystem.get(URI.create("hdfs://MyHadoopMasterMachine:9000"),conf);
	}
	
	@Test
	public void mkdir() throws IllegalArgumentException, IOException
	{
		fs.mkdirs(new Path("/EclipseDir/input"));
		//fs.delete(new Path("/EclipseDir/input");
	}
	
	@Test
	public void copyToHDFS() throws IOException 
	{
		Path scrPath=new Path("/usr/local/kof/hadoop-2.8.4/input/weather.rar");
		Path dstPath=new Path("/EclipseDir/input");
		fs.copyFromLocalFile(scrPath, dstPath);
	}
	
	@Test
	public void copyFileToLocal() throws IOException
	{
		Path srcPath=new Path("/EclipseDir/input/wordcount");
		Path dstPath=new Path("/usr/local/kof/eclipseProTest");
		fs.copyToLocalFile(srcPath, dstPath);
	}
	
	@Test
	public void listAllFile() throws FileNotFoundException, IOException
	{ 
		Path srcPath=new Path("/EclipseDir/input");
		FileStatus[] status=fs.listStatus(srcPath);
		Path[] listedPaths=FileUtil.stat2Paths(status);
		
		for(Path p:listedPaths)
		{
			System.out.println(p);
		}
	}
	
	@Test
	public void getFileLocal() throws IOException
	{
		Path p=new Path("/EclipseDir/input/wordcount");
		FileStatus fileStatus=fs.getFileStatus(p);
		BlockLocation[] blkLocations=fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
		for(int i=0;i<blkLocations.length;i++)
		{
			String[] hosts=blkLocations[i].getHosts();
			System.out.println("block:"+i+"_location:"+hosts[0]);
		}
	}
	
	@Test
	public void getHDFSNodes() throws IOException
	{
		DistributedFileSystem hdfs=(DistributedFileSystem)fs;
		DatanodeInfo[] dataNodeInfoStauts=hdfs.getDataNodeStats();
		for(int i=0;i<dataNodeInfoStauts.length;i++)
		{
			System.out.println("DataNode_"+i+dataNodeInfoStauts[i].getHostName());
		}
	}
	
	@After
	public void finished() throws IOException
	{
		fs.close();
	}
			
}
