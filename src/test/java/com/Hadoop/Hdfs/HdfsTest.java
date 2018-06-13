package com.Hadoop.Hdfs;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import util.Hadoop.RegexAcceptPathFilter;

public class HdfsTest
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
	
	@Test
	public void testMkdir() throws IllegalArgumentException, IOException
	{
		fs.mkdirs(new Path("/EclipseDir/input"));
		//fs.delete(new Path("/EclipseDir/input");
	}
	
	@Test
	public void testCopyToHDFS() throws IOException 
	{
		Path scrPath=new Path("/usr/local/kof/hadoop-2.8.4/input/weather.rar");
		Path dstPath=new Path("/EclipseDir/input");
		fs.copyFromLocalFile(scrPath, dstPath);
	}
	
	@Test
	public void testCopyFileToLocal() throws IOException
	{
		Path srcPath=new Path("/EclipseDir/input/wordcount");
		Path dstPath=new Path("/usr/local/kof/eclipseProTest");
		fs.copyToLocalFile(srcPath, dstPath);
	}
	
	@Test
	public void getTvPlayResult() throws IOException
	{
		Path srcPath=new Path("/data/output/TvPlay");
		Path dstPath=new Path("/usr/local/kof/eclipseProTest");
		fs.copyToLocalFile(srcPath, dstPath);
	}
	
	@Test
	public void testListAllFile() throws FileNotFoundException, IOException
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
	public void testGetFileLocal() throws IOException
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
	public void testGetHDFSNodes() throws IOException
	{
		DistributedFileSystem hdfs=(DistributedFileSystem)fs;
		DatanodeInfo[] dataNodeInfoStauts=hdfs.getDataNodeStats();
		for(int i=0;i<dataNodeInfoStauts.length;i++)
		{
			System.out.println("DataNode_"+i+dataNodeInfoStauts[i].getHostName());
		}
	}
	
	@Test
	public void testUpLoadFiles() throws IOException
	{
		
		FileStatus[] localStatus=localFiles.globStatus(new Path("/usr/local/kof/data/205_data/data/*"));
		Path[] listedPaths=FileUtil.stat2Paths(localStatus);
		Path out=new Path("/EclipseDir/input");
		for(Path p:listedPaths)
		{
			fs.copyFromLocalFile(p, out);
		}
	}
	
	@Test
	public void testUpLoadCompressedFilesToHdfs() throws IOException
	{
		FileStatus[] upLoadStatus=localFiles.globStatus(new Path("/mnt/hgfs/ShareFolder/73/*"),new RegexAcceptPathFilter("^.*svn$", false));
		Path[] upLoaddirs=FileUtil.stat2Paths(upLoadStatus);
		
		for(Path dir:upLoaddirs)
		{
			String fileName = dir.getName();//文件名称
			//只接受日期目录下的.txt文件，^匹配输入字符串的开始位置,$匹配输入字符串的结束位置,*匹配0个或多个字符。
			FileStatus[] localStatus = localFiles.globStatus(new Path(dir+"/*"),new RegexAcceptPathFilter("^.*txt$"));
			// 获得日期目录下的所有文件
			Path[] listedPaths = FileUtil.stat2Paths(localStatus);
			//输出路径
			Path block = new Path("/EclipseDir/upLoadCompressedFiles/"+fileName+".txt");
			// 打开输出流
			out = fs.create(block);			
			for (Path p : listedPaths) {
				in = localFiles.open(p);// 打开输入流
				IOUtils.copyBytes(in, out, 4096, false); // 复制数据，IOUtils.copyBytes可以方便地将数据写入到文件，不需要自己去控制缓冲区，也不用自己去循环读取输入源。false表示不自动关闭数据流，那么就手动关闭。
				// 关闭输入流
				in.close();
			}
			if (out != null) {
				// 关闭输出流
				out.close();
			}
		}
	}
	
	
	
	@After
	public void finished() throws IOException
	{
		fs.close();
		localFiles.close();
		if(in != null)
		{
			in.close();
		}
		
		if (out != null) 
		{
			// 关闭输出流
			out.close();
		}
	}
			
}
