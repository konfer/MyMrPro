package com.Hadoop.HBase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.Util.HBase.ResultIteratorBySysOut;

import util.StringTools.MyStringUtil;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class HBaseTest 
{
	private Configuration conf ;
	private HTable hTable;
	private HBaseAdmin hBaseAdmin;
	private Connection connection;
	
	private TableName tableName;
	private HTableDescriptor table;
	private Get rowGet;
	private Scan scan;
	private Scan scanFilter;
	private ResultScanner rScanner;
	
	private Delete d ;
	
	private static final String TABLE_NAME = "wordCount";

	@Before
	public void init() throws IOException
	{
		conf = HBaseConfiguration.create();

		conf.set("hbase.zookeeper.quorum", "MyHadoopMasterMachine,MyHadoopSlaveMachine1,MyHadoopSlaveMachine2,MyHadoopSlaveMachine3,MyHadoopSlaveMachine4");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.master", "MyHadoopMasterMachine:16000");
			
		connection=ConnectionFactory.createConnection(conf);
		hBaseAdmin=(HBaseAdmin) connection.getAdmin();
		
		tableName=TableName.valueOf(TABLE_NAME);
		table=new HTableDescriptor(tableName);
		
		hTable=(HTable) connection.getTable(tableName);
		rowGet=new Get(MyStringUtil.getBytes("MyRowKey"));
		scan=new Scan();
		scanFilter=new Scan();
		d=new Delete(MyStringUtil.getBytes("MyRowKey"));
		
		rScanner=hTable.getScanner(scan);
		
	}

	@Test
	public void testCreateTable() throws MasterNotRunningException,ZooKeeperConnectionException,IOException 
	{
		System.out.println("-------createTable--------");
		System.out.println("get test");
		if(hBaseAdmin.tableExists(tableName))
		{
			System.out.println("already exists");
			hBaseAdmin.disableTable(tableName);
			hBaseAdmin.deleteTable(tableName);
		}
		
		System.out.println("file check finished");
		
		table.addFamily(new HColumnDescriptor("address").setCompressionType(Algorithm.NONE));
		table.addFamily(new HColumnDescriptor("info").setCompressionType(Algorithm.NONE));
		
		hBaseAdmin.createTable(table);

	}
	
	@Test
	public void testCrfInsertDataByPut() throws IOException 
	{
		System.out.println("-------testInsertDataByPut--------");
		
		if(hBaseAdmin.tableExists(tableName))
		{
			System.out.println("already exists, start insert ");
			
			
			Put addressPut=new Put(MyStringUtil.getBytes("MyRowKey"));
			addressPut.addColumn(MyStringUtil.getBytes("address"),MyStringUtil.getBytes("country"),MyStringUtil.getBytes("china"));
			addressPut.addColumn(MyStringUtil.getBytes("address"),MyStringUtil.getBytes("province"),MyStringUtil.getBytes("zj"));
			addressPut.addColumn(MyStringUtil.getBytes("address"),MyStringUtil.getBytes("city"),MyStringUtil.getBytes("hz"));
			
			
			addressPut.addColumn(MyStringUtil.getBytes("info"),MyStringUtil.getBytes("age"),MyStringUtil.getBytes("28"));
			addressPut.addColumn(MyStringUtil.getBytes("info"),MyStringUtil.getBytes("birthday"),MyStringUtil.getBytes("1994-005"));
			addressPut.addColumn(MyStringUtil.getBytes("info"),MyStringUtil.getBytes("company"),MyStringUtil.getBytes("taobao"));
			
			hTable.put(addressPut);
		}
		
	}
	
	@Test
	public void testGetAllTables() throws IOException
	{
		System.out.println("-------testGetAllTables--------");
		
		String tName="";
		TableName[] tableNames=hBaseAdmin.listTableNames();
		for(TableName tb:tableNames)
		{
			tName=tb.toString();
			System.out.println(tName);
		}
	}
	
	@Test
	public void testGetTableByRowKey() throws IOException
	{
		System.out.println("-------testGetTableByRowKey--------");
		
		Result rs=hTable.get(rowGet);
		ResultIteratorBySysOut.resultSysOut(rs);
	}
	
	@Test
	public void testGetColumnResult() throws IOException
	{
		System.out.println("-------testGetColumnResult--------");
		
		rowGet.addColumn(MyStringUtil.getBytes("address"),MyStringUtil.getBytes("country"));
		Result rs=hTable.get(rowGet);
		ResultIteratorBySysOut.resultSysOut(rs);
	}
	
	@Test
	public void testGetAllResultByScann() throws IOException
	{
		System.out.println("-------testGetAllResultByScann--------");
		
		for(Result rs:rScanner)
		{
			ResultIteratorBySysOut.resultSysOut(rs);
		}
	}
	
	@Test
	public void testGetResultByScannFilter() throws IOException
	{
		System.out.println("-------testGetResultByScannFilter--------");
		
		scanFilter.addFamily(MyStringUtil.getBytes("address"));
		scanFilter.withStartRow(MyStringUtil.getBytes("country"));
		scanFilter.withStopRow(MyStringUtil.getBytes("city"));

		rScanner=hTable.getScanner(scanFilter);
		
		for(Result rs:rScanner)
		{
			ResultIteratorBySysOut.resultSysOut(rs);
		}
		
	}
	
	@Test
	public void testAHDelete() throws IOException
	{
		if(hBaseAdmin.tableExists(tableName))
		{
			d.addFamily(MyStringUtil.getBytes("address"));
			hTable.delete(d);
			d.addFamily(MyStringUtil.getBytes("info"));
			hTable.delete(d);
		}
	}
	
	@After
	public void Finish() throws IOException
	{
		rScanner.close();
		hBaseAdmin.close();
		hTable.close();
		connection.close();
		
	}
	
	
	
	
	
	

}