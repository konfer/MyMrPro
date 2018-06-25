package com.Hadoop.HBase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HBaseTest 
{
	private Configuration conf ;
	private HTable htable;
	private HBaseAdmin hBaseAdmin;
	private Connection connection;
	
	private TableName tableName;
	private HTableDescriptor table;
	
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
		
	}

	@Test
	public void testCreateTable() throws Exception 
	{
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
		table.addFamily(new HColumnDescriptor("num").setCompressionType(Algorithm.NONE));
		
		hBaseAdmin.createTable(table);

	}
	
	@After
	public void Finish() throws IOException
	{
		hBaseAdmin.close();
		connection.close();
	}
	
	
	
	
	
	

}