package com.Util.HBase;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class ResultIteratorBySysOut {

	public static void resultSysOut(Result rs)
	{
//		for(Cell cell:rs.listCells())
//		{
//			System.out.println("-----");
//			System.out.println(Bytes.toString(cell.getFamilyArray()));
//			System.out.println(Bytes.toString(cell.getQualifierArray()));
//			System.out.println(Bytes.toString(cell.getValueArray()));
//			System.out.println(cell.getTimestamp());
//        }
		
		System.out.println(rs);
		System.out.println("-------------------------------"); 
//		for(KeyValue kv:rs.raw())
//		{
//			System.out.println(new String(kv.getColumn()));
//		}
		
		for (Cell cell : rs.listCells()) 
		{  
			System.out.println("RowKey:" +Bytes.toString(CellUtil.cloneRow(cell)) );  
			System.out.println("family:" +Bytes.toString(CellUtil.cloneFamily(cell)) );  
	        System.out.println("qualifier:" +Bytes.toString(CellUtil.cloneQualifier(cell)) );  
	        System.out.println("value:" +Bytes.toString(CellUtil.cloneValue(cell)) );  
	        System.out.println("-------------------------------");  
	    } 
		
	}
	
}
