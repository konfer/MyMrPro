package util.StringTools;

import org.apache.hadoop.hbase.util.Bytes;

public class MyStringUtil {

	public static byte[] getBytes(String str)
	{
		if(str==null)
		{
			str="";
		}
		return Bytes.toBytes(str);
	}
	
}
