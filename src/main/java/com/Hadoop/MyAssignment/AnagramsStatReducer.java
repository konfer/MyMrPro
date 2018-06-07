package com.Hadoop.MyAssignment;

import util.StringTools.IterableSplit;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.sql.Array;

public class AnagramsStatReducer extends Reducer<Text, Text, Text, Text>
{
	private Text outPutKey=new Text();
	private Text outPutValue=new Text();

	@Override protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		String valueSplitedByCommaString = IterableSplit.IterableSplittedByComma(values);
		String[] valueArr=valueSplitedByCommaString.split(",");


		outPutKey.set(key.toString());
		outPutValue.set(valueSplitedByCommaString);
		if(valueArr.length>2)
		{
			context.write(outPutKey,outPutValue);
		}


	}

	public static void main(String[] args)
	{
		String a="---- rusty , yurts ,";
		int m=a.split(",").length;
		System.out.println(m);
	}

}
