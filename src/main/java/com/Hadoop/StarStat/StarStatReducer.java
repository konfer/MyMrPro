package com.Hadoop.StarStat;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StarStatReducer extends Reducer<Text, Text, Text, Text>
{

	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException
	{
		// TODO Auto-generated method stub
		Text reduceText=new Text();
		Text actorText=StarStatCombineMethod.combine(values);
		
		String[] tokens=actorText.toString().split("@");
		
		
		
		context.write(new Text(tokens[0]), new Text(key+" "+tokens[1]));
	
	}

}
