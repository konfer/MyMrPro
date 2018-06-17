package com.Hadoop.StarStat;

import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class StarStatCombiner extends Reducer<Text, Text, Text, Text>
{
	
	@Override
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException
	{
		Text actorInfo=new Text();
		actorInfo=StarStatCombineMethod.combine(values);
//		// TODO Auto-generated method stub
//		Text text=new Text();
//		
//		int maxHotIndex=Integer.MIN_VALUE;
//		int hotIndex=0;
//		String maxIndexActorName="";
//		
//		for(Text value:values)
//		{
//			String[] tokens=value.toString().split("@");
//			maxHotIndex=Integer.parseInt(tokens[1]);
//			if(hotIndex>maxHotIndex)
//			{
//				hotIndex=maxHotIndex;
//				maxIndexActorName=tokens[0];
//			}
//		}
//		text.set(maxIndexActorName+"@"+maxHotIndex);
		System.out.println(actorInfo);
		
		
		context.write(key, actorInfo);
	}
	
//	private Text text = new Text();
//	
//	@Override
//	public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException 
//	{
//		int maxHotIndex = Integer.MIN_VALUE;
//		int hotIndex = 0;
//		String name="";
//		
//		for (Text val : values) 
//		{
//			String[] valTokens = val.toString().split("@");
//			hotIndex = Integer.parseInt(valTokens[1]);
//			if(hotIndex>maxHotIndex)
//			{
//				name = valTokens[0];
//				maxHotIndex = hotIndex;
//			}
//		}
//		text.set(name+"\t"+maxHotIndex);
//		context.write(key, text);
//	}

}
