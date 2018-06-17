package com.Hadoop.StarStat;

import org.apache.hadoop.io.Text;

public class StarStatCombineMethod
{
	private static Text text=new Text();
	
	public static Text combine(Iterable<Text> values)
	{
		//  male  李易峰  32670
		//        aa  32671
		//        bb  32673
	
		int maxHotIndex=Integer.MIN_VALUE;
		int hotIndex=0;
		String maxIndexActorName="";
		
		for(Text value:values)
		{
			String[] tokens=value.toString().split("@");
			hotIndex=Integer.parseInt(tokens[1]);
			if(hotIndex>maxHotIndex)
			{
				maxHotIndex=hotIndex;
				maxIndexActorName=tokens[0];
			}
		}
		text.set(maxIndexActorName+"@"+maxHotIndex);
		
		return text;
	}
	
}
 