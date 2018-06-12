package com.Hadoop.TvPlay;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.Hadoop.Bean.TvPlaySiteEnum;
import com.Hadoop.Bean.WritableBean.TvplayWritable;

public class TvPlayReducer extends Reducer<Text, TvplayWritable, Text, Text>
{
	private MultipleOutputs<Text, Text> mos;
	private Text tvPlayKey=new Text();
	private Text tvPlayValue=new Text();

	@Override
	protected void setup(Reducer<Text, TvplayWritable, Text, Text>.Context context) throws IOException, InterruptedException
	{
		// TODO Auto-generated method stub
		mos = new MultipleOutputs<Text, Text>(context);
	}

	@Override
	protected void reduce(Text key, Iterable<TvplayWritable> values, Reducer<Text, TvplayWritable, Text, Text>.Context context)
			throws IOException, InterruptedException
	{
		// TODO Auto-generated method stub
		
		int totalPlayNum=0;
		int totalCollectedNum=0;
		int totalCommentNum=0;
		int totalDissNum=0;
		int totalPhraseNum=0;
		
		for(TvplayWritable tvPlayRecord:values)
		{
			totalPlayNum+=tvPlayRecord.getPlayNum();
			totalCollectedNum=tvPlayRecord.getCollectedNum();
			totalCommentNum=tvPlayRecord.getCommentNum();
			totalDissNum=tvPlayRecord.getDissNum();
			totalPhraseNum=tvPlayRecord.getPhraseNum();
		}
		
		String[] records = key.toString().split("\t");
		tvPlayKey.set(records[0]);
		tvPlayValue.set(totalPlayNum+"\t"+totalCollectedNum+"\t"+totalCommentNum+"\t"+totalDissNum+"\t"+totalPhraseNum);
		
		mos.write(TvPlaySiteEnum.getName(records[1]), tvPlayKey, tvPlayValue);
		
	}
	
	@Override
	protected void cleanup(Reducer<Text, TvplayWritable, Text, Text>.Context context) throws IOException, InterruptedException
	{
		// TODO Auto-generated method stub
		mos.close();
	}
	

}
