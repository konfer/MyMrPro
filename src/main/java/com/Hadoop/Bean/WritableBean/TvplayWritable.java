package com.Hadoop.Bean.WritableBean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class TvplayWritable implements WritableComparable<Object>
{
	private int playNum;
	private int collectedNum;
	private int commentNum;
	private int dissNum;
	private int phraseNum;
	
	public int getPlayNum()
	{
		return playNum;
	}

	public int getCollectedNum()
	{
		return collectedNum;
	}

	public int getCommentNum()
	{
		return commentNum;
	}

	public int getDissNum()
	{
		return dissNum;
	}

	public int getPhraseNum()
	{
		return phraseNum;
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		// TODO Auto-generated method stub
		playNum=in.readInt();
		collectedNum=in.readInt();
		commentNum=in.readInt();
		dissNum=in.readInt();
		phraseNum=in.readInt();
	}
	
	@Override
	public void write(DataOutput out) throws IOException
	{
		// TODO Auto-generated method stub
		out.writeInt(playNum);
		out.writeInt(phraseNum);
		out.writeInt(collectedNum);
		out.writeInt(dissNum);
		out.writeInt(commentNum);
	}
	
	public void set(int playNum,int phraseNum,int collectedNum,int dissNum,int commentNum)
	{
		this.phraseNum=phraseNum;
		this.playNum=playNum;
		this.collectedNum=collectedNum;
		this.dissNum=dissNum;
		this.commentNum=commentNum;
	}
		
	@Override
	public int compareTo(Object o)
	{
		// TODO Auto-generated method stub
		return 0;
	}
}
