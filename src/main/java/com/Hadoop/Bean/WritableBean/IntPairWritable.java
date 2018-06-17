package com.Hadoop.Bean.WritableBean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class IntPairWritable implements WritableComparable<IntPairWritable>
{
	private int first;
	private int second;
	
	public void set(int left,int right)
	{
		this.first=left;
		this.second=right;
	}
	
	public int getFirst()
	{
		return first;
	}

	public int getSecond()
	{
		return second;
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		// TODO Auto-generated method stub
		first=in.readInt();
		second=in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		// TODO Auto-generated method stub
		out.writeInt(first);
		out.writeInt(second);
	}

	@Override
	public int compareTo(IntPairWritable o)
	{
		// TODO Auto-generated method stub
		if(first != o.first)
		{
			return first<o.first?1:-1;
		}
		else if(second!=o.second)
		{
			return second<o.second?-1:1;
		}
		else 
		{
			return 0;
		}
	}

	@Override
	public int hashCode()
	{
		// TODO Auto-generated method stub
		return first * 157 + second;
	}

	@Override
	public boolean equals(Object right)
	{
		// TODO Auto-generated method stub
		if(right==null)
		{
			return false;
		}
		if(this==right)
		{
			return true;
		}
		if(right instanceof IntPairWritable)
		{
			IntPairWritable r=(IntPairWritable)right;
			return r.first==this.first&&r.second==this.second;
		}
		else
		{
			return false;
		}
	}


}
