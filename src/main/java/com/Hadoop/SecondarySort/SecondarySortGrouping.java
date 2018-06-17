package com.Hadoop.SecondarySort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.Hadoop.Bean.WritableBean.IntPairWritable;

public class SecondarySortGrouping extends WritableComparator
{
	protected SecondarySortGrouping()
	{
        super(IntPairWritable.class, true);
    }

	@Override
	public int compare(WritableComparable a, WritableComparable b)
	{
		IntPairWritable ipw1=(IntPairWritable)a;
		IntPairWritable ipw2=(IntPairWritable)b;
		int l=ipw1.getFirst();
		int r=ipw2.getFirst();
		return l==r?0:(l<r? 1:-1);
	}	
	
}
