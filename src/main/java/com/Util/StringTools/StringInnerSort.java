package com.Util.StringTools;

import java.util.Arrays;

//
public class StringInnerSort
{
	public static String StringSort(String str)
	{
		char[] strArr = str.toCharArray();
		Arrays.sort(strArr);
		String sortResultString = new String(strArr);
		return sortResultString;
	}

	public static void main(String[] args)
	{
		String str="sfdjksfjdk";
		String sortedString=StringSort(str);
		System.out.println(sortedString);

	}
}
