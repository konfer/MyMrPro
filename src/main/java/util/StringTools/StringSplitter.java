package util.StringTools;

public class StringSplitter
{
	public static String[] tabSplitter(String str)
	{
		//String tvPlayLine="继承者们	1	4105447	202	844	48	671";
		String[] pieces=str.split("\\s+");
		for(String s:pieces)
		{
			System.out.println(s);
		}
			
		return pieces;
	}
}
