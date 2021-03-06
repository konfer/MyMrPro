package com.Hadoop.Bean;

public enum TvPlaySiteEnum
{
	//1优酷2搜狐3土豆4爱奇艺5迅雷看看
	Youku("Youku","1"),
	Sohu("Souhu","2"),
	Tudou("Tudou","3"),
	Iqiyi("Iqiyi","4"),
	Xunlei("Xunlei","5");
	
	private String name;
	private String index;
	
	private TvPlaySiteEnum(String name,String index)
	{
		this.name=name;
		this.index=index;
	}
	
	public static String getName(String index) throws Exception 
	{  
        for (TvPlaySiteEnum c : TvPlaySiteEnum.values()) 
        {  
            if (c.getIndex().equals(index)) 
            {  
                return c.name;  
            }  
        }  
        throw new Exception();
    }

	public void setName(String name)
	{
		this.name = name;
	}

	public void setIndex(String index)
	{
		this.index = index;
	}

	public String getName()
	{
		return name;
	}

	public String getIndex()
	{
		return index;
	}
	
	
    
}
