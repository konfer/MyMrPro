package util.Hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class RegexAcceptPathFilter implements PathFilter
{
	private final String regex;
	private Boolean isAccept=true;

	public RegexAcceptPathFilter(String regex) {
		this.regex = regex;
	}
	
	public RegexAcceptPathFilter(String regex,boolean isAccept) {
		this.regex = regex;
		this.isAccept=isAccept;
	}

	@Override
	public boolean accept(Path path) {
		// TODO Auto-generated method stub
		boolean flag = path.toString().matches(regex);
		//只接受 regex 格式的文件
		if(isAccept)
		{
			return flag;
		}
		else
		{
			return !flag;
		}
	}
}
