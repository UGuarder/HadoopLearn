package com.bj58.sa.zhishu.house.pv.util;

public class UrlUtil {
	public static String getInfoIdByTrackUrl(String url){
		
		String infoId  ;
		
		if(url==null||"".equals(url)){
			infoId="0";
		}
		
		if(url.indexOf("x.shtml?")>0){
			infoId=url.substring(0 , url.indexOf("x.shtml?"));
			infoId=infoId.substring(infoId.lastIndexOf("/")+1);
		}else{
			infoId="0";
		}
		return infoId;
	}
	
	public static void main(String[] args) {
		String url ="http://bj.58.com/hezu/18324374758793x.shtml?PGTID=14031634490770.032368643674999475&ClickID=1";
		String i = getInfoIdByTrackUrl(url);
		System.out.println(i);
	}
}
