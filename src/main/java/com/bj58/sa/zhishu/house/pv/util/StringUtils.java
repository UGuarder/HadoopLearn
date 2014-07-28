package com.bj58.sa.zhishu.house.pv.util;

import java.util.HashMap;
import java.util.Map;

public class StringUtils {
	public static Map < String , String > getString4Map(String value ){
		
		if(value==null||"".equals(value)){
			return null;
		}
		
		String [] fields= value.split("@@");
		Map <String , String>map = new HashMap<String ,String>();
		
		String [] kv ; 
		for(String f: fields){
			kv=f.split(":");
			if(kv.length>1){
				map.put(kv[0], kv[1]);
			}
		}
		
		return map ; 
	}
}
