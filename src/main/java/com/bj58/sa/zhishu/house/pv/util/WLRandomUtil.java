package com.bj58.sa.zhishu.house.pv.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class WLRandomUtil {
	static Map < String , String > map = new HashMap<String,String>();
	
	static{
		map.put("201301","0.55~0.65");
		map.put("201302","0.60~0.65");
		map.put("201303","0.55~0.65");
		map.put("201304","0.65~0.70");
		map.put("201305","0.65~0.70");
		map.put("201306","0.60~0.65");
		map.put("201307","0.55~0.65");
		map.put("201308","0.60~0.65");
		map.put("201309","0.65~0.70");
		map.put("201310","0.80~0.90");
		map.put("201311","0.80~0.90");
		map.put("201312","0.90~1.00");
		map.put("201401","1.00~1.10");
		map.put("201402","1.10~1.15");
		map.put("201403","1.00~1.10");
		map.put("201404","1.00~1.10");
		map.put("201405","1.10~1.15");
		map.put("201406","1.10~1.15");
		map.put("201407","1.10~1.15");
	}
	
	public static Double getRandom(String key ){
		String temp =map.get(key);
		
		if(temp==null){
			temp="1.10~1.15";
		}
		
		double min = Double.parseDouble(temp.split("~")[0]);
		double max = Double.parseDouble(temp.split("~")[1]);
		
		min=min*100.0;
		max=max*100.0;
		
		int minInt = (int)min;
		int maxInt =(int)max;
		
		Random rand = new Random();  
		Integer ran =rand.nextInt(maxInt-minInt+2)+minInt;
		
		Double res = Double.parseDouble(ran.toString())/100.0;
		return res;
	}
	
	public static void main(String[] args) {
		double temp=100*WLRandomUtil.getRandom("201308");
		int tempInt = (int)temp;
		System.out.println(tempInt);
	}
}
