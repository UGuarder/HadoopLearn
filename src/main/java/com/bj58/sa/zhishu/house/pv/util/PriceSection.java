package com.bj58.sa.zhishu.house.pv.util;

import java.util.HashMap;
import java.util.Map;

public class PriceSection {
	public static Map<String , String> map = new HashMap <String , String>();
	
	static {
		map.put("0-600","1");
		map.put("600-1000","2");
		map.put("1000-1500","3");
		map.put("1500-2000","4");
		map.put("2000-3000","5");
		map.put("3000-5000","6");
		map.put("5000-8000","7");
		map.put("8000-9999999999","8");
	}
	
	
	public static String  getPriceSection(String price){
		String res =""; 
		
		if(price ==null ||"".equals(price)||!price.matches("^[0-9]*[1-9][0-9]*$")){
			return null ;
		}
		
		double dprice =Double.parseDouble(price);
		
		for (String key : map.keySet()) {
			String [] ks = key.split("-");
			Double d1 =Double.parseDouble(ks[0]);
			Double d2 =Double.parseDouble(ks[1]); 
			if(d1<dprice&&dprice<=d2){
				res= map.get(key);
				break;
			}
		}
		return res;
	}
	
	
	public static void main(String[] args) {
		System.out.println("1111".matches("^[0-9]*[1-9][0-9]*$"));
	}
}
