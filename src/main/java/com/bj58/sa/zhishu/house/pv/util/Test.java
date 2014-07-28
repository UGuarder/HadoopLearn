package com.bj58.sa.zhishu.house.pv.util;

import java.util.ArrayList;
import java.util.List;


public class Test{
	public String[] getAllMonths(String start, String end){
		String splitSign="-";
		String regex="\\d{4}"+splitSign+"(([0][1-9])|([1][012]))"; //判断YYYY-MM时间格式的正则表达式
		if(!start.matches(regex) || !end.matches(regex)) return new String[0];
		
		List<String> list=new ArrayList<String>();
		if(start.compareTo(end)>0){
			//start大于end日期时，互换
			String temp=start;
			start=end;
			end=temp;
		}
		
		String temp=start; //从最小月份开始
		while(temp.compareTo(start)>=0 && temp.compareTo(end)<0){
			list.add(temp); //首先加上最小月份,接着计算下一个月份
			String[] arr=temp.split(splitSign);
			int year=Integer.valueOf(arr[0]);
			int month=Integer.valueOf(arr[1])+1;
			if(month>12){
				month=1;
				year++;
			}
			if(month<10){//补0操作
				temp=year+splitSign+"0"+month;
			}else{
				temp=year+splitSign+month;
			}
		}
		list.add(end);
		int size=list.size();
		
		String[] result=new String[size]; 
		for(int i=0;i<size;i++){
			result[i]=list.get(i);
		}
		return result;
	}
	
	
	public static void main(String[] args) {
		String start="2010-07";
		String end="2010-07";
		Test te=new Test();
		String[] result=te.getAllMonths(start, end);
		for (String str : result) {
			System.out.println(str);
		}
	}
}
