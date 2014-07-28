package com.bj58.sa.zhishu.house.pv.util;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale; 
import java.util.TimeZone;

public class MyDateUtil {

	public static String TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
	public static String DATE_FORMAT = "yyyyMMdd";
	
	private static  SimpleDateFormat mouthSdf =new  SimpleDateFormat("yyyyMM");
	private  static SimpleDateFormat daySdf =new  SimpleDateFormat("yyyyMMdd");

	// 计算起始日期间隔多少天
	public static String getDayInterval(String st, String ed) {
		SimpleDateFormat myFormatter = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss");
		long day = 0;
		try {
			java.util.Date startDate = myFormatter.parse(st);
			java.util.Date endDate = myFormatter.parse(ed);
			day = (endDate.getTime() - startDate.getTime())
					/ (24 * 60 * 60 * 1000);
		} catch (Exception e) {
			return "";
		}
		return day + "";
	}

	// 判断起始时间差是否在多少分钟内
	public static boolean getMinitueInterval(String st, String ed, int mins) {
		SimpleDateFormat myFormatter = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss");
		boolean inMins = false;
		try {
			java.util.Date startDate = myFormatter.parse(st);
			java.util.Date endDate = myFormatter.parse(ed);
			double minitue = (endDate.getTime() * 1.0 - startDate.getTime())
					/ (60 * 1000);
			System.out.println(minitue);
			if (minitue >= 0 && minitue <= mins) {
				inMins = true;
			} else {
				inMins = false;
			}

		} catch (Exception e) {
			inMins = false;
		}
		return inMins;
	}

	// 指定日期 N 天以前的日期
	public static String getNDaysAgo(String date, int nDaysAgo) {
		Calendar cal1 = Calendar.getInstance();
		Date dateFormat = null;
		try {
			dateFormat = new SimpleDateFormat(DATE_FORMAT).parse(date);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		Long dateLong = dateFormat.getTime(); // 得到的是 long，类似 date
												// -d"2012-11-12 12:00:00" +%s
		cal1.setTime(new java.util.Date(dateLong));
		cal1.add(Calendar.DATE, -nDaysAgo);
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
		return formatter.format(cal1.getTime());
	}

	// 得到起始范围内的日期列表，包含起始日期，可设置间隔天数
	public static List<String> getDateList(String startDate, String endDate,
			int intervalDay) {
		List<String> listDate = new ArrayList<String>();

		SimpleDateFormat f = new SimpleDateFormat("yyyyMMdd");
		Date dBegin;
		Date dEnd;
		try {
			dBegin = f.parse(startDate);
			dEnd = f.parse(endDate);
			if (dBegin.getTime() <= dEnd.getTime()) {
				for (long i = dBegin.getTime(); i <= dEnd.getTime(); i += 86400000 * (intervalDay + 1)) {
					Date d = new Date(i);
					String date = f.format(d);
					// System.out.println(date);
					listDate.add(date);
				}
			} else {
				for (long i = dBegin.getTime(); i >= dEnd.getTime(); i -= 86400000 * (intervalDay + 1)) {
					Date d = new Date(i);
					String date = f.format(d);
					// System.out.println(date);
					listDate.add(date);
				}
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return listDate;
	}

	// 获取指定日期的前一周（1~7天）时间
	public static List<String> getWeekList(String startDate) {
		List<String> listDate = new ArrayList<String>();

		SimpleDateFormat f = new SimpleDateFormat("yyyyMMdd");
		Date dBegin;
		try {
			dBegin = f.parse(startDate);
			for (long i = 1; i <= 7; i++) {
				Date d = new Date(dBegin.getTime() - 86400000 * i);
				String date = f.format(d);
				// System.out.println(date);
				listDate.add(date);
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return listDate;
	}

	// 将长整型字符串转换为日期字符串，注意：不能带毫秒
	public static String millis2Time(String longStr) {
		long seconds = Long.parseLong(longStr);
		long millis = seconds * 1000;
		Date date = new Date(millis);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",
				Locale.CHINA);
		sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"));
		String formattedDate = sdf.format(date);
		return formattedDate;
	}

	// 将日期字符串转换为长整型数字，注意：输出不带毫秒
	public static long time2Millis(String date) {
		Date dateFormat = null;
		try {
			dateFormat = new SimpleDateFormat(TIME_FORMAT).parse(date);
		} catch (ParseException e) {
			System.out.println(e.toString());
		}
		Long dateLong = dateFormat.getTime(); // 得到的是 long，类似 date
												// -d"2012-11-12 12:00:00" +%s
		return dateLong;
	}

	// 得到指定日期的星期数，注意：1=星期日 2=星期一 7=星期六，其他类推
	public static String getWeek(String sdate) {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
		ParsePosition pos = new ParsePosition(0);
		Date strtodate = formatter.parse(sdate, pos);
		// Date date = strToDate(sdate);
		Calendar c = Calendar.getInstance();
		c.setTime(strtodate);
		// int hour=c.get(Calendar.DAY_OF_WEEK);
		// hour中存的就是星期几了，其范围 1~7
		// 1=星期日 2=星期一 7=星期六，其他类推
		// return new SimpleDateFormat("E").format(c.getTime()); // 返回 星期一
		return String.valueOf(c.get(Calendar.DAY_OF_WEEK));
	}

	// 得到 1 年以前的日期
	public static String getOneYearsAgo(String dateStr) {
		SimpleDateFormat f = new SimpleDateFormat("yyyyMMdd");
		Calendar cal = Calendar.getInstance();
		try {
			cal.setTime(f.parse(dateStr));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		cal.add(Calendar.YEAR, -1);
		Date oneYearsAgo = cal.getTime();
		return f.format(oneYearsAgo);
	}

	// 得到 N 个月以前的日期
	public static String getNMonthsAgo(String dateStr, int n) {
		SimpleDateFormat f = new SimpleDateFormat("yyyyMMdd");
		Calendar cal = Calendar.getInstance();
		try {
			cal.setTime(f.parse(dateStr));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		cal.add(Calendar.MONTH, n);
		Date nMonthsAgo = cal.getTime();
		return f.format(nMonthsAgo);
	}

	// 获得上月最后一天的日期
	public static String getPreMonthEnd(String dateStr) {
		String str = "";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		Calendar lastDate = Calendar.getInstance();
		try {
			lastDate.setTime(sdf.parse(dateStr));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		lastDate.add(Calendar.MONTH, -1);// 减一个月
		lastDate.set(Calendar.DATE, 1);// 把日期设置为当月第一天
		lastDate.roll(Calendar.DATE, -1);// 日期回滚一天，也就是本月最后一天
		str = sdf.format(lastDate.getTime());
		return str;
	}

	// 获得上月最第一天的日期
	public static String getPreMonthFirst(String dateStr) {
		String str = "";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		Calendar lastDate = Calendar.getInstance();
		try {
			lastDate.setTime(sdf.parse(dateStr));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		lastDate.set(Calendar.DATE, 1);// 设为当前月的1号
		lastDate.add(Calendar.MONTH, -1);// 减一个月，变为下月的1号
		str = sdf.format(lastDate.getTime());
		return str;
	}

	public static String getCurDate(String d ){
		
		Double dd = Double.parseDouble(d);
		
		long l = (long) (dd* 1000);
		
		SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd HH");  
		
		Date da=new Date(l);
		
		return sdf.format(da);  
	}
	/**
	  * 求某年月的最后一天 
	  *
	  * @param year
	  * @param month
	  * @return Date
	 * @throws ParseException 
	  */
	 public static String getCurrMonthLast(String ym) throws ParseException {
		 Date date = new Date ();
		 String dStr = mouthSdf.format(date);
		 if(dStr.equals(ym)){
			 return getSpecifiedDayBefore(date);
		 }
		 
		 int year = Integer.parseInt(ym.substring(0,4));
		 int month= Integer.parseInt(ym.substring(4));
		 
		 Calendar calendar = Calendar.getInstance();
         calendar.clear();
         calendar.set(Calendar.YEAR, year);
         calendar.set(Calendar.MONTH, month-1);
         calendar.roll(Calendar.DAY_OF_MONTH, -1);//向指定日历字段添加指定（有符号的）时间量，不更改更大的字段。负的时间量意味着向下滚动。 
         Date currMonthLast = calendar.getTime();
         
         String  s=daySdf.format(currMonthLast);       
		 return s;
	 }
	 
	 /** 
	     * 获得指定日期的前一天 
	     *  
	     * @param specifiedDay 
	     * @return 
	 * @throws ParseException 
	     * @throws Exception 
	     */  
	    public static String getSpecifiedDayBefore( Date date) throws ParseException {//可以用new Date().toLocalString()传递参数  
	        Calendar c = Calendar.getInstance();  
	        c.setTime(date);  
	        int day = c.get(Calendar.DATE);  
	        c.set(Calendar.DATE, day - 1);  
	        String dayBefore = daySdf.format(c   .getTime());  
	        return dayBefore;  
	    }  
	    /** 
	     * 获取两个时间的月份
	     *  
	     * @param specifiedDay 
	     * @return 
	 * @throws ParseException 
	     * @throws Exception 
	     */  
	    public static String[] getAllMonths(String start, String end) throws ParseException{
	    	SimpleDateFormat sdf=new SimpleDateFormat("yyyyMM");  
	    	SimpleDateFormat sdf1=new SimpleDateFormat("yyyy-MM");  
	    	
	    	Date s =sdf.parse(start);
	    	Date e =sdf.parse(end);
	    	
	    	start=sdf1.format(s);
	    	end=sdf1.format(e);
	    	
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
				Date s1 = sdf1.parse(list.get(i));
				String str= sdf.format(s1);
				result[i]=str;
			}
			return result;
		}
	
	public static void main(String[] args) throws ParseException {
		
		String start="201006";
		String end="201007";
		String[] result=MyDateUtil.getAllMonths(start, end);
		for (String str : result) {
			System.out.println(str);
		}
	}
}
