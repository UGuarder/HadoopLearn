package com.bj58.sa.zhishu.house.pv.util;

import com.bj58.sa.client.constant.Area;
import com.bj58.sa.client.constant.Platform;
import com.bj58.sa.client.constant.Table;
import com.bj58.sa.client.key.KeyGenerator;
import com.bj58.sa.client.key.KeyUtils;

public class KeyUtil {
	public static Long getKey(String [] fields , String date  , Table table  ){
		Long kLon=0l;
		try {
			String platform =fields[4];
			Platform p = null; 
			if("PC".equals(platform)){
				p=Platform.PC;
			}else{
				p=Platform.WL;
			}
			
			String xiaoquId =fields[3];
			String busId=fields[2];
			String areaId=fields[1];
			String cityId=fields[0];
			//调用生成key服务
			if("A".equals(xiaoquId)){
				if("A".equals(busId)){
					if("A".equals(areaId)){
						if("A".equals(cityId)){
							//do noting
						}else{
							kLon=	KeyGenerator.getInstance().getKeyByTable(table, Area.city, p, Integer.parseInt(cityId), new Integer (date));
						}
					}else{
						kLon=KeyGenerator.getInstance().getKeyByTable(table, Area.area, p,Integer.parseInt(areaId),  new Integer (date));
					}
				}else{
					kLon=KeyGenerator.getInstance().getKeyByTable(table, Area.shangquan, p, Integer.parseInt(busId),  new Integer (date));
				}
			}else{
				if(xiaoquId==null||!xiaoquId.matches("^[0-9]*[1-9][0-9]*$")){
					return 0L;
				}
				kLon=KeyGenerator.getInstance().getKeyByTable(table, Area.xiaoqu, p, Integer.parseInt(xiaoquId),  new Integer (date));
			}
		} catch (Exception e) {
			return 0L;
		}
		
		return kLon ;
	}
	 
	public static void main(String[] args) {
		
//		Long kLon=KeyGenerator.getInstance().getKeyByTable(Table.house_pv_effect, Area.xiaoqu, Platform.PC, Integer.parseInt("25"), new Integer ("0"));
//		
//		
		String s= KeyUtils.getKeyInfo(234244827942748160l);
//		
//		
		System.out.println(s);
//		
//		System.out.println(kLon);
		
		String [] fields= {"1"  , "A"  , "A" , "A" , "WL" };
		
		long k =KeyUtil.getKey(fields, "201407" , Table.house_pv_hour);
		
		System.out.println(k);
	}
}
