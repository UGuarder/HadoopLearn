package com.bj58.sa.zhishu.house.pv.util;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class StatisticsUtils {

	static HashMap<String, String> cateName = new HashMap<String, String>();
	static HashMap<String, String> areaName = new HashMap<String, String>();

	public List<String> id2Name(String idType, String str) {
		String[] strArr = str.split(",");
		List<String> nameList = new ArrayList<String>();
		if (strArr.length < 2) {
			return nameList;
		} else if (strArr.length == 2) {
			if ("cate".equalsIgnoreCase(idType)) {
				String cate1 = cateName.get(strArr[1]);
				cate1 = cate1 == null ? strArr[1] : cate1;
				nameList.add(cate1 + "@" + "A" + "@" + "A");
			} else if ("area".equalsIgnoreCase(idType)) {
				String area1 = areaName.get(strArr[1]);
				area1 = area1 == null ? strArr[1] : area1;
				nameList.add(area1 + "@" + "A" + "@" + "A");
			}

		} else if (strArr.length == 3) {
			if ("cate".equalsIgnoreCase(idType)) {
				String cate1 = cateName.get(strArr[1]);
				cate1 = cate1 == null ? strArr[1] : cate1;
				String cate2 = cateName.get(strArr[2]);
				cate2 = cate2 == null ? strArr[2] : cate2;
				nameList.add(cate1 + "@" + cate2 + "@" + "A");
				nameList.add(cate1 + "@" + "A" + "@" + "A");
			} else if ("area".equalsIgnoreCase(idType)) {
				String area1 = areaName.get(strArr[1]);
				area1 = area1 == null ? strArr[1] : area1;
				String area2 = areaName.get(strArr[2]);
				area2 = area2 == null ? strArr[2] : area2;
				nameList.add(area1 + "@" + area2 + "@" + "A");
				nameList.add(area1 + "@" + "A" + "@" + "A");
			}

		} else {
			if ("cate".equalsIgnoreCase(idType)) {
				String cate1 = cateName.get(strArr[1]);
				cate1 = cate1 == null ? strArr[1] : cate1;
				String cate2 = cateName.get(strArr[2]);
				cate2 = cate2 == null ? strArr[2] : cate2;
				String cate3 = cateName.get(strArr[3]);
				cate3 = cate3 == null ? strArr[3] : cate3;
				nameList.add(cate1 + "@" + cate2 + "@" + cate3);
				nameList.add(cate1 + "@" + cate2 + "@" + "A");
				nameList.add(cate1 + "@" + "A" + "@" + "A");
			} else if ("area".equalsIgnoreCase(idType)) {
				String area1 = areaName.get(strArr[1]);
				area1 = area1 == null ? strArr[1] : area1;
				String area2 = areaName.get(strArr[2]);
				area2 = area2 == null ? strArr[2] : area2;
				String area3 = areaName.get(strArr[3]);
				area3 = area3 == null ? strArr[3] : area3;
				nameList.add(area1 + "@" + area2 + "@" + area3);
				nameList.add(area1 + "@" + area2 + "@" + "A");
				nameList.add(area1 + "@" + "A" + "@" + "A");
			}
		}
		return nameList;
	}

	public static List<String> getDimList(String str) {
		String[] strArr = str.split(",");
		List<String> dimList = new ArrayList<String>();
		if (strArr.length < 2) {
			dimList.add("A" + "@" + "A" + "@" + "A");
			return dimList;
		} else if (strArr.length == 2) {
			String cate1 = strArr[1];
			cate1 = cate1 == null ? strArr[1] : cate1;
			dimList.add(cate1 + "@" + "A" + "@" + "A");
			dimList.add("A" + "@" + "A" + "@" + "A");
		} else if (strArr.length == 3) {
			String cate1 = strArr[1];
			cate1 = cate1 == null ? strArr[1] : cate1;
			String cate2 = strArr[2];
			cate2 = cate2 == null ? strArr[2] : cate2;
			dimList.add(cate1 + "@" + cate2 + "@" + "A");
			dimList.add(cate1 + "@" + "A" + "@" + "A");
			dimList.add("A" + "@" + "A" + "@" + "A");
		} else {
			String cate1 = strArr[1];
			cate1 = cate1 == null ? strArr[1] : cate1;
			String cate2 = strArr[2];
			cate2 = cate2 == null ? strArr[2] : cate2;
			String cate3 = strArr[3];
			cate3 = cate3 == null ? strArr[3] : cate3;
			dimList.add(cate1 + "@" + cate2 + "@" + cate3);
			dimList.add(cate1 + "@" + cate2 + "@" + "A");
			dimList.add(cate1 + "@" + "A" + "@" + "A");
			dimList.add("A" + "@" + "A" + "@" + "A");
		}
		return dimList;
	}
	
	public static List<String> getDimList4Area(String str , String xiaoquId) {
		if(xiaoquId==null||"".equals(xiaoquId)){
			xiaoquId="A";
		}
		
		String[] strArr = str.split(",");
		List<String> dimList = new ArrayList<String>();
		if (strArr.length < 2) {
			dimList.add("A" + "@" + "A" + "@" + "A" + "@" + "A");
			return dimList;
		} else if (strArr.length == 2) {
			String cate1 = strArr[1];
			cate1 = cate1 == null ? strArr[1] : cate1;
			dimList.add(cate1 + "@" + "A" + "@" + "A" + "@" + "A");
			dimList.add("A" + "@" + "A" + "@" + "A" + "@" + "A");
		} else if (strArr.length == 3) {
			String cate1 = strArr[1];
			cate1 = cate1 == null ? strArr[1] : cate1;
			String cate2 = strArr[2];
			cate2 = cate2 == null ? strArr[2] : cate2;
			dimList.add(cate1 + "@" + cate2 + "@" + "A" + "@" +"A");
			dimList.add(cate1 + "@" + "A" + "@" + "A" + "@" + "A");
			dimList.add("A" + "@" + "A" + "@" + "A" + "@" + "A");
		} else if (strArr.length == 4){
			String cate1 = strArr[1];
			cate1 = cate1 == null ? strArr[1] : cate1;
			String cate2 = strArr[2];
			cate2 = cate2 == null ? strArr[2] : cate2;
			String cate3 = strArr[3];
			cate3 = cate3 == null ? strArr[3] : cate3;
			dimList.add(cate1 + "@" + cate2 + "@" + cate3+"@"+ xiaoquId);
			dimList.add(cate1 + "@" + cate2 + "@" + cate3+"@"+ "A");
			dimList.add(cate1 + "@" + cate2 + "@" + "A"+"@"+ "A");
			dimList.add(cate1 + "@" + "A" + "@" + "A"+"@"+ "A");
			dimList.add("A" + "@" + "A" + "@" + "A"+"@"+"A");
		}else{
			String cate1 = strArr[1];
			cate1 = cate1 == null ? strArr[1] : cate1;
			String cate2 = strArr[2];
			cate2 = cate2 == null ? strArr[2] : cate2;
			String cate3 = strArr[3];
			cate3 = cate3 == null ? strArr[3] : cate3;
			dimList.add(cate1 + "@" + cate2 + "@" + cate3+"@"+ xiaoquId);
			dimList.add(cate1 + "@" + cate2 + "@" + cate3+"@"+ "A");
			dimList.add(cate1 + "@" + cate2 + "@" + "A"+"@"+ "A");
			dimList.add(cate1 + "@" + "A" + "@" + "A"+"@"+ "A");
			dimList.add("A" + "@" + "A" + "@" + "A"+"@"+"A");
		}
		return dimList;
	}

	public static Path[] getRecursivePaths(FileSystem fs, String basePath) throws IOException,
			URISyntaxException {
		List<Path> result = new ArrayList<Path>();
		basePath = fs.getUri() + basePath;
		FileStatus[] listStatus = fs.globStatus(new Path(basePath + "/*"));
		for (FileStatus fstat : listStatus) {
			readSubDirectory(fstat, basePath, fs, result);
		}
		return (Path[]) result.toArray(new Path[result.size()]);
	}

	private static void readSubDirectory(FileStatus fileStatus, String basePath, FileSystem fs,
			List<Path> paths) throws IOException, URISyntaxException {
		if (!fileStatus.isDir()) {
			paths.add(fileStatus.getPath());
		} else {
			String subPath = fileStatus.getPath().toString();
			FileStatus[] listStatus = fs.globStatus(new Path(subPath + "/*"));
			if (listStatus.length == 0) {
				paths.add(fileStatus.getPath());
			}
			for (FileStatus fst : listStatus) {
				readSubDirectory(fst, subPath, fs, paths);
			}
		}
	}
	
	public static Boolean ifCatesIsRight(String cates){
		
		if("".equalsIgnoreCase(cates) || cates == null){
			return false ; 
		}
		
		String [] cs = cates.split(",");
		
		if(cs.length<3||!cs[1].equals("1")||(!cs[2].equals("8")&&!cs[2].equals("10"))){
			return true; 
		}else{
			return false ;
		}
	}

	public static void main(String[] args) {
//		for (String item : getDimList("0,1")) {
//			System.out.println(item);
//		}
//		System.out.println("".split("\\.")[0]);
//		System.out.println(new HashSet<String>().size());
//		System.out.println(String.format("%.2f", 0f));
		
		String cates="0,1,8,19";
		
		String [] cs = cates.split(",");
		
		System.out.println(ifCatesIsRight(cates));
		System.out.println(cs.length);
		
	}

}
