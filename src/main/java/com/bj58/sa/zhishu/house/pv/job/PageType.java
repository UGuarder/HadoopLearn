package com.bj58.sa.zhishu.house.pv.job;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.bj58.data.track.utility.format.TrackInputFormat;
import com.bj58.data.track.utility.info.TrackInfo;
import com.bj58.data.track.utility.tool.TrackJobUtils;
import com.bj58.data.track.utility.tool.TrackObjectFormat;
import com.bj58.data.track.utility.tool.TrackSplitUtils;
import com.bj58.sa.zhishu.house.pv.util.MyDateUtil;
import com.bj58.sa.zhishu.house.pv.util.StatisticsUtils;

public class PageType {

	public static class M1 extends Mapper<LongWritable, Text, Text, Text> {

		private Text oKey = new Text();
		private Text oVal = new Text();
		TrackSplitUtils trackSplit = TrackSplitUtils.getInstance();
		static Map<String, String> trackURLMap = new HashMap<String, String>();
		//		Pattern urlRe = Pattern.compile("(?<=https?://)[^/]+(/[^/]+)?");
		TrackInfo trackinfo = null;

		public void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			List<List<String>> listLines = trackSplit.getSplitVisit(value);// 按cookid和time来划分
			for (List<String> listLine : listLines) { // 某个cookid的所有session，迭代session（visit）
				Set<String> outKeySet = new HashSet<String>();
				int count = 0;
				String cookieId = "E";
				//  1 表示新访客
				int newVisitFlag = 0;
				for (String line : listLine) {// 某个session，也叫 visit，包含很多记录
					// 时间 	类别 	城市 	页面类型	访问次数   独立访客数   浏览量    人均浏览量    
					// 跳出率:  整个 session 中只访问一个页面的session占比
					// 新访客比例: GA 参数的最后一位等于 1，则是新访客
					count++;
					trackinfo = TrackObjectFormat.getTrackinfoByString(line);
					trackURLMap = TrackJobUtils.analysisTrackUrl(trackinfo.getTkTrackURL());// 公共参数Map
					cookieId = trackinfo.getTkCookieID();
					String cates = trackinfo.getTkCatePath();
					String areas = trackinfo.getTkAreaPath();
					String pageType = trackinfo.getTkPageType();
					// 如果为空将被归类到 NULL 类型去
					if ("".equalsIgnoreCase(pageType) || pageType == null) {
						pageType = "Null";
					}
					String[] visitCountArr = trackinfo.getTkGautma().split("\\.");
					if ("1".equalsIgnoreCase(visitCountArr[visitCountArr.length - 1])) {
						newVisitFlag = 1;
					}
					
					if(StatisticsUtils.ifCatesIsRight(cates)){
						continue;
					}

					for (String tmpCate : StatisticsUtils.getDimList(cates)) {
						for (String tmpArea : StatisticsUtils.getDimList(areas)) {
							for (String pageTypeTmp : new String[] { "A", pageType }) {
								// 访问次数visit  独立访客数uv  浏览量pv  跳出率/跳出次数  新访客比例/新访客数	||	人均浏览量
								String outKey = tmpCate + "@" + tmpArea + "@" + pageTypeTmp;
								outKeySet.add(outKey);
								String outValue = "0|E|1|0|0";
								oKey.set(outKey);
								oVal.set(outValue);
								context.write(oKey, oVal);
							}
						}
					}
				}
				// 一个session/visit处理完毕
				for (String outKey : outKeySet) {
					count = count == 1 ? 1 : 0;
					//					String newVisit = newVisitFlag == 1 ? cookieId : "E";
					int newVisit = newVisitFlag == 1 ? 1 : 0;
					String outValue = "1|" + cookieId + "|" + "0|" + count + "|" + newVisit;
					oKey.set(outKey);
					oVal.set(outValue);
					context.write(oKey, oVal);
				}
			}
			// map即将结束：
		}
	}

	static HashMap<String, String> cateName = new HashMap<String, String>();
	static HashMap<String, String> areaName = new HashMap<String, String>();
	
	public static class R1 extends Reducer<Text, Text, Text, Text> {
		Text oKey = new Text();
		Text oVal = new Text();
		String DATE;
		final String FS = "\t";

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			BufferedReader in = null;
			Text oKey = new Text();
			Text oVal = new Text();

			try {
				// 从当前作业中获取要缓存的文件
				Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				String line = null;
				String[] strArr = null;
				for (Path path : paths) {
					if (path.toString().contains("cmc_diplocal")) {
						in = new BufferedReader((new InputStreamReader(new FileInputStream(path.toString()),
								"utf-8")));
						while (null != (line = in.readLine())) {
							//							line = new String(line.getBytes("utf-8"), "utf-8");
							strArr = line.split("\t", -1);
							if (strArr.length < 3) {
								continue;
							}
							// 存放 pid=value 与 value name 的映射关系
							areaName.put(strArr[0], strArr[2]);
						}
					} else if (path.toString().contains("cmc_dispcategory")) {

						in = new BufferedReader((new InputStreamReader(new FileInputStream(path.toString()),
								"utf-8")));
						while (null != (line = in.readLine())) {
							//							line = new String(line.getBytes("utf-8"), "utf-8");
							strArr = line.split("\t", -1);
							if (strArr.length < 3) {
								continue;
							}
							// 存放 pid=value 与 value name 的映射关系
							cateName.put(strArr[0], strArr[2]);
						}
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					if (in != null) {
						in.close();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException {

			String[] keyArr = key.toString().split("@");
			String cateName1 = cateName.get(keyArr[0]) == null ? keyArr[0] : cateName.get(keyArr[0]);
			keyArr[0] = "A".equalsIgnoreCase(keyArr[0]) ? "A" : cateName1;

			String cateName2 = cateName.get(keyArr[1]) == null ? keyArr[1] : cateName.get(keyArr[1]);
			keyArr[1] = "A".equalsIgnoreCase(keyArr[1]) ? "A" : cateName2;

			String cateName3 = cateName.get(keyArr[2]) == null ? keyArr[2] : cateName.get(keyArr[2]);
			keyArr[2] = "A".equalsIgnoreCase(keyArr[2]) ? "A" : cateName3;

			String areaName1 = areaName.get(keyArr[3]) == null ? keyArr[3] : areaName.get(keyArr[3]);
			keyArr[3] = "A".equalsIgnoreCase(keyArr[3]) ? "A" : areaName1;

			String areaName2 = areaName.get(keyArr[4]) == null ? keyArr[4] : areaName.get(keyArr[4]);
			keyArr[4] = "A".equalsIgnoreCase(keyArr[4]) ? "A" : areaName2;

			String areaName3 = areaName.get(keyArr[5]) == null ? keyArr[5] : areaName.get(keyArr[5]);
			keyArr[5] = "A".equalsIgnoreCase(keyArr[5]) ? "A" : areaName3;
			
			

			String outKey = cateName1 + FS + cateName2 + FS + cateName3 + FS + areaName1 + FS + areaName2
					+ FS + areaName3 + FS + keyArr[6];

			if (DATE == null) {
				DATE = context.getConfiguration().get("DATE");
			}
			Set<String> cookieIdSet = new HashSet<String>();
			//			Set<String> newVisitSet = new HashSet<String>();
			long visit = 0l;
			long uv = 0l;
			long pv = 0l;
			long bounce = 0l;
			long newVisit = 0l;

			for (Text val : values) {
				String[] valArr = val.toString().split("\\|");
				visit += Integer.parseInt(valArr[0]);
				if (!"E".equalsIgnoreCase(valArr[1])) {
					cookieIdSet.add(valArr[1]);
				}
				pv += Integer.parseInt(valArr[2]);
				bounce += Integer.parseInt(valArr[3]);
				/*if(!"E".equalsIgnoreCase(valArr[4])){
					newVisitSet.add(valArr[4]);
				}*/
				newVisit += Integer.parseInt(valArr[4]);

			}

			uv = cookieIdSet.size() == 0 ? 1 : cookieIdSet.size();
			visit = visit == 0 ? 1 : visit;
			String bounceRate = String.format("%.2f", ((float) bounce / visit) * 100);
			//			String newVisitRate = String.format("%.2f", ((float) newVisitSet.size() / uv) * 100);
			String newVisitRate = String.format("%.2f", ((float) newVisit / visit) * 100);
			String pvAvg = String.format("%.2f", (float) pv / uv);
			// 访问次数visit 	独立访客数uv 	浏览量pv 	人均浏览量 	跳出率	新访客比例
			// 20140506_A_A_A_A_A_A_A  10161838_7988623_110,485,825_13.83_21.58_47.34   (uv计算新访客)
			// 20140506_A_A_A_A_A_A_A  10161838_7988623_110485825_13.83_21.58_39.68		(visit计算新访客)

			oKey.set(DATE + FS + "PC" + FS + outKey);
			oVal.set(visit + FS + uv + FS + pv + FS + pvAvg + FS + bounceRate + FS + newVisitRate);
			context.write(oKey, oVal);
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException,
			URISyntaxException {

		Configuration conf = new Configuration();
		conf.set("mapred.job.queue.name", "regular"); // default,regular,realtime
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		int exitCode = 127;

		if (otherArgs.length != 1) {
			System.out.println("************************************************************");
			System.out.println("************************************************************");
			System.out.println("Usage: please input 1 params, for example: file.jar args[0]");
			System.out.println("args[0] is dateList: 20130101,20130106 or 20130106");
			System.out.println("************************************************************");
			System.out.println("************************************************************");
			System.exit(exitCode);
		}

		String startDate = otherArgs[0].split(",")[0];
		String endDate = otherArgs[0].split(",").length == 2 ? otherArgs[0].split(",")[1] : startDate;

		String baseInPath01 = "/dsap/rawdata/track.58.com";
		String baseOutPath = "/dsap/middata/infostat/shx_pagetype";
		
		String baseInPath3 = "/dsap/rawdata/cmc_diplocal";
		String baseInPath4 = "/dsap/rawdata/cmc_dispcategory";

		// 例行调度：昨天的日期
		for (String runDate : MyDateUtil.getDateList(startDate, endDate, 0)) {
			conf.set("DATE", runDate);
			Job job = new Job(conf, "shx_pagetype");
			job.setInputFormatClass(TrackInputFormat.class);
			job.setJarByClass(PageType.class);
			job.setMapperClass(M1.class);
			job.setReducerClass(R1.class);
			job.setNumReduceTasks(60);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			// 加载 track.58.com
			FileInputFormat.addInputPath(job, new Path(baseInPath01 + "/" + runDate));

			// 加载 cmc 表现地域
			String baseInPath2 = baseInPath3 + "/" + runDate;
			FileSystem hdfs1 = FileSystem.get(conf);
			FileStatus[] fileList = hdfs1.listStatus(new Path(baseInPath2));
			for (int i = 0; i < fileList.length; i++) {
				String fileName = fileList[i].getPath().getName();
				if (fileName.contains("part")) {
					System.out.println("----------------->>> " + baseInPath2 + "/" + fileName);
					String inPath2Link = new Path(baseInPath2 + "/" + fileName).toUri().toString() + "#"
							+ "cmc_diplocal" + i;
					DistributedCache.addCacheFile(new URI(inPath2Link), job.getConfiguration());
				}
			}

			// 加载 cmc 表现分类
			baseInPath2 = baseInPath4 + "/" + runDate;
			fileList = hdfs1.listStatus(new Path(baseInPath2));
			for (int i = 0; i < fileList.length; i++) {
				String fileName = fileList[i].getPath().getName();
				if (fileName.contains("part")) {
					System.out.println("----------------->>> " + baseInPath2 + "/" + fileName);
					String inPath2Link = new Path(baseInPath2 + "/" + fileName).toUri().toString() + "#"
							+ "cmc_dispcategory" + i;
					DistributedCache.addCacheFile(new URI(inPath2Link), job.getConfiguration());
				}
			}
			hdfs1.close();

			String outPath = baseOutPath + "/" + runDate;
			FileSystem.get(conf).delete(new Path(outPath), true);
			FileOutputFormat.setOutputPath(job, new Path(outPath));
			exitCode = job.waitForCompletion(true) ? 0 : 1;
			System.out.println("--------------------------------------------END1");

		}
		System.exit(exitCode);

	}

}
