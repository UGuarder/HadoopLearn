package com.bj58.sa.zhishu.house.pv.job;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
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
import com.bj58.sa.zhishu.house.pv.util.PriceSection;
import com.bj58.sa.zhishu.house.pv.util.StatisticsUtils;
import com.bj58.sa.zhishu.house.pv.util.StringUtils;
import com.bj58.sa.zhishu.house.pv.util.WLRandomUtil;
/**
 * desc :  计算从20130101 至当天每小时的 租房和合租房详情页的 pv数据
 * input : /dsap/rawdata/track.58.com
 * output:/dsap/middata/infostat/shx_zufang_pv
 * outputformat:
 * 	cateID   areaId  xiaoquId  priceSection_2(价格区间)或者huxing_1(户型)  platform  (wl, pc)  访问次数visit  独立访客数uv  浏览量pv  跳出率/跳出次数  新访客比例/新访客数	||	人均浏览量
 * 	例如 ：1	10	A	10015	10016	A	A	4_1	2013-01-01 16	PC		2	2	2	1.00	50.00	50.00
 * **/ 
public class PageViewStatictis {
	static final String FS = "\t";

	public static class M1 extends Mapper<LongWritable, Text, Text, Text> {

		private Text oKey = new Text();
		private Text oVal = new Text();
		TrackSplitUtils trackSplit = TrackSplitUtils.getInstance();
		static Map<String, String> trackURLMap = new HashMap<String, String>();
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
					String d = trackinfo.getTkEpoch();//获取点击时间戳
					
					String curDate=MyDateUtil.getCurDate(d);
					
					if(StatisticsUtils.ifCatesIsRight(cates)){
						continue  ;
					}
					
					// 只统计详情页pv
					if ("".equalsIgnoreCase(pageType) || pageType == null||!"detail".equalsIgnoreCase(pageType)) {
						continue  ;
					}
					
					String params =trackinfo.getTkUnitParams();
					
					Map <String , String > paramMaps =StringUtils.getString4Map(params);
					
					String xiaoquId ; 
					String shi ; 
					String price ; 
					
					if(paramMaps!=null){
						if(cates.startsWith("0,1,8")){
							xiaoquId=paramMaps.get("1588");
							shi=paramMaps.get("1590");
							price=paramMaps.get("1016");
						}else{
							xiaoquId=paramMaps.get("1619");
							shi=paramMaps.get("1612");
							price=paramMaps.get("1610");
						}
					}else{
						xiaoquId="A";
						shi="A";
						price=null;
					}	
					
					String[] visitCountArr = trackinfo.getTkGautma().split("\\.");
					if ("1".equalsIgnoreCase(visitCountArr[visitCountArr.length - 1])) {
						newVisitFlag = 1;
					}

					//小区是否为数字
//						if(xiaoquId==null||!xiaoquId.matches("^[0-9]*[1-9][0-9]*$")){
//							continue  ;
//						}
					
					String priceSection =PriceSection.getPriceSection(price); 
					
					if(priceSection==null){
						priceSection="A";
					}
					
					for (String tmpCate : StatisticsUtils.getDimList(cates)) {
						for (String tmpArea : StatisticsUtils.getDimList4Area(areas,xiaoquId)) {
							for (String type : new String[] { "A", shi+"_1",priceSection+"_2"}) {
								// 访问次数visit  独立访客数uv  浏览量pv  跳出率/跳出次数  新访客比例/新访客数	||	人均浏览量
								String outKey = tmpCate + "@" + tmpArea + "@" + type+"@"+curDate;
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

	
	public static class R1 extends Reducer<Text, Text, Text, Text> {
		Text oKey = new Text();
		Text oVal = new Text();
		String DATE;
		


		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException {
			
			String[] keyArr = key.toString().split("@");
			
			if(!"A".equals( keyArr[0])||!"A".equals( keyArr[1])||!"A".equals( keyArr[2])){
				return ;
			}
			
			if (DATE == null) {
				DATE = context.getConfiguration().get("DATE");
			}
			
			if(!DATE.equals(keyArr[8].split(" ")[0])){
				return ;
			}
			
			String outKey =  keyArr[3] + FS + keyArr[4]+ FS + keyArr[5] + FS + keyArr[6]+ FS + keyArr[7]+ FS + keyArr[8];
			
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

			oKey.set(outKey+ FS + "PC" );
			oVal.set(visit + FS + uv + FS + pv + FS + pvAvg + FS + bounceRate + FS + newVisitRate);
			context.write(oKey, oVal);
			
			oKey.set(outKey+ FS + "WL" );
			double temp=pv*WLRandomUtil.getRandom(DATE.substring(0,6));
			pv=  (long)temp;
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
		String baseOutPath = "/dsap/resultdata/sa/zufang_pv";
		
//		String baseInPath3 = "/dsap/rawdata/cmc_diplocal";
//		String baseInPath4 = "/dsap/rawdata/cmc_dispcategory";

		// 例行调度：昨天的日期
		for (String runDate : MyDateUtil.getDateList(startDate, endDate, 0)) {
			conf.set("DATE", runDate);
			Job job = new Job(conf, "PageViewStatictis");
			job.setInputFormatClass(TrackInputFormat.class);
			job.setJarByClass(PageViewStatictis.class);
			job.setMapperClass(M1.class);
			job.setReducerClass(R1.class);
			job.setNumReduceTasks(60);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			// 加载 track.58.com
			FileInputFormat.addInputPath(job, new Path(baseInPath01 + "/" + runDate));


			String outPath = baseOutPath + "/" + runDate;
			FileSystem.get(conf).delete(new Path(outPath), true);
			FileOutputFormat.setOutputPath(job, new Path(outPath));
			exitCode = job.waitForCompletion(true) ? 0 : 1;
			System.out.println("--------------------------------------------END1");

		}
		System.exit(exitCode);

	}

}
