package com.bj58.sa.zhishu.house.pv.job;

import java.io.IOException;
import java.math.BigDecimal;
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
import com.bj58.sa.zhishu.house.pv.util.StatisticsUtils;
import com.bj58.sa.zhishu.house.pv.util.StringUtils;
import com.bj58.sa.zhishu.house.pv.util.UrlUtil;
/**
 * desc :  计算从20130101 至当天每小时的 租房和合租房详情页的 pv效果数据
 * input : /dsap/rawdata/track.58.com
 * output:/dsap/middata/infostat/shx_zufang_pve
 * outputformat:
 * 	cateID   areaId  xiaoquId  priceSection_2(价格区间)或者huxing_1(户型)  platform  (wl, pc)  访问次数visit  独立访客数uv  浏览量pv  跳出率/跳出次数  新访客比例/新访客数	||	人均浏览量
 * 	例如 ：1	10	A	10015	10016	A	A	4_1	2013-01-01 16	PC		2	2	2	1.00	50.00	50.00
 * **/ 
public class PageViewEffectStatictis {
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
					
					String url = trackinfo.getTkUrl();
					
					String infoId=UrlUtil.getInfoIdByTrackUrl(url);
					
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
					
					if(paramMaps!=null){
						if(cates.startsWith("0,1,8")){
							xiaoquId=paramMaps.get("1588");
						}else{
							xiaoquId=paramMaps.get("1619");
						}
					}else{
						xiaoquId="A";
					}	
					
					String[] visitCountArr = trackinfo.getTkGautma().split("\\.");
					if ("1".equalsIgnoreCase(visitCountArr[visitCountArr.length - 1])) {
						newVisitFlag = 1;
					}
					
					for (String tmpCate : StatisticsUtils.getDimList(cates)) {
						for (String tmpArea : StatisticsUtils.getDimList4Area(areas,xiaoquId)) {
							for (String type : new String[] {"A"}) {
								// 访问次数visit  独立访客数uv  浏览量pv  跳出率/跳出次数  新访客比例/新访客数	||	人均浏览量
								String outKey = tmpCate + "@" + tmpArea + "@" + type;
								outKeySet.add(outKey);
								String outValue = "0|E|1|0|0|"+infoId;
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
					String outValue = "1|" + cookieId + "|" + "0|" + count + "|" + newVisit+"|0";
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
			
			String outKey =  keyArr[3] + FS + keyArr[4]+ FS + keyArr[5] + FS + keyArr[6]+ FS + keyArr[7];
			
			Long pv = 0l;
			//帖子个数
			Set <String> infoSets = new HashSet<String>();
			
			for (Text val : values) {
				String[] valArr = val.toString().split("\\|");
				
				pv += Integer.parseInt(valArr[2]);

				infoSets.add(valArr[5]);
			}
			
			Double pve = 0.0;
			if(infoSets.size()>0){
				pve = Double.parseDouble(pv.toString())/infoSets.size();
			}
			
			BigDecimal   b   =   new   BigDecimal(pve);  
			Double   f1   =   b.setScale(2,   BigDecimal.ROUND_HALF_UP).doubleValue();  

			oKey.set(outKey+ FS + DATE+FS +"PC" );
			oVal.set( pv + FS+infoSets.size()+FS +f1);
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
		String baseOutPath = "/dsap/middata/infostat/shx_zufang_pve";
		
		// 例行调度：昨天的日期
		for (String runDate : MyDateUtil.getDateList(startDate, endDate, 0)) {
			conf.set("DATE", runDate);
			Job job = new Job(conf, "PageViewEffectStatictis");
			job.setInputFormatClass(TrackInputFormat.class);
			job.setJarByClass(PageViewEffectStatictis.class);
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
			System.out.println("--------------------------------------------END"+runDate);

		}
		System.exit(exitCode);

	}

}
