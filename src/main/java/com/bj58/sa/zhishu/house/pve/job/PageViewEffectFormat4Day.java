package com.bj58.sa.zhishu.house.pve.job;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
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

import com.bj58.sa.client.constant.Table;
import com.bj58.sa.zhishu.house.pv.util.ExceptionUtil;
import com.bj58.sa.zhishu.house.pv.util.KeyUtil;
import com.bj58.sa.zhishu.house.pv.util.MyDateUtil;


/**
 * desc: 格式化从20130101 至当天 每天的 租房和合租房详情页的 pv效果数据
 * input : /dsap/middata/infostat/shx_zufang_pve
 * output:/dsap/middata/infostat/shx_zufang_pve_day_res
 * outputformat:  key value
 * key ：Area.city,Platform.pc,new Integer(cityId) ,0   例如225179985663492096
 * value：{"pc":{"period":"20130101|20140611","indexes":"272503,361546,417769,516828,528111,526244,514887,507943,388568.....}}
 * **/
public class PageViewEffectFormat4Day {
	final static String FS = "\t";
	public static class M1 extends Mapper<LongWritable, Text, Text, Text> {

		private Text oKey = new Text();
		private Text oVal = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {
			try{
				String kStr=value.toString();
				
				String [] fields = kStr.split(FS);	
				// 同一个小区可以属于不同商圈、合成一个纪录
 				String k="";
				if(!"A".equals(fields[3])){
					Integer xiaoquId =Integer.parseInt(fields[3]);
					k="A"+FS+"A"+FS+"A"+FS+xiaoquId+FS+fields[6];;
				}else{
					k =fields[0]+FS+ fields[1]+FS+ fields[2]+FS+ fields[3]+FS+fields[6];;
				}
				
				Long kLon=KeyUtil.getKey(k.split(FS), "0" , Table.house_pv_effect);
				
				if(kLon.toString().length()<10){
					return ;
				}
				
				String v=fields[5]+FS+ fields[9];
				oKey.set(kLon.toString());
				oVal.set(v);
				context.write(oKey, oVal);
				
			} catch (Exception e) {
				return ;
//				oVal =new Text(ExceptionUtil.getExceptionDetail(e));
//				context.write(oKey, oVal);
			}
		}
	}

	
	public static class R1 extends Reducer<Text, Text, Text, Text> {
		Text oKey = new Text();
		Text oVal = new Text();
		String CURDATE;
		String START_DATE ;
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException {
			try{
				oKey.set(key);
				//oKey.set(key);
				//构造每天的点击量的map	
				TreeMap <String ,Double>map = new TreeMap<String, Double>();
				
				CURDATE = context.getConfiguration().get("CURDATE");
				
				if (CURDATE == null) {
					Calendar   cal   =   Calendar.getInstance();
					cal.add(Calendar.DATE,   -1);
					CURDATE = new SimpleDateFormat( "yyyyMMdd ").format(cal.getTime());
				}
				
				START_DATE = context.getConfiguration().get("START_DATE");
				
				if (START_DATE == null) {
					START_DATE = "20140501";
				}
				
				
				for (String runDate : MyDateUtil.getDateList(START_DATE, CURDATE, 0)) {
					map.put(runDate, 0.0);
				}
			
			
				for (Text val : values) {
					String v=val.toString();
					String[] ks=v.split(FS);
					String mKey =ks[0];
					if(map.containsKey(mKey)){
						Double mVal = map.get(mKey);
						mVal+=Double.parseDouble(ks[1]);
						BigDecimal   b   =   new   BigDecimal(mVal);  
						Double   f1   =   b.setScale(2,   BigDecimal.ROUND_HALF_UP).doubleValue();  
						map.put(mKey, f1);
					}else{
						map.put(mKey, Double.parseDouble(ks[1]));
					}
				}
				
				String  str="";
				
				for(String k : map.keySet()){
					str+=map.get(k)+",";
				}
				
				str=str.substring(0,str.lastIndexOf(","));
				StringBuffer sb= new StringBuffer();
				sb.append("\"period\":\"");
				sb.append(START_DATE);
				sb.append("|").append(CURDATE).append("\",\"indexes\":\"");
				sb.append(str).append("\"");
				
				oVal.set(sb.toString());
				//oVal.set(map.toString());
				context.write(oKey,oVal);
			} catch (Exception e) {
				oVal =new Text(ExceptionUtil.getExceptionDetail(e));
				context.write(oKey, oVal);
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException,
			URISyntaxException, ParseException {

		Configuration conf = new Configuration();
		conf.set("mapred.job.queue.name", "regular"); // default,regular,realtime
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		int exitCode = 127;

		System.out.println("************************************************************");
		System.out.println("************************************************************");
		System.out.println("Usage:example: file.jar  , one  param ");
		System.out.println("************************************************************");
		System.out.println("************************************************************");
		System.out.println("********************start************************************");
		
		
		String []ms = otherArgs[0].split("-");
		if(ms.length<1){
			System.out.println("***args length is 0 ******");
			return ; 
		}
		
		for(String s : ms ){
			System.out.println("param :"+s);
		}
		
		ms=MyDateUtil.getAllMonths(ms[0] , ms[1]);
		
		conf.set("START_DATE", ms[0]+"01");  

		String baseInPath01 = "/dsap/resultdata/sa/zufang_pve";
		String baseOutPath = "/dsap/resultdata/sa/zufang_pve_day_res";
		
		Job job = new Job(conf, "PageViewEffectFormat4Day");
		job.setJarByClass(PageViewEffectFormat4Day.class);
		job.setMapperClass(M1.class);
		job.setReducerClass(R1.class);
		job.setNumReduceTasks(60);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		Date dt = new Date();   
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM");   
		String curDate=sdf.format(dt);
	    
	    FileSystem fs = FileSystem.get(conf);
	    
	    FileStatus[] stats = fs.listStatus(new Path(baseInPath01));
        
        for(int i = 0; i < stats.length; ++i)
        {
             if (stats[i].isDir()){
            	 for(String s : ms){
            		 if(stats[i].getPath().toString().indexOf(s)>0){
            			 System.out.println(stats[i].getPath().toString());
                		 FileInputFormat.addInputPath(job, new Path(stats[i].getPath().toString()));
                	 }
            	 }
            }
        }
        
		fs.close();

		String outPath = baseOutPath + "/" +curDate;
		FileSystem.get(conf).delete(new Path(outPath), true);
		FileOutputFormat.setOutputPath(job, new Path(outPath));
		exitCode = job.waitForCompletion(true) ? 0 : 1;
		System.out.println("********************end************************************");
		System.exit(exitCode);

	}

}
