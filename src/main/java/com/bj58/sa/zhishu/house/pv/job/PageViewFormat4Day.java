package com.bj58.sa.zhishu.house.pv.job;

import java.io.IOException;
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
 * desc: 格式化从20130101 至当天 每天的 租房和合租房详情页的 pv数据
 * input : /dsap/middata/infostat/shx_zufang_pv
 * output:/dsap/middata/infostat/shx_zufang_pv_day_res
 * outputformat:  key value
 * key ：Area.city,Platform.pc,new Integer(cityId) ,0   例如225179985663492096
 * value：{"pc":{"period":"20130101|20140611","indexes":"272503,361546,417769,516828,528111,526244,514887,507943,388568.....}}
 * **/
public class PageViewFormat4Day {
	 
	final static String FS = "\t";
	public static class M1 extends Mapper<LongWritable, Text, Text, Text> {

		private Text oKey = new Text();
		private Text oVal = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {
			try{
				String kStr=value.toString();
				
				String [] fields = kStr.split(FS);	
				if(!"A".equals(fields[4])){
					return ; 
				}
				// 同一个小区可以属于不同商圈、合成一个纪录
				
				String datetime =fields[5];
				
				String platform = fields[6];
				
				datetime=datetime.substring(0,6);
				
 				String k="";
				if(!"A".equals(fields[3])){
					Integer xiaoquId =Integer.parseInt(fields[3]);
					k="A"+FS+"A"+FS+"A"+FS+xiaoquId+FS+platform;
				}else{
					k =fields[0]+FS+ fields[1]+FS+ fields[2]+FS+ fields[3]+FS+platform;
				}
				
				Long kLon=KeyUtil.getKey(k.split(FS), "0" , Table.house_pv );
				
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
		String START_DATE;
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException {
			try{	
				
				//oKey.set(kLon.toString());
				oKey.set(key);
				//构造每天的点击量的map	
				TreeMap <String ,Long>map = new TreeMap<String, Long>();
				
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
					map.put(runDate, 0l);
				}
			
			
				for (Text val : values) {
					String v=val.toString();
					String[] ks=v.split(FS);
					String mKey =ks[0].split(" ")[0];
					if(map.containsKey(mKey)){
						Long mVal = map.get(mKey);
						mVal+=Long.parseLong(ks[1]);
						map.put(mKey, mVal);
					}else{
						map.put(mKey, Long.parseLong(ks[1]));
					}
				}
				
				String top ="\"period\":\""+START_DATE+"|"+CURDATE+"\",\"indexes\":\"";
				String bottom="\"";
				
				
				//构造 pc 点击数格式
				String  pcstr="";
				for(String k : map.keySet()){
					pcstr+=map.get(k)+",";
				}
				pcstr=pcstr.substring(0,pcstr.lastIndexOf(","));
				oVal.set(top+pcstr+bottom);
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

		String baseInPath01 = "/dsap/resultdata/sa/zufang_pv";
		String baseOutPath = "/dsap/resultdata/sa/zufang_pv_day_res";
		
		Job job = new Job(conf, "PageViewFormat4Day");
		job.setJarByClass(PageViewFormat4Day.class);
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
            			// System.out.println(stats[i].getPath().toString());
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
