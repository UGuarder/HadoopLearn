package com.bj58.sa.zhishu.house.pv.job;

import java.io.IOException;
import java.net.URISyntaxException;

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

import com.bj58.sa.zhishu.house.pv.util.ExceptionUtil;
import com.bj58.sa.zhishu.house.pv.util.MyDateUtil;

public class PageViewFormat {
	final static String FS = "\t";
	public static class M1 extends Mapper<LongWritable, Text, Text, Text> {

		private Text oKey = new Text();
		private Text oVal = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {
			try{
				String kStr=value.toString();
				
				String [] fields = kStr.split("\t");
				String k =fields[0]+FS+ fields[1]+FS+ fields[2]+FS+ fields[3]+FS+ fields[4]+FS+ fields[5]+FS+ fields[6]+FS+ fields[7];
				String v= fields[10];
				oKey.set(k);
				oVal.set(v);
				context.write(oKey, oVal);
			} catch (Exception e) {
				oVal =new Text(ExceptionUtil.getExceptionDetail(e));
				context.write(oKey, oVal);
			}
		}
	}

	
	public static class R1 extends Reducer<Text, Text, Text, Text> {
		Text oKey = new Text();
		Text oVal = new Text();
		String DATE;

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException {
			try{
				Long sum =0l; 
				for (Text val : values) {
					String v=val.toString();
					
					sum+=Integer.parseInt(v);
				}
				oVal.set(sum.toString());
				context.write(key,oVal);
			} catch (Exception e) {
				oVal =new Text(ExceptionUtil.getExceptionDetail(e));
				context.write(oKey, oVal);
			}
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

		String baseInPath01 = "/dsap/middata/infostat/shx_zufang_pv";
		String baseOutPath = "/dsap/middata/infostat/shx_zufang_pv_res";
		
		System.out.println("args --------"+otherArgs[0]);
		
//		String baseInPath3 = "/dsap/rawdata/cmc_diplocal";
//		String baseInPath4 = "/dsap/rawdata/cmc_dispcategory";

		// 例行调度：昨天的日期
		for (String runDate : MyDateUtil.getDateList(startDate, endDate, 0)) {
			conf.set("DATE", runDate);
			Job job = new Job(conf, "PageViewFormat");
			//job.setInputFormatClass(TrackInputFormat.class);
			job.setJarByClass(PageViewFormat.class);
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
//			String baseInPath2 = baseInPath3 + "/" + runDate;
//			FileSystem hdfs1 = FileSystem.get(conf);
//			FileStatus[] fileList = hdfs1.listStatus(new Path(baseInPath2));
//			for (int i = 0; i < fileList.length; i++) {
//				String fileName = fileList[i].getPath().getName();
//				if (fileName.contains("part")) {
//					System.out.println("----------------->>> " + baseInPath2 + "/" + fileName);
//					String inPath2Link = new Path(baseInPath2 + "/" + fileName).toUri().toString() + "#"
//							+ "cmc_diplocal" + i;
//					DistributedCache.addCacheFile(new URI(inPath2Link), job.getConfiguration());
//				} 
//			}

			// 加载 cmc 表现分类
//			baseInPath2 = baseInPath4 + "/" + runDate;
//			fileList = hdfs1.listStatus(new Path(baseInPath2));
//			for (int i = 0; i < fileList.length; i++) {
//				String fileName = fileList[i].getPath().getName();
//				if (fileName.contains("part")) {
//					System.out.println("----------------->>> " + baseInPath2 + "/" + fileName);
//					String inPath2Link = new Path(baseInPath2 + "/" + fileName).toUri().toString() + "#"
//							+ "cmc_dispcategory" + i;
//					DistributedCache.addCacheFile(new URI(inPath2Link), job.getConfiguration());
//				}
//			}
//			hdfs1.close();

			String outPath = baseOutPath + "/" + runDate;
			FileSystem.get(conf).delete(new Path(outPath), true);
			FileOutputFormat.setOutputPath(job, new Path(outPath));
			exitCode = job.waitForCompletion(true) ? 0 : 1;
			System.out.println("--------------------------------------------END1");

		}
		System.exit(exitCode);

	}

}
