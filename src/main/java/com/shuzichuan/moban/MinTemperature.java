package com.shuzichuan.moban;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


/**
 * 根据天气数据分析最低气温
 * @author zhufeng
 *
 */
public class MinTemperature {
	enum Counter 
	{
		LINESKIP,	//出错的行
	}
	static class MinTemperatureMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
		private static final int MISSING = 9999;
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			try {
				System.out.println("========================map============================");
				String line = value.toString();
				String year = line.substring(15,19);
				int airTemperature;
				if(line.charAt(87)=='+'){
					airTemperature = Integer.parseInt(line.substring(88, 92));
				}else{
					airTemperature = Integer.parseInt(line.substring(87, 92));
				}
				if(airTemperature<-300){
					System.out.println("========================<-300============================");
					context.setStatus("wendu  xiao yu -300 : see log");
					context.getCounter(Counter.LINESKIP).increment(1);	//出错令计数器+1
				}
				context.setStatus("hahaha");
				String quality = line.substring(92,93);
				if(airTemperature!=MISSING && quality.matches("[01459]")){
					context.write(new Text(year), new IntWritable(airTemperature));
				}
			} catch (Exception e) {
				System.out.println("========================maperror============================");
				context.getCounter(Counter.LINESKIP).increment(1);	//出错令计数器+1
				return;
			}
			
		}
		
	}
	static class MinTemperatureReducer extends Reducer<Text,IntWritable,Text,IntWritable>{

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context)
				throws IOException, InterruptedException {
			System.out.println("========================reduce============================");
			int minValue = Integer.MAX_VALUE;
			for(IntWritable value:values){
				minValue = Math.min(minValue, value.get());
			}
			context.write(key,new IntWritable(minValue));
		}
	}
	public static void main(String args[]) throws Exception{
		Configuration  conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: MinTemperature <in> <out>");
	      System.exit(2);
	    }
	    Job job = new Job(conf,"min temperature");
		job.setJarByClass(MinTemperature.class);
		job.setMapperClass(MinTemperatureMapper.class);
		job.setReducerClass(MinTemperatureReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}
