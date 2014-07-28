package com.bj58.sa.zhishu.house.pv.job;

import java.io.IOException;
import java.net.URISyntaxException;
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
/**
 * desc: 格式化制定月份户型为单位的  租房和合租房详情页的 pv数据
 * input : /dsap/middata/infostat/shx_zufang_pv
 * output:/dsap/middata/infostat/shx_zufang_pv_pricesection_res
 * outputformat:  key value
 *  		key:
 *  		value:
 * */
public class PageViewFormat4HouseTypeOfMouth {
	final static String FS = "\t";
	public static class M1 extends Mapper<LongWritable, Text, Text, Text> {

		private Text oKey = new Text();
		private Text oVal = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {
			try{
				String kStr=value.toString();
				
				String [] fields = kStr.split(FS);	
				if(fields[4].indexOf("_1")<0||fields[7].indexOf("A")>0){
					return ; 
				}
				
				//同一时间格式为yyyyMMdd
				String datetime =fields[5];
				
				datetime=datetime.substring(0,6);
				// 同一个小区可以属于不同商圈、合成一个纪录
				
 				String k="";
				if(!"A".equals(fields[3])){
					Integer xiaoquId =Integer.parseInt(fields[3]);
					k="A"+FS+"A"+FS+"A"+FS+xiaoquId+FS+ fields[6]+FS+datetime;
				}else{
					k =fields[0]+FS+ fields[1]+FS+ fields[2]+FS+ fields[3]+FS+ fields[6]+FS+datetime;
				}
				
				Long kLon=KeyUtil.getKey(k.split(FS), datetime , Table.house_pv_huxing);
				
				if(kLon.toString().length()<10){
					return ;
				}		
				
				String v=fields[4]+FS+fields[5]+FS+ fields[9];
				oKey.set(kLon.toString()+FS+datetime);
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
		String DATE;

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException {
			try{
				String kStr=key.toString();
				String [] fields = kStr.split(FS);	
				
				String datetime =fields[1];
				
				oKey.set(fields[0]);
				//oKey.set(key);
				//构造每天的点击量的map	
				TreeMap <String ,Long>map = new TreeMap<String, Long>();
				for (Text val : values) {
					String v=val.toString();
					String[] ks=v.split(FS);
					String mKey=ks[1].split(" ")[0];
					String pricesection =ks[0].split("_")[0];
					if("A".equals(pricesection)){
						continue;
					}
					if("null".equals(pricesection)||Integer.parseInt(pricesection)>4){
						pricesection="5";
					}
					mKey=mKey+"_"+pricesection;
					if(map.containsKey(mKey)){
						Long mVal = map.get(mKey);
						mVal+=Long.parseLong(ks[2]);
						map.put(mKey, mVal);
					}else{
						map.put(mKey, Long.parseLong(ks[2]));
					}
				}
				
				String  str="";
				String d = null;
				for(String k : map.keySet()){
					String day=k.split("_")[0];
					String huxing=k.split("_")[1];
					if(d==null){
						d=day;
					}
					if(!d.equals(day)){
						if(str.lastIndexOf(",")>0){
							str=str.substring(0,str.lastIndexOf(","));
						}
						str=str+"|"+huxing+"="+map.get(k)+",";
						d=day;
					}else{
						str=str+huxing+"="+map.get(k)+",";
					}
				}
				
				str=str.substring(0,str.lastIndexOf(","));
				
				String top ="\"period\":\""+datetime+"\",\"indexes\":\"";
				String bottom="\"";
				oVal.set(top+str+bottom);
				//oVal.set(map.toString());
				context.write(oKey,oVal);
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
			System.out.println("args[0] is dateList: 201301,201303 or 201301");
			System.out.println("************************************************************");
			System.out.println("************************************************************");
			System.exit(exitCode);
		}

//		String startDate = otherArgs[0].split(",")[0];
//		String endDate = otherArgs[0].split(",").length == 2 ? otherArgs[0].split(",")[1] : startDate;

		String baseInPath01 = "/dsap/resultdata/sa/zufang_pv";
		String baseOutPath = "/dsap/resultdata/sa/zufang_pv_housetype_res";
		
		System.out.println("args --------"+otherArgs[0]);
		
		FileSystem fs = FileSystem.get(conf);
	    FileStatus[] stats = fs.listStatus(new Path(baseInPath01));
        
	    String param =otherArgs[0] ; 
	    
	    if(param.indexOf("-")>0){
	    	
	    }
    	String []ms = param.split(",");
    	for (int y = 0; y < ms.length;y++) {
    		
    		if(ms[y].length()<1){
    			continue ;
    		}
    		
    		ms[y]=ms[y].substring(0, 6);
    		
    		Job job = new Job(conf, "PageViewFormat4HouseTypeOfMouth");
    		job.setJarByClass(PageViewFormat4HouseTypeOfMouth.class);
    		job.setMapperClass(M1.class);
    		job.setReducerClass(R1.class);
    		job.setNumReduceTasks(60);
    		job.setOutputKeyClass(Text.class);
    		job.setOutputValueClass(Text.class);
    		job.setMapOutputKeyClass(Text.class);
    		job.setMapOutputValueClass(Text.class);
    		
    		
    		 for(int i = 0; i < stats.length; ++i) {
                 if (stats[i].isDir()){
                	String path=stats[i].getPath().toString();
                	if(path.indexOf(ms[y])>0){
                		 System.out.println(path);
                         FileInputFormat.addInputPath(job, new Path(stats[i].getPath().toString()));
                	}
                }
            }
    		 
    		 
    		String outPath = baseOutPath + "/" + ms[y];
			FileSystem.get(conf).delete(new Path(outPath), true);
			FileOutputFormat.setOutputPath(job, new Path(outPath));
			exitCode = job.waitForCompletion(true) ? 0 : 1;
			System.out.println("--------------------------------------------END---"+ms[y]);
		}
		fs.close();
		System.exit(exitCode);

	}

}
