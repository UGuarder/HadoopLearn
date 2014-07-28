package com.bj58.sa.zhishu.house.pv.job;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DeletePath {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException,
			URISyntaxException {

		Configuration conf = new Configuration();

		if (args.length != 1) {
			System.out.println("************************************************************");
			System.out.println("************************************************************");
			System.out.println("Usage: please input 1 params, for example: file.jar args[0]");
			System.out.println("args[0] is path '/dsap/middata/infostat/GetHouseInfo_Job' ");
			System.out.println("************************************************************");
			System.out.println("************************************************************");
		}else{
			String baseOutPath = args[0];

			FileSystem.get(conf).delete(new Path(baseOutPath), true);
		}
		System.out.println("--------------------------------------------END1");
	}
}
