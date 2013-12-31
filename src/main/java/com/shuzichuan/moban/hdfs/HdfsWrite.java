package com.shuzichuan.moban.hdfs;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.Random; 

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable; 

/**
 * 自行在本地文件系统生成一个大约100字节的文本文件，写一段程序（可以利用Java API或C API），
 * 读入这个文件，并将其第101-120字节的内容写入HDFS成为一个新文件
 * @author zf
 *
 */
public class HdfsWrite {
 
 public static void main(String args[]){
  try {
   //参数1本地文件的地址  参数2本地文件的字节数  参数3 HDFS地址
   String localFileName = args[0];
   int byteNum = Integer.parseInt(args[1]);
   String hdfsFileName = args[2];
   if (byteNum > 120) {
    File localFile = new File(localFileName);
    if (!localFile.exists()) {
     localFile.createNewFile();
     FileOutputStream localout=new FileOutputStream(localFile,true); 
    for(int i=0;i<byteNum;i++){
      StringBuffer sb=new StringBuffer();
               sb.append(new Random().nextInt(10));
               localout.write(sb.toString().getBytes());
     }
     localout.close();
     //读取文件把第101-120字节的内容写入HDFS成为一个新文件
     Configuration conf = new Configuration();
     FileSystem fs = FileSystem.get(URI.create(hdfsFileName), conf);
     InputStream in = null;
     FSDataOutputStream out = null;
     try {
      in = new BufferedInputStream(new FileInputStream(localFileName));
      in.skip(100);
      byte[] b = new byte[20];
      in.read(b, 0, 20);
      out = fs.create(new Path(hdfsFileName),new Progressable(){
       public void progress() {
        System.out.print(".");
       }
      });
      InputStream is = new ByteArrayInputStream(b);
      IOUtils.copyBytes(is, out, 4096, true);
      System.out.println("write success");
     } finally {
      out.close();	 
      IOUtils.closeStream(in);
      IOUtils.closeStream(out);
     }
     
    } else {
     System.out.println("file " + localFileName + " already exits  please change name ！");
    } 

  } else {
    System.out.println("bytes num  must  more than 120");
   }
  } catch (Exception e) {
   e.printStackTrace();
  }
 }
} 
