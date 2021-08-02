package stream.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class HdfsUtil {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME","hdfs");
        System.out.println(System.getProperty("HADOOP_USER_NAME"));
        long ln = System.currentTimeMillis();
        System.out.println("System.currentTimeMillis():"+ln);
        FileSystem HDFS = FileSystem.get(new URI("hdfs://dn171.hadoop.unicom:8020"),new Configuration(),"hdfs");
        if(HDFS.exists(new Path("/client-ssl/ca-cert"))){
            System.out.println("文件存在");
            HDFS.copyToLocalFile(false,new Path("/client-ssl/ca-cert"),new Path("/tmp/"+ ln +"/ca-cert"),true);
        }else{
            System.out.println("文件不存在!");
        }
        HDFS.close();
    }
}
