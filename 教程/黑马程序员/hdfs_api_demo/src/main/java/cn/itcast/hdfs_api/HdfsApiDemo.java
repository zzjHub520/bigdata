package cn.itcast.hdfs_api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import org.apache.commons.io.IOUtils;

public class HdfsApiDemo {

    @Test
    public void getFileSystem1() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node01:8020/");
        FileSystem fileSystem = FileSystem.get(configuration);
        System.out.println(fileSystem.toString());

    }



    @Test
    public void urlHdfs()throws Exception{
        //第一步：注册hdfs的URL
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());

        //获取文件输入流
        InputStream inputStream = new URL("hdfs://node01:8020/a.txt").openStream();

        //获取文件输出流
        FileOutputStream outputStream = new FileOutputStream(new File("D:\\hello.txt"));

        //实现文件的拷贝
        IOUtils.copy(inputStream, outputStream);

        //关闭流
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(outputStream);
    }


}
