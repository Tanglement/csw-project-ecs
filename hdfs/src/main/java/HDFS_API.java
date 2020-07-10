import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

public class HDFS_API {
    public static FileSystem fs;
    public static Configuration conf;

    public static void init() throws Exception {
        //通过这种方式设置客户端身份
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        conf = new Configuration ();
        conf.set("fs.defaultFS","hdfs://bigdata:9000");
        conf.set("dfs.client.use.datanode.hostname", "true");
        conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
        fs = FileSystem.get(conf);

        //或者使用下面的方式设置客户端身份
        //fs = FileSystem.get(new URI("hdfs://bigdata:9000"),conf,"hadoop");
    }
    public static void close() throws Exception {
        if(fs != null)
            fs.close ();
    }

    //遍历path路径下文件及文件信息
    public static void listFiles(String path) throws Exception {
        //listStatus可以添加FileFilter类过滤不要的文件
        FileStatus[] files = fs.listStatus (new Path(path));
        for(FileStatus file : files) {
            //文件大小
            System.out.println ( file.getLen () );
            //文件路径
            System.out.println ( file.getPath () );
            //文件权限
            System.out.println ( file.getPermission () );
            //file.isFile()
            //file.isDirectory()
        }
    }
    //创建文件夹
    public static void mkDir(String path) throws Exception {
        //第一个参数是路径Path，第二个参数是目录权限管理
        fs.mkdirs ( new Path(path) );
    }
    //删除文件夹
    public static void deleteDir(String path) throws Exception {
        fs.delete ( new Path(path), true );
    }
    //下载文件，通过copyTocalFile()和copyFromLocalFile()
    //或者通过FSDataInputStream和FSDataOutputStream
    public static void getFileToLocal(String inputPath, String outputPath) throws Exception {
        //fs.copyToLocalFile (new Path(inputPath), new Path(outputPath));
        //FileSystem对象的open()方法返回一个FSDataInputStream，用以读数据，建立输入流
        FSDataInputStream inputStream = fs.open (new Path(inputPath));
        //本地的输出流
        FileOutputStream outputStream = new FileOutputStream (new File (outputPath));
        IOUtils.copyBytes (inputStream,outputStream,conf);
        IOUtils.closeStreams ( inputStream, outputStream );
    }
    //上传文件
    public static void putFile(String inputPath, String outputPath) throws Exception {
        //第一个参数是本地路径，第二个路径是上传路径，将本地文件上传到HDFS上
        //fs.copyFromLocalFile (new Path(inputPath), new Path(outputPath));
        FileInputStream inputStream = new FileInputStream (new File(inputPath));
        FSDataOutputStream outputStream = fs.create (new Path(outputPath));
        IOUtils.copyBytes (inputStream,outputStream,conf);
        IOUtils.closeStreams ( inputStream,outputStream );
    }

    //在HDFS上用流读写数据
    public static void read_write(String inputPath, String outputPath) throws Exception {
        FSDataInputStream inputStream = fs.open (new Path(inputPath));
        FSDataOutputStream outputStream = fs.create (new Path(outputPath));
        //只能操作字节流
        byte[] buf = new byte[1024];
        inputStream.read(buf);
        //不可以用buf.toString，其没有重写toString()方法，只会返回类名和地址
        System.out.println (new String(buf));
        outputStream.write (buf);
        IOUtils.closeStreams (inputStream,outputStream);
    }


    public static void main(String[] args) throws Exception {
        init();
        mkDir ( "input" );
        read_write ( "test.txt","input/file.txt" );
        close ();
    }
}
