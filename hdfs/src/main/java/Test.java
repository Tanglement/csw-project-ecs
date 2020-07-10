import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileInputStream;

public class Test{
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        //这里指定使用的是 hdfs文件系统
        conf.set("fs.defaultFS", "hdfs://bigdata:9000");
        conf.set("dfs.client.use.datanode.hostname", "true");
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        FileSystem fs = FileSystem.get(conf);

        //使用Stream的形式操作HDFS，这是更底层的方式
        //FSDataOutputStream outputStream = fs.create(new Path("test.txt"), true); //输出流到HDFS
        FSDataInputStream inputStream = fs.open(new Path("hdfs://bigdata:9000/user/hadoop/file/file1.txt"));
        byte[] data = new byte[1024];
        inputStream.read ( data );
        String str = new String(data);
        System.out.println (str);
        System.out.println (data.toString ());
        //IOUtils.copy(inputStream, outputStream); //完成从本地上传文件到hdfs
        //outputStream.write (data);
        fs.close();
    }
}