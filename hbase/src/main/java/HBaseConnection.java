import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


public class HBaseConnection {
    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;

    public static void init(){
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.master","121.199.53.82:16000");
        configuration.set("hbase.zookeeper.quorum", "121.199.54.82");
        configuration.set("hbase.rootdir","hdfs://121.199.54.82:9000/hbase");
        configuration.set("hbase.zookeeper.property.clientPort","2181");
        try{
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public static void close(){
        try{
            if(admin != null){
                admin.close();
            }
            if(null != connection){
                connection.close();
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        init();
        System.out.println ("connected");
        //TableName tablename = TableName.valueOf("student");
        String tableName = "student";
        if(admin != null) {
            try{
                if (admin.tableExists(TableName.valueOf(tableName)))
                //如果表已经存在
                {
                    System.out.println(tableName + "已存在");
                }
                else
                //如果表不存在
                {
                    System.out.println(tableName + "不存在");
                }
            }catch (IOException e){
                e.printStackTrace ();
            }
        }
        close();
    }
}
