import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;

import java.io.IOException;

public class HBase_API {
    public static Configuration conf;
    public static Connection conn;
    public static Admin admin;

    public static void init() {
        conf = HBaseConfiguration.create ();
        conf.set("hbase.master","121.199.53.82:16000");
        conf.set("hbase.zookeeper.quorum", "121.199.54.82");
        conf.set("hbase.rootdir","hdfs://121.199.54.82:9000/hbase");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        try{
            conn = ConnectionFactory.createConnection(conf);
            admin = conn.getAdmin();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public static void close() {
        try {
            if(admin != null)
                admin.close ();
            if(conn != null)
                conn.close();
        } catch (IOException e) {
            e.printStackTrace ();
        }
    }

    public static void createTable(String tablename, String[] colFamily) throws IOException {
        TableName tableName = TableName.valueOf ( tablename );
        if(admin.tableExists ( tableName )) {
            System.out.println ("table is exists");
            admin.disableTable ( tableName );
            admin.deleteTable ( tableName );
        }

        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder ( tableName );
        for(String family : colFamily) {
            tableDescriptorBuilder.setColumnFamily (ColumnFamilyDescriptorBuilder.newBuilder ( Bytes.toBytes (family)).build ());
        }
        admin.createTable ( tableDescriptorBuilder.build ());
    }

    //删除记录，Delete删除数据，参数为，表明，行键，列族，列限定符
    public static void deleteRow(String tableName,String row,String colFamily,String col)throws IOException{
        Table table = conn.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(row));
        //删除指定列族
        //delete.addFamily(Bytes.toBytes(colFamily));
        //删除指定列
        delete.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col));
        table.delete(delete);
        table.close();
    }

    //添加记录，Put添加数据，参数为表明，行键，多个列族:列限定符，多个值
    public static void insertRecord(String tableName,String row,String[] colfamily,String[] values) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        for(int i = 0;i != colfamily.length;i++){
            Put put = new Put(row.getBytes());
            //colfamily形式 info:name，其中info为列族，name为列限定符
            String[] cols = colfamily[i].split(":");
            put.addColumn(cols[0].getBytes(), cols[1].getBytes(), values[i].getBytes());
            table.put(put);
        }
        table.close();
    }

    //scan查找某一列族下的所有值，Scan和ResultScanner
    public static void scanColumn(String tableName,String column)throws  IOException{
        Table table = conn.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(column));
        ResultScanner scanner = table.getScanner(scan);
        for (Result result = scanner.next(); result != null; result = scanner.next()){
            //得到的Result类型是同一个rowkey的一行记录，多个Cell，showCell()是对Result进行输出。
            showCell(result);
        }
        table.close();
    }
    public static void showCell(Result result) throws  IOException{
        //将Result转变为Cell，一行多个Cell
        Cell[] cells = result.rawCells();
        for(Cell cell:cells){
            System.out.println("RowName:"+new String(Bytes.toString(cell.getRowArray(),cell.getRowOffset(), cell.getRowLength()))+" ");
            System.out.println("Timetamp:"+cell.getTimestamp()+" ");
            System.out.println("column Family:"+new String(Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(), cell.getFamilyLength()))+" ");
            System.out.println("row Name:"+new String(Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(), cell.getQualifierLength()))+" ");
            System.out.println("value:"+new String(Bytes.toString(cell.getValueArray(),cell.getValueOffset(), cell.getValueLength()))+" ");
            /*
            System.out.println("RowName:"+new String(CellUtil.cloneRow(cell))+" ");
            System.out.println("Timetamp:"+cell.getTimestamp()+" ");
            System.out.println("column Family:"+new String(CellUtil.cloneFamily(cell))+" ");
            System.out.println("row Name:"+new String(CellUtil.cloneQualifier(cell))+" ");
            System.out.println("value:"+new String(CellUtil.cloneValue(cell))+" ");
             */
        }
    }

    public static void main(String[] args) throws IOException {
        init();
        String[] str = new String[]{"score:music","score:chinese"};
        String[] val = new String[]{"92","95"};
        insertRecord("student","zhangsan",str,val);
        close ();
    }
}
