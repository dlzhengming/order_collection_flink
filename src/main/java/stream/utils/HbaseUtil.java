package stream.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class HbaseUtil {

    //private static final String ZOOKEEPER_QUORUM = "dn01.hadoop.unicom:2181,dn08.hadoop.unicom:2181,dn16.hadoop.unicom:2181";
    //private static final String ZOOKEEPER_ZNODE_PARENT = "/hbase-unsecure";
    //private static final String ZOOKEEPER_QUORUM = "nn170.hadoop.unicom:2181,dn171.hadoop.unicom:2181,dn172.hadoop.unicom:2181";
    //private static final String ZOOKEEPER_ZNODE_PARENT = "/hbase-unsecure";
    private static final String ZOOKEEPER_QUORUM = "audit-dp04:2181,audit-dp05:2181,audit-dp06:2181";
    private static final String ZOOKEEPER_ZNODE_PARENT = "/hbase-unsecure";
    private static Admin admin = null;
    private static Connection conn = null;
    static{
        // 创建hbase配置对象
        Configuration conf = HBaseConfiguration.create();
        conf.set(HConstants.ZOOKEEPER_QUORUM, ZOOKEEPER_QUORUM);
        conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, ZOOKEEPER_ZNODE_PARENT);
        // System.setProperty("HADOOP_USER_NAME","ocdp");
        conf.set("hadoop.user.name", "ocdp");
        try {
            conn = ConnectionFactory.createConnection(conf);
            // 得到管理程序
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建表 create_namespace 'datamanage'
     */
    public void createNameSpace(String namespace) throws Exception {
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
        admin.createNamespace(namespaceDescriptor);
        System.out.println("HBASE表空间:"+namespace+"创建成功");
    }

    /**
     * 创建表 create "CBSS_ORDER_CNT","STREAM_ORACLE_9900_CRM01"
     */
    public void createTable(String tabName,String famliyname) throws Exception {

        HTableDescriptor tab = new HTableDescriptor(TableName.valueOf("FlinkDBA",tabName));
        // 添加列族,每个表至少有一个列族.
        HColumnDescriptor colDesc = new HColumnDescriptor(famliyname);
        tab.addFamily(colDesc);
        // 创建表
        admin.createTable(tab);
        System.out.println("HBASE表:"+tabName+"创建成功");
    }



    public boolean createTableBySplitKeys(String tableName, List<String> columnFamily) {
        try {
            HTableDescriptor tableDescriptor = new HTableDescriptor(
                    TableName.valueOf(tableName));
            for (String cf : columnFamily) {
                tableDescriptor.addFamily(new HColumnDescriptor(cf));
            }
            byte[][] splitKeys = getSplitKeys();
            admin.createTable(tableDescriptor, splitKeys);//指定splitkeys
            System.out.print("===Create Table " + tableName
                    + " Success!columnFamily:" + columnFamily.toString()
                    + "===");
        } catch (MasterNotRunningException e) {
            // TODO Auto-generated catch block
            System.out.print(e);
            return false;
        } catch (ZooKeeperConnectionException e) {
            // TODO Auto-generated catch block
            System.out.print(e);
            return false;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            System.out.print(e);
            return false;
        }
        return true;
    }

    /**
     * 插入数据
     */
    public static void put(String tablename, String rowkey, String famliyname, Map<String,String> datamap) throws Exception {
        Table table = conn.getTable(TableName.valueOf("FlinkDBA",tablename));
        // 将字符串转换成byte[]
        byte[] rowkeybyte = Bytes.toBytes(rowkey);
        Put put = new Put(rowkeybyte);
        if(datamap != null){
            Set<Map.Entry<String,String>> set = datamap.entrySet();
            for(Map.Entry<String,String> entry : set){
                String key = entry.getKey();
                Object value = entry.getValue();
                put.addColumn(Bytes.toBytes(famliyname), Bytes.toBytes(key), Bytes.toBytes(value+""));
            }
        }
        table.put(put);
        table.close();
    }
    public static void putDefault(String tablename, String rowkey, String famliyname, Map<String,String> datamap) throws Exception {
        Table table = conn.getTable(TableName.valueOf("default",tablename));
        // 将字符串转换成byte[]
        byte[] rowkeybyte = Bytes.toBytes(rowkey);
        Put put = new Put(rowkeybyte);
        if(datamap != null){
            Set<Map.Entry<String,String>> set = datamap.entrySet();
            for(Map.Entry<String,String> entry : set){
                String key = entry.getKey();
                Object value = entry.getValue();
                put.addColumn(Bytes.toBytes(famliyname), Bytes.toBytes(key), Bytes.toBytes(value+""));
            }
        }
        table.put(put);
        table.close();
    }
    /**
     * 获取数据
     */
    public static String getdata(String tablename, String rowkey, String famliyname,String colum) throws Exception {
        Table table = conn.getTable(TableName.valueOf("FlinkDBA",tablename));
        //Table table = conn.getTable(TableName.valueOf(tablename));
        // 将字符串转换成byte[]
        byte[] rowkeybyte = Bytes.toBytes(rowkey);
        Get get = new Get(rowkeybyte);
        Result result =table.get(get);
        byte[] resultbytes = result.getValue(famliyname.getBytes(),colum.getBytes());
        if(resultbytes == null){
            return null;
        }

        return new String(resultbytes);
    }

    /**
     * 插入数据
     */
    public static void putdata(String tablename, String rowkey, String famliyname,String colum,String data) throws Exception {
        Table table = conn.getTable(TableName.valueOf(tablename));
        Put put = new Put(rowkey.getBytes());
        put.addColumn(famliyname.getBytes(),colum.getBytes(),data.getBytes());
        table.put(put);
    }

    /**
     * 修改列族
     */
    private void modifyTable(String tabName, List<String> famliyLst) throws IOException {
        TableName t = TableName.valueOf(tabName);
        HTableDescriptor tab = new HTableDescriptor(TableName.valueOf(tabName));
        // 添加列族,每个表至少有一个列族.
        for(String famliyname:famliyLst){
            HColumnDescriptor colDesc = new HColumnDescriptor(famliyname);
            tab.addFamily(colDesc);
        }
        admin.disableTable(t);
        admin.modifyTable(t, tab);
        admin.enableTableAsync(t);

        System.out.println("HBASE表:"+tabName+"修改成功");
    }
    private byte[][] getSplitKeys() {
        String[] keys = new String[] { "10|", "30|", "50|", "70|", "90|" };
        byte[][] splitKeys = new byte[keys.length][];
        TreeSet<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);//升序排序
        for (int i = 0; i < keys.length; i++) {
            rows.add(Bytes.toBytes(keys[i]));
        }
        Iterator<byte[]> rowKeyIter = rows.iterator();
        int i=0;
        while (rowKeyIter.hasNext()) {
            byte[] tempRow = rowKeyIter.next();
            rowKeyIter.remove();
            splitKeys[i] = tempRow;
            i++;
        }
        return splitKeys;
    }
    
    
    public static void main(String[] args) throws Exception {
        DateTimeFormatter minuteFtf = DateTimeFormatter.ofPattern("yyyyMMddHHmm");
        String minute = minuteFtf.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(System.currentTimeMillis()), ZoneId.of("Asia/Shanghai")));
        System.out.println(StringUtils.substring(minute,8,12));
        //public static void put(String tablename, String rowkey, String famliyname, Map<String,String> datamap)
        Map<String,String> dataMap = new HashMap<>();
        dataMap.put("c1","c1");
        HbaseUtil.putDefault("TEST_SPLIT_T","20b6589fc6ab0dc82cf12099d1c2d40ab994e8410c","F1",dataMap);

        //System.out.println(System.getenv().get("HADOOP_USER_NAME"));
        //HbaseUtil hbaseUtil = new HbaseUtil();
        //List<String> famliyLst = new ArrayList<>();
        //famliyLst.add("F1");
        //hbaseUtil.createTableBySplitKeys("TEST_SPLIT_T",famliyLst);
        //hbaseUtil.createNameSpace("FlinkDBA");
        //hbaseUtil.createTable("ZRR_DAY_MSG_CNT","F1");
        //hbaseUtil.createTable("ZRR_HOUR_MSG_CNT","F1");
        //hbaseUtil.createTable("ZRR_MINUTE_MSG_CNT","F1");
        //hbaseUtil.createTable("DAY_BY_DAY_MSG_CNT","F1");

        //String getdata = HbaseUtil.getdata("ZRR_DAY_MSG_CNT",  "STREAM_ORACLE_1902_CRM01-121-20200107", "F1", "DAY_CNT");
        //System.out.println(getdata);
        //List<String> famliyLst = new ArrayList<>();
        //famliyLst.add("STREAM_ORACLE_9900_CRM01");
        //famliyLst.add("STREAM_ORACLE_9900_CRM02");
        //famliyLst.add("STREAM_ORACLE_9900_CRM03");
        //famliyLst.add("STREAM_ORACLE_9900_CRM04");
        //famliyLst.add("STREAM_ORACLE_9900_CRM05");
        //famliyLst.add("STREAM_ORACLE_9900_CRM06");
        //famliyLst.add("STREAM_ORACLE_9900_CRM07");
        //famliyLst.add("STREAM_ORACLE_9900_CRM08");
        //hbaseUtil.modifyTable("CBSS_ORDER_CNT",famliyLst);
    }
}
