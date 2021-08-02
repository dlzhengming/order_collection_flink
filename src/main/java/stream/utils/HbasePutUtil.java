package stream.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;

public class HbasePutUtil {
    /** 审计集群
     private static final String TABLE_NAME = "ORDER_RELATION_TABLE";
     private static final String ZOOKEEPER_QUORUM = "audit-dp04:2181,audit-dp05:2181,audit-dp06:2181";
     private static final String ZOOKEEPER_ZNODE_PARENT = "/hbase-unsecure"; */

    /** 灰度  */
    private static final String TABLE_NAME = "ORDER_RELATION_TABLE";
    private static final String ZOOKEEPER_ZNODE_PARENT = "/hbase-unsecure";
    private static final String ZOOKEEPER_QUORUM = "hdn03.hadoop.unicom:2181,hdn07.hadoop.unicom:2181,hdn12.hadoop.unicom:2181";

    /** 连调
     private static final String TABLE_NAME = "ORDER_RELATION_TABLE";
     private static final String ZOOKEEPER_ZNODE_PARENT = "/hbase-unsecure";
     private static final String ZOOKEEPER_QUORUM = "dn31.hadoop.unicom:2181,dn32.hadoop.unicom:2181,dn33.hadoop.unicom:2181"; */

    /**
     * 生产
     private static final String TABLE_NAME = "ORDER_RELATION_TABLE";
     * private static final String ZOOKEEPER_ZNODE_PARENT = "/hbase-unsecure";
     * private static final String ZOOKEEPER_QUORUM = "dn01.hadoop.unicom:2181,dn08.hadoop.unicom:2181,dn16.hadoop.unicom:2181";
     */
    private static Admin admin = null;
    private static Connection conn = null;
    static{
        Configuration conf = HBaseConfiguration.create();
        conf.set(HConstants.ZOOKEEPER_QUORUM, ZOOKEEPER_QUORUM);
        conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, ZOOKEEPER_ZNODE_PARENT);
        conf.set("hadoop.user.name", "ocdp");
        try {
            conn = ConnectionFactory.createConnection(conf);
            // 得到管理程序
            admin = conn.getAdmin();
        } catch (IOException e) {
            System.err.println(LocalDateTime.now()+"HBaseConfiguration.create(),"+e);
        }
    }

    /**
     * 插入数据
     */
    public static void put(List<Put> puts) throws Exception {
        Table table = conn.getTable(TableName.valueOf(TABLE_NAME));
        try {
            table.put(puts);
        }catch (Exception e){
            System.err.println(LocalDateTime.now()+","+e);
        }
    }
}
