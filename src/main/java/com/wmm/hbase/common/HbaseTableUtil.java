package com.wmm.hbase.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * hbase的工具类
 */
public class HbaseTableUtil {
    private static final Configuration conf;
    public static final Connection CONNECTION;

    //初始化
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.213.212");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            CONNECTION = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void initNameSpace(String nameSpace, String creator) throws IOException {
        try(HBaseAdmin admin = (HBaseAdmin) CONNECTION.getAdmin()) {
            //admin.deleteNamespace(nameSpace);
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor
                                                              .create(nameSpace)
                                                                      .addConfiguration("creator", creator)
                                                                              .addConfiguration("createTime", LocalDateTime.now().toString())
                                                                                      .build();
            admin.createNamespace(namespaceDescriptor);
        }
    }

    public static void listTable() throws IOException {
        try(HBaseAdmin admin = (HBaseAdmin) CONNECTION.getAdmin()) {
            for (HTableDescriptor hTableDescriptor : admin.listTables()) {
                System.out.println(hTableDescriptor.getTableName() + "： ");
                for (HColumnDescriptor hColumnDescriptor : hTableDescriptor.getColumnFamilies()) {
                    System.out.print(hColumnDescriptor.getNameAsString() + " ");
                }
                System.out.println();
            }
        }
    }

    /**
     * 表是否存在
     *
     * @param tableName 表名
     * @return 存在：true
     * @throws IOException
     */
    public static boolean isTableExist(String tableName) throws IOException {
        try(HBaseAdmin admin = (HBaseAdmin) CONNECTION.getAdmin()) {
            return admin.tableExists(tableName);
        }
    }

    /**
     * 创建表
     *
     * @param tableName    表名
     * @param columnFamily 列簇
     * @throws IOException
     */
    public static void createTable(String tableName, String... columnFamily) throws IOException {
        try(HBaseAdmin admin = (HBaseAdmin) CONNECTION.getAdmin()) {

            if (isTableExist(tableName)) {
                return;
            }

            //创建表属性对象,表名需要转字节
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            //创建多个列族
            for (String cf : columnFamily) {
                descriptor.addFamily(new HColumnDescriptor(cf));
            }
            //根据对表的配置，创建表
            admin.createTable(descriptor);
            System.out.println("表" + tableName + "创建成功");
        }
    }

    /**
     * 删除表
     *
     * @param tableName 表名
     * @throws IOException
     */
    public static void dropTable(String tableName) throws IOException {
        try(HBaseAdmin admin = (HBaseAdmin) CONNECTION.getAdmin()) {
            if (isTableExist(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                System.out.println("表" + tableName + "删除成功");
            } else {
                System.out.println("表" + tableName + "不存在");
            }
        }
    }

    /**
     * 向指定表插入数据
     *
     * @param tableName    表名
     * @param rowKey       行键
     * @param columnFamily 列簇
     * @param column       列名
     * @param value        值
     * @throws IOException
     */
    public static void addRowData(String tableName, String rowKey, String columnFamily, String column, String value) throws IOException {
        try(HTable hTable = (HTable) CONNECTION.getTable(TableName.valueOf(tableName))) {
            //向表中插入数据
            Put put = new Put(Bytes.toBytes(rowKey));
            //向 Put 对象中组装数据
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
            hTable.put(put);
            hTable.close();
            System.out.println("向表" + tableName + "插入数据成功");
        }
    }

    /**
     * 删除多行
     *
     * @param tableName 表名
     * @param rows      行
     * @throws IOException
     */
    public static void deleteMultiRow(String tableName, String... rows) throws IOException {
        try(HTable hTable = (HTable) CONNECTION.getTable(TableName.valueOf(tableName))) {
            List<Delete> deleteList = new ArrayList<>();
            for (String row : rows) {
                Delete delete = new Delete(Bytes.toBytes(row));
                deleteList.add(delete);
            }
            hTable.delete(deleteList);
        }
    }

    /**
     * 获取制定表的全部行
     *
     * @param tableName 表名
     * @throws IOException
     */
    public static void getAllRows(String tableName) throws IOException {
        //用于扫描region
        Scan scan = new Scan();
        try(
                HTable hTable = (HTable) CONNECTION.getTable(TableName.valueOf(tableName));
                ResultScanner resultScanner = hTable.getScanner(scan)
        ) {
            //获取扫描结果
            for (Result result : resultScanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    //得到rowKey
                    System.out.println("行键 :" + Bytes.toString(CellUtil.cloneRow(cell)));
                    //得到列族
                    System.out.println("列族 " + Bytes.toString(CellUtil.cloneFamily(cell)));
                    System.out.println("列 :" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                    System.out.println("值 :" + Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
        }
    }

    /**
     * 获取指定行键的行
     *
     * @param tableName 表名
     * @param rowKey    行键
     * @throws IOException
     */
    public static void getRow(String tableName, String rowKey) throws IOException {
        try(HTable table = (HTable) CONNECTION.getTable(TableName.valueOf(tableName))) {
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            for (Cell cell : result.rawCells()) {
                //得到rowKey
                System.out.println("行键 :" + Bytes.toString(CellUtil.cloneRow(cell)));
                //得到列族
                System.out.println("列族 " + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("列 :" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("值 :" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }

    /**
     * 获取某一行指定“列簇：列”的数据
     *
     * @param tableName 表名
     * @param rowKey    行键
     * @param family    列簇
     * @param qualifier 列名
     * @throws IOException
     */
    public static void getRowQualifier(String tableName, String rowKey, String family, String qualifier) throws IOException {
        try(HTable table = (HTable) CONNECTION.getTable(TableName.valueOf(tableName))) {
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
            Result result = table.get(get);
            for (Cell cell : result.rawCells()) {
                //得到rowKey
                System.out.println("行键 :" + Bytes.toString(CellUtil.cloneRow(cell)));
                //得到列族
                System.out.println("列族 " + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("列 :" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("值 :" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }
}
