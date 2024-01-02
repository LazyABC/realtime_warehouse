package com.lazy.realtime.common.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @Name: Lazy
 * @Date: 2023/12/29 15:40:35
 * @Details:
 * 套路：
 *     1.创建客户端
 *         表的CRUD：  创建表，删除表，改表。 需要Admin(超级管理员)
 *         数据的CRUD：   插入，更新，删除，查询数据。 需要 Table(对应一张表)
 *
 *         Connection:  App和HBase集群的连接。
 *                         1)可以通过ConnectionFactory创建
 *                         2）负责连接各种服务，这种功能可以被从Conection中创建的Table和Admin所共享。
 *                         3) Connection的创建是重量级(耗费资源和时间)，建议在一个App中只创建一次，在不同的线程中分享(线程安全的)
 *                         4）Table和Admin不是线程安全的。创建方式是轻量级(省时间，省资源)的，每个线程都有自己的Table和Admin。不要共享。
 *
 *         原理：客户端连接hbase集群，只需要知道hbase集群存储元数据的zk的地址即可。
 *
 *    2.提供的功能都是和表的管理相关
 *         提供一个单例连接
 *         创建表
 *         判断表是否存在
 *         创建库
 *         判断库是否存在
 *         构造TableName
 *         返回Admin
 */
public class HBaseUtil {
    //单例。保证一个App只有一个Connection对象
    private static Connection connection;

    static {
        try {
            //在方法的构造中，会读取hadoop的各种配置及hbase的各种配置
            connection = ConnectionFactory.createConnection();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public static void closeConn() throws IOException {
        if (connection != null){
            connection.close();
        }
    }

    //返回负责创建表的客户端
    public static Admin getAdmin() throws IOException {
        return connection.getAdmin();
    }

    //根据库名和表名返回表的TableName
    public static TableName getTableName(String ns,String tn){
        if (StringUtils.isAnyBlank(ns,tn)){
            throw new RuntimeException("表名或库名非法!");
        }else {
            return TableName.valueOf(ns+":"+tn);
        }
    }

    //判断库是否存在
    public static boolean checkNamespaceExists(Admin admin,String ns){
        if (StringUtils.isAnyBlank(ns)){
            throw new RuntimeException("表名或库名非法!");
        }

        try {
            admin.getNamespaceDescriptor(ns);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    //检测表是否存在
    public static boolean checkTableExists(Admin admin,String ns,String tn) throws IOException {
        TableName tableName = getTableName(ns, tn);
        return admin.tableExists(tableName);
    }


    //创建库
    public static void createNamespace(Admin admin,String ns) throws IOException {
        if (StringUtils.isAnyBlank(ns)){
            throw new RuntimeException("表名或库名非法!");
        }

        admin.createNamespace(NamespaceDescriptor.create(ns).build());
    }

    //创建表  create 库名:表名,列族名
    public static void createTable(Admin admin,String ns,String tn,String... cfs) throws IOException {

        //检测参数
        if (cfs.length < 1 || StringUtils.isAnyBlank(ns,tn) ){
            throw new RuntimeException("参数非法!");
        }
        //检测库是否建好
        if (!checkNamespaceExists(admin,ns)){
            //库还不存在，建库
            createNamespace(admin,ns);
        }

        //判断表是否已经存在
        if (!checkTableExists(admin,ns,tn)){
            //准备列族的描述
            List<ColumnFamilyDescriptor> cfds = Arrays.stream(cfs).map(cfName -> ColumnFamilyDescriptorBuilder.of(cfName)).collect(Collectors.toList());

            //准备表的描述
            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(getTableName(ns, tn)).setColumnFamilies(cfds).build();
            //建表
            admin.createTable(tableDescriptor);
        }
    }

    public static void dropTable(Admin admin,String ns,String tn) throws IOException {

        TableName tableName = getTableName(ns, tn);

        //先禁用表
        admin.disableTable(tableName);
        //再删表
        admin.deleteTable(tableName);
    }


    public static Table getTable(String ns,String tn) throws IOException {
        return connection.getTable(getTableName(ns,tn));
    }
    /*
        封装put。
             put 表名 rowkey  列族名:列名 列值
     */
    public static Put getPut(String rk, String cf, String cq, String v){
        //代表队当行的写入操作
        Put put = new Put(Bytes.toBytes(rk));
        //每写入一个cell，可以执行以下方法
        return put.addColumn(
                Bytes.toBytes(cf),
                Bytes.toBytes(cq),
                Bytes.toBytes(v)
        );
    }

    public static void printResult(Result result) {
        //Result可以看做Cell的集合
        Cell[] cells = result.rawCells();

        for (Cell cell : cells) {
            System.out.println(
                    "rk:"+ Bytes.toString(CellUtil.cloneRow(cell)) +","+
                            Bytes.toString(CellUtil.cloneFamily(cell)) +":"+
                            Bytes.toString(CellUtil.cloneQualifier(cell)) +" :"+
                            Bytes.toString(CellUtil.cloneValue(cell)) +",type:"+
                            cell.getType() +","+
                            cell.getTimestamp()
            );
        }
    }
}
