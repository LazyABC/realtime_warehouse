package com.lazy.realtime.common.util;

import org.apache.hadoop.hbase.client.Admin;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by Smexy on 2023/12/29
 */
public class HBaseUtilTest
{

    @Test
    public void getTableName() {

    }

    @Test
    public void checkNamespaceExists() throws IOException {
        Admin admin = HBaseUtil.getAdmin();
        System.out.println(HBaseUtil.checkNamespaceExists(admin, "test"));
    }

    @Test
    public void checkTableExists() throws IOException {
        Admin admin = HBaseUtil.getAdmin();
        System.out.println(HBaseUtil.checkTableExists(admin, "hbase", "meta"));
    }

    @Test
    public void createNamespace() throws IOException {
        Admin admin = HBaseUtil.getAdmin();
        HBaseUtil.createNamespace(admin,"test1");

    }

    @Test
    public void createTable() throws IOException {
        Admin admin = HBaseUtil.getAdmin();
        HBaseUtil.createTable(admin,"test2","t1","f1","f2");
    }
}