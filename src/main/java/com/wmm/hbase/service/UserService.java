package com.wmm.hbase.service;

import com.wmm.hbase.common.HbaseTableUtil;

import java.io.IOException;

import static com.wmm.hbase.common.HbaseTableUtil.*;

public class UserService {
    private String tableName = "user";

    public boolean isTableExisted() throws IOException {
        return isTableExist(tableName);
    }

    public void createTable() throws IOException {
        String[] columnFamily = {"info"};
        HbaseTableUtil.createTable(tableName, columnFamily);
    }

    public void dropTable() throws IOException {
        HbaseTableUtil.dropTable(tableName);
    }

    public void addRowData(String rowKey, String columnFamily, String column, String value) throws IOException {
        HbaseTableUtil.addRowData(tableName, rowKey, columnFamily, column, value);
    }

    public void deleteMultiRow(String... rows) throws IOException {
        HbaseTableUtil.deleteMultiRow(tableName, rows);
    }

    public void getAllRows() throws IOException {
        HbaseTableUtil.getAllRows(tableName);
    }

    public void getRow(String rowKey) throws IOException {
        HbaseTableUtil.getRow(tableName, rowKey);
    }

    public void getRowQualifier(String rowKey, String family, String qualifier) throws IOException {
        HbaseTableUtil.getRowQualifier(tableName, rowKey, family, qualifier);
    }
}
