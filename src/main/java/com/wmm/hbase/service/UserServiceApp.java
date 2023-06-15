package com.wmm.hbase.service;

import com.wmm.hbase.common.HbaseTableUtil;

import java.io.IOException;

public class UserServiceApp {
    public static void main(String[] args) throws IOException {
        UserService userService = new UserService();
        userService.dropTable();
        userService.createTable();
        HbaseTableUtil.listTable();
        userService.dropTable();
        HbaseTableUtil.listTable();
        userService.createTable();
        userService.addRowData("james", "info", "age", "18");
        userService.addRowData("smith", "info", "age", "20");
        userService.addRowData("james", "info", "sex", "male");
        userService.getRow("james");
        System.out.println("--------------------");
        userService.getAllRows();
        System.out.println("--------------------");
        userService.getRowQualifier("smith", "info", "age");
        System.out.println("--------------------");
    }
}
