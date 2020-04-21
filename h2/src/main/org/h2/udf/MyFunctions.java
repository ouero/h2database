package org.h2.udf;

public class MyFunctions {
    public static String shobj_description(long oid, String database) {
        return database + " desc";
    }

    public static String col_description(int id, int num) {
        return id + "," + num;
    }
}
