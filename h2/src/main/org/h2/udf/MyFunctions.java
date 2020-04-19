package org.h2.udf;

public class MyFunctions {
    public static String shobj_description(long oid, String database) {
        return database + " desc";
    }
}
