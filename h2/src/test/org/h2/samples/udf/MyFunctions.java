package org.h2.samples.udf;

public class MyFunctions {
    public static String shobj_description(long oid, String database) {
        return database + " desc";
    }
}
