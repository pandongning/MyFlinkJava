package com.pdn.apitest.hdfs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class MakeDir {
    public static void main(String[] args) throws IOException {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        FileSystem fileSystem = FileSystem.get(conf);
        boolean mkdirs = fileSystem.mkdirs(new Path("/tt"));
        System.out.println(mkdirs);



    }
}
