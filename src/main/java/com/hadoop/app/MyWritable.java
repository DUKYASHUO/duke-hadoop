package com.hadoop.app;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.*;

public class MyWritable{

    public static void main(String[] args) throws IOException {
        IntWritable writable = new IntWritable(163);
        byte[] bytes = serialize(writable);

        System.out.println(bytes.length);

        IntWritable intWritable = new IntWritable();
        deserialize(intWritable, bytes);
        System.out.println(intWritable.get());
    }

    public static byte[] serialize(Writable writable) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(out);
        writable.write(dataOut);
        dataOut.close();
        return out.toByteArray();
    }

    public static byte[] deserialize(Writable writable, byte[] bytes) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        DataInputStream dataIn = new DataInputStream(in);
        writable.readFields(dataIn);
        dataIn.close();
        return bytes;
    }
}
