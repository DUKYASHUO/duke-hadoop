package com.hadoop.app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class SequenceFileWriteDemo {

    private static final String[] DATA = {
      "one two, ..11111122222222222222",
      "three four, 3333333333444444444",
      "five six, 555555555555556666666",
      "seven eight, 777777777888888888",
      "nine ten, 999999999991010101010",
    };

    public static void main(String[] args) throws IOException {
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(uri);

        IntWritable key = new IntWritable();
        Text value = new Text();
        SequenceFile.Writer writer = null;

        try {
            writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());

            for(int i = 0; i < 100; i++) {
                key.set(100 - i);
                value.set(DATA[i % DATA.length]);
                System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
                writer.append(key, value);
            }
        } finally {
            IOUtils.closeStream(writer);
        }




    }

}
