package com.hadoop.app;

import org.apache.commons.io.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

public class WordMean extends Configured implements Tool {

    private double mean = 0;

    private final static Text COUNT = new Text("count");
    private final static Text LENGTH = new Text("length");
    private final static LongWritable ONE = new LongWritable(1);

    public static class WordMeanMapper extends Mapper<Object, Text, Text, LongWritable> {
        private LongWritable wordLen = new LongWritable();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String str = itr.nextToken();
                this.wordLen.set(str.length());
                context.write(LENGTH, this.wordLen);
                context.write(COUNT, ONE);
            }
        }
    }

    public static class WordMeanReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable sum = new LongWritable();

        @Override
        public void reduce(Text key, Iterable<LongWritable> itr, Context context)
                throws IOException, InterruptedException {
            int theSum = 0;
            for (LongWritable val : itr ) {
                theSum += val.get();
            }
            sum.set(theSum);
            context.write(key, sum);
        }
    }

    private double readAndCalcMean(Path path, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path file = new Path(path, "part-r-00000");

        if (!fs.exists(file)) {
            throw new IOException("output not found");
        }

        BufferedReader br = null;

        try {
            br = new BufferedReader(new InputStreamReader(fs.open(file), Charsets.UTF_8));

            long count = 0;
            long length = 0;

            String line;

            while ((line = br.readLine()) != null) {
                StringTokenizer st = new StringTokenizer(line);

                String type = st.nextToken();

                if (type.equals(COUNT.toString())) {
                    String countList = st.nextToken();
                    count = Long.parseLong(countList);
                } else if (type.equals(LENGTH.toString())) {
                    String lengthList = st.nextToken();
                    length = Long.parseLong(lengthList);
                }
            }

            double theMean = (((double) length) / ((double) count));
            System.out.println("The mean is: " + theMean);

            return theMean;
        } finally {
            if (br != null) {
                br.close();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new WordMean(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("aaa");
            return 0;
        }

        Configuration conf = getConf();

        Job job = Job.getInstance(conf, "word mean");
        job.setJarByClass(WordMean.class);
        job.setMapperClass(WordMeanMapper.class);
        job.setCombinerClass(WordMeanReducer.class);
        job.setReducerClass(WordMeanReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputpath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputpath);

        boolean result = job.waitForCompletion(true);
        mean = readAndCalcMean(outputpath, conf);

        return result ? 0 : 1;
    }

    public double getMean() {
        return mean;
    }

}
