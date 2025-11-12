package com.stratosdns.a1bd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.List;

public class Job1_TermCount {

    public static class Mapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);
        private String docId;

        @Override
        protected void setup(Context context) throws IOException {
            FileSplit split = (FileSplit) context.getInputSplit();
            Path p = split.getPath();
            docId = p.getName(); // document identifier is the filename
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            List<String> tokens = Tokenizer.tokenize(value.toString());
            for (String tok : tokens) {
                context.write(new Text("T#" + docId + "\t" + tok), ONE); // term occurrence
                context.write(new Text("D#" + docId), ONE);              // doc total counter
            }
        }
    }

    public static class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {
        private MultipleOutputs<Text, IntWritable> mos;

        @Override
        protected void setup(Context context) {
            mos = new MultipleOutputs<>(context);
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values) sum += v.get();
            String k = key.toString();
            if (k.startsWith("T#")) {
                String rest = k.substring(2);
                String[] parts = rest.split("\t", 2);
                String docId = parts[0];
                String term = parts.length > 1 ? parts[1] : "";
                mos.write("tfraw", new Text(term + "\t" + docId), new IntWritable(sum));
            } else if (k.startsWith("D#")) {
                String docId = k.substring(2);
                mos.write("doctotal", new Text(docId), new IntWritable(sum));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }

    public static Job configure(Configuration conf, Path input, Path outStep1) throws IOException {
        Job job = Job.getInstance(conf, "A1_BD - Step1 - TermCount and DocTotals");
        job.setJarByClass(Job1_TermCount.class);

        job.setMapperClass(Mapper1.class);
        job.setReducerClass(Reducer1.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        TextInputFormat.addInputPath(job, input);
        job.setInputFormatClass(TextInputFormat.class);

        TextOutputFormat.setOutputPath(job, outStep1);
        job.setOutputFormatClass(TextOutputFormat.class);

        MultipleOutputs.addNamedOutput(job, "tfraw", TextOutputFormat.class, Text.class, IntWritable.class);
        MultipleOutputs.addNamedOutput(job, "doctotal", TextOutputFormat.class, Text.class, IntWritable.class);

        return job;
    }
}