package com.stratosdns.a1bd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Job2_TF {

    public static class Mapper2 extends Mapper<LongWritable, Text, Text, Text> {
        // Accepts both tfraw and doctotal lines from Step1
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            String[] parts = line.split("\t");
            if (parts.length == 3) {
                // tfraw: term  docId  f
                String term = parts[0];
                String docId = parts[1];
                String f = parts[2];
                context.write(new Text(docId), new Text("T\t" + term + "\t" + f));
            } else if (parts.length == 2) {
                // doctotal: docId  total
                String docId = parts[0];
                String total = parts[1];
                context.write(new Text(docId), new Text("D\t" + total));
            }
        }
    }

    public static class Reducer2 extends Reducer<Text, Text, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String docId = key.toString();
            int totalTerms = 0;
            List<String[]> termCounts = new ArrayList<>();
            for (Text v : values) {
                String s = v.toString();
                if (s.startsWith("D\t")) {
                    totalTerms += Integer.parseInt(s.substring(2));
                } else if (s.startsWith("T\t")) {
                    String[] parts = s.split("\t");
                    if (parts.length == 3) termCounts.add(new String[]{parts[1], parts[2]});
                }
            }
            if (totalTerms == 0) return;

            for (String[] tf : termCounts) {
                String term = tf[0];
                int f = Integer.parseInt(tf[1]);
                double tfVal = (double) f / (double) totalTerms;
                context.write(new Text(term + "\t" + docId), new DoubleWritable(tfVal));
            }
        }
    }

    public static Job configure(Configuration conf, Path inTfRaw, Path inDocTotals, Path outStep2) throws IOException {
        Job job = Job.getInstance(conf, "A1_BD - Step2 - TF");
        job.setJarByClass(Job2_TF.class);

        job.setMapperClass(Mapper2.class);
        job.setReducerClass(Reducer2.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Accept both inputs (tfraw + doctotal)
        TextInputFormat.addInputPath(job, inTfRaw);
        TextInputFormat.addInputPath(job, inDocTotals);
        job.setInputFormatClass(TextInputFormat.class);

        TextOutputFormat.setOutputPath(job, outStep2);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job;
    }
}