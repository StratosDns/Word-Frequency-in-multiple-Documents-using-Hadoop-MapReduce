package com.stratosdns.a1bd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

public class Job5_TopNByFrequency {

    public static class Mapper5 extends Mapper<Object, Text, Text, Text> {
        // Input from Step1 tfraw: "term<TAB>docId<TAB>f"
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            String[] parts = line.split("\t");
            if (parts.length == 3) {
                String term = parts[0];
                String docId = parts[1];
                String f = parts[2];
                context.write(new Text(docId), new Text(term + "\t" + f));
            }
        }
    }

    public static class Reducer5 extends Reducer<Text, Text, Text, Text> {
        private int N;

        @Override
        protected void setup(Context context) {
            N = context.getConfiguration().getInt("topn.N", 5);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Min-heap by frequency; keep only N largest
            PriorityQueue<String[]> pq = new PriorityQueue<>(Comparator.comparingInt(a -> Integer.parseInt(a[1])));
            for (Text v : values) {
                String[] parts = v.toString().split("\t");
                if (parts.length != 2) continue;
                String term = parts[0];
                int f = Integer.parseInt(parts[1]);
                pq.offer(new String[]{term, String.valueOf(f)});
                if (pq.size() > N) pq.poll();
            }
            // Emit in descending order
            PriorityQueue<String[]> out = new PriorityQueue<>((a, b) -> Integer.parseInt(b[1]) - Integer.parseInt(a[1]));
            out.addAll(pq);
            int rank = 1;
            while (!out.isEmpty()) {
                String[] e = out.poll();
                context.write(key, new Text(rank + "\t" + e[0] + "\t" + e[1]));
                rank++;
            }
        }
    }

    public static Job configure(Configuration conf, Path inTfRaw, Path outTopN, int N) throws IOException {
        Job job = Job.getInstance(conf, "A1_BD - TopN By Frequency");
        job.setJarByClass(Job5_TopNByFrequency.class);

        job.getConfiguration().setInt("topn.N", N);

        job.setMapperClass(Mapper5.class);
        job.setReducerClass(Reducer5.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.addInputPath(job, inTfRaw);
        job.setInputFormatClass(TextInputFormat.class);

        TextOutputFormat.setOutputPath(job, outTopN);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job;
    }
}