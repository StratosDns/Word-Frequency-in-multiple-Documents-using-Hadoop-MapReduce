package com.stratosdns.a1bd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Job3_IDF {

    public static class Mapper3 extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        // Input lines from Step2 TF: "term<TAB>docId<TAB>tf"
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            String[] parts = line.split("\t");
            if (parts.length == 3) {
                String term = parts[0];
                double tf = Double.parseDouble(parts[2]);
                context.write(new Text(term), new DoubleWritable(tf));
            }
        }
    }

    public static class Reducer3 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private int numDocs = 0;

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            String docTotalsDir = conf.get("idf.docTotalsDir");
            if (docTotalsDir == null) {
                throw new IOException("idf.docTotalsDir not provided");
            }
            numDocs = countDocs(conf, new Path(docTotalsDir));
        }

        private int countDocs(Configuration conf, Path docTotalsDir) throws IOException {
            FileSystem fs = docTotalsDir.getFileSystem(conf);
            int count = 0;
            RemoteIterator<LocatedFileStatus> it = fs.listFiles(docTotalsDir, false);
            while (it.hasNext()) {
                LocatedFileStatus st = it.next();
                String name = st.getPath().getName();
                if (name.startsWith("doctotal")) {
                    try (FSDataInputStream in = fs.open(st.getPath());
                         BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
                        while (br.readLine() != null) count++;
                    }
                }
            }
            return count;
        }

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sumTf = 0.0;
            for (DoubleWritable v : values) sumTf += v.get();
            if (sumTf <= 0.0 || numDocs <= 0) return;
            double idf = Math.log((double) numDocs / sumTf);
            context.write(key, new DoubleWritable(idf)); // term \t idf
        }
    }

    public static Job configure(Configuration conf, Path inStep2TF, Path docTotalsDir, Path outStep3) throws IOException {
        Job job = Job.getInstance(conf, "A1_BD - Step3 - IDF");
        job.setJarByClass(Job3_IDF.class);

        job.getConfiguration().set("idf.docTotalsDir", docTotalsDir.toString());

        job.setMapperClass(Mapper3.class);
        job.setReducerClass(Reducer3.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        TextInputFormat.addInputPath(job, inStep2TF);
        job.setInputFormatClass(TextInputFormat.class);

        TextOutputFormat.setOutputPath(job, outStep3);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job;
    }
}