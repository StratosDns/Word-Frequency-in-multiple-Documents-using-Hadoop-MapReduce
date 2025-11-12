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
import java.util.HashMap;

public class Job4_TFIDF {

    public static class Mapper4 extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private final HashMap<String, Double> idfMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            String idfDir = conf.get("tfidf.idfDir");
            if (idfDir == null) throw new IOException("tfidf.idfDir not provided");
            loadIdf(conf, idfDir);
        }

        private void loadIdf(Configuration conf, String idfDir) throws IOException {
            FileSystem fs = FileSystem.get(conf);
            RemoteIterator<LocatedFileStatus> it = fs.listFiles(new Path(idfDir), false);
            while (it.hasNext()) {
                LocatedFileStatus st = it.next();
                String name = st.getPath().getName();
                if (name.startsWith("part-") || name.startsWith("part-r-") || name.startsWith("part-m-")) {
                    try (FSDataInputStream in = fs.open(st.getPath());
                         BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
                        String line;
                        while ((line = br.readLine()) != null) {
                            String[] p = line.split("\t");
                            if (p.length == 2) {
                                idfMap.put(p[0], Double.parseDouble(p[1]));
                            }
                        }
                    }
                } else if (name.startsWith("doctotal") || name.startsWith("tfraw")) {
                    // ignore step1 outputs if the idfDir path was pointed to step1 by mistake
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            String[] parts = line.split("\t");
            if (parts.length != 3) return;
            String term = parts[0];
            String docId = parts[1];
            double tf = Double.parseDouble(parts[2]);
            Double idf = idfMap.get(term);
            if (idf == null) return;
            double tfidf = tf * idf;
            context.write(new Text(docId + "\t" + term), new DoubleWritable(tfidf));
        }
    }

    public static Job configure(Configuration conf, Path inStep2TF, Path idfDir, Path outStep4) throws IOException {
        Job job = Job.getInstance(conf, "A1_BD - Step4 - TFIDF");
        job.setJarByClass(Job4_TFIDF.class);

        job.getConfiguration().set("tfidf.idfDir", idfDir.toString());

        job.setMapperClass(Mapper4.class);
        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        TextInputFormat.addInputPath(job, inStep2TF);
        job.setInputFormatClass(TextInputFormat.class);

        TextOutputFormat.setOutputPath(job, outStep4);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job;
    }
}