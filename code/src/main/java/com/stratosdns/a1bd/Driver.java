package com.stratosdns.a1bd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

public class Driver {

    private static boolean isInteger(String s) {
        if (s == null) return false;
        try {
            Integer.parseInt(s.trim());
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static void deleteIfExists(Configuration conf, Path p) throws Exception {
        org.apache.hadoop.fs.FileSystem fs = p.getFileSystem(conf);
        if (fs.exists(p)) {
            fs.delete(p, true);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage:");
            System.err.println("  hadoop jar a1-bd-tfidf.jar com.stratosdns.a1bd.Driver <hdfs_input_dir> <hdfs_output_base> [topN]");
            System.err.println("  hadoop jar a1-bd-tfidf.jar com.stratosdns.a1bd.Driver <hdfs_input_dir> [topN] <hdfs_output_base>  (accepted too)");
            System.exit(1);
        }

        // Robust parsing that accepts both argument orders
        String inputDir = args[0];
        String outBase;
        int topN = 5; // default

        if (args.length == 2) {
            // <input> <output>
            outBase = args[1];
        } else {
            // args.length >= 3
            String a1 = args[1];
            String a2 = args[2];
            if (isInteger(a2)) {
                // <input> <output> <topN>
                outBase = a1;
                topN = Integer.parseInt(a2);
            } else if (isInteger(a1)) {
                // <input> <topN> <output>  (older variant)
                topN = Integer.parseInt(a1);
                outBase = a2;
            } else {
                // 3rd arg not integer; treat as <input> <output> and ignore extra
                outBase = a1;
                System.err.println("WARN: Third argument is not an integer; using default topN=5. Got: '" + a2 + "'");
            }
        }

        // Debug print of resolved args
        System.err.println("ARGS RESOLVED:");
        System.err.println("  inputDir = " + inputDir);
        System.err.println("  outBase  = " + outBase);
        System.err.println("  topN     = " + topN);

        Configuration conf = new Configuration();

        Path inPath   = new Path(inputDir);
        Path outStep1 = new Path(outBase + "/step1");
        Path outStep2 = new Path(outBase + "/step2_tf");
        Path outStep3 = new Path(outBase + "/step3_idf");
        Path outStep4 = new Path(outBase + "/step4_tfidf");
        Path outTopN  = new Path(outBase + "/top" + topN + "_freq");

        // Clean previous outputs (so reruns don't fail)
        deleteIfExists(conf, outStep1);
        deleteIfExists(conf, outStep2);
        deleteIfExists(conf, outStep3);
        deleteIfExists(conf, outStep4);
        deleteIfExists(conf, outTopN);

        // Step 1: Raw term counts f(t,d) + document totals
        Job j1 = Job1_TermCount.configure(conf, inPath, outStep1);
        if (!j1.waitForCompletion(true)) System.exit(2);

        // Glob paths for named MultipleOutputs from Step1
        Path tfrawGlob    = new Path(outStep1.toString() + "/tfraw*");
        Path doctotalGlob = new Path(outStep1.toString() + "/doctotal*");

        // Step 2: TF(t,d)
        Job j2 = Job2_TF.configure(conf, tfrawGlob, doctotalGlob, outStep2);
        if (!j2.waitForCompletion(true)) System.exit(3);

        // Step 3: IDF(t,D)
        Job j3 = Job3_IDF.configure(conf, outStep2, new Path(outStep1.toString()), outStep3);
        if (!j3.waitForCompletion(true)) System.exit(4);

        // Step 4: TFIDF(t,d,D)
        Job j4 = Job4_TFIDF.configure(conf, outStep2, outStep3, outStep4);
        if (!j4.waitForCompletion(true)) System.exit(5);

        // Top-N (raw frequency) per document
        Job j5 = Job5_TopNByFrequency.configure(conf, tfrawGlob, outTopN, topN);
        if (!j5.waitForCompletion(true)) System.exit(6);

        System.out.println("Pipeline completed.");
        System.out.println("Outputs:");
        System.out.println(" - Step1 (f(t,d) and doc totals): " + outStep1);
        System.out.println(" - Step2 (tf): " + outStep2);
        System.out.println(" - Step3 (idf): " + outStep3);
        System.out.println(" - Step4 (tfidf): " + outStep4);
        System.out.println(" - Top" + topN + " by frequency per document: " + outTopN);
    }
}