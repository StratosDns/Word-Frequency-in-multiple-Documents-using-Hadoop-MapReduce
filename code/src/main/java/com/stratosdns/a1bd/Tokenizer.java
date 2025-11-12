package com.stratosdns.a1bd;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Tokenizer {
    // Optional stopwords. Comment out the "continue" line below to disable filtering.
    private static final List<String> STOP = Arrays.asList(
            "a","an","the","and","or","but","if","then","else","when","of","on","in","to","from","by","for","with","is","are","was","were","be","been","being","it","its"
    );

    public static List<String> tokenize(String line) {
        List<String> out = new ArrayList<>();
        if (line == null) return out;
        String[] toks = line.toLowerCase().split("[^a-z0-9]+");
        for (String t : toks) {
            if (t.isEmpty()) continue;
            if (STOP.contains(t)) continue; // disable this line to count all tokens
            out.add(t);
        }
        return out;
    }
}