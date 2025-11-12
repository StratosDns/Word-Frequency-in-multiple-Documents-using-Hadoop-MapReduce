# A1_BD — TF, IDF, TF‑IDF and Top‑N Words with Hadoop MapReduce (Java)

This project computes:
- Raw term frequency per document: f(t,d)
- Term Frequency: tf(t,d)
- Inverse Document Frequency (per your assignment definition): idf(t, D)
- TF‑IDF: tfidf(t,d,D) = tf(t,d) × idf(t,D)
- Top‑N most frequent terms per document (default N = 5)

It is designed for WSL on Windows with Hadoop 3.x and mirrors a typical academic Hadoop assignment structure (similar to your previous Java MapReduce repo).

---

## 1) Definitions (as in the assignment)

Let:
- t be a term (token/word)
- d be a document
- D be the corpus (set of all documents)
- f(t,d) be the raw count of term t in document d

Formulas:
- Term Frequency (relative term frequency within each document)
  
  $$tf(t, d) = \frac{f(t,d)}{\sum_{t' \in d} f(t', d)}$$

- Inverse Document Frequency (assignment’s definition using Σ tf over all docs)
  
  $$idf(t, D) = \log\left(\frac{N}{\sum_{d \in D} tf(t, d)}\right)$$

  where N is the number of documents in D (counted from Step 1 outputs).

- TF‑IDF:
  
  $$tfidf(t, d, D) = tf(t, d) \cdot idf(t, D)$$

Note: This IDF variant differs from the classical log(N/df) but follows your instructions.

---

## 2) Environment and Paths

- Local (WSL):
  - Project code: `/home/stratos/A1_BD/code`
  - Local input files: `/home/stratos/A1_BD/input/` (contains input1.txt, input2.txt, input3.txt)

- HDFS:
  - Corpus input: `/A1_BD/input/`
  - Outputs base (created by the pipeline): `/A1_BD/output/`

Hadoop version used in pom: 3.3.6  
Java: 8 or 11 recommended for Hadoop 3.x

---

## 3) Project Layout

```
/home/stratos/A1_BD/code
├─ pom.xml
├─ README.md
└─ src/main/java/com/stratosdns/a1bd/
   ├─ Driver.java                    # Orchestrates the pipeline; supports flexible argument order
   ├─ Tokenizer.java                 # Tokenization + optional stopword filtering
   ├─ Job1_TermCount.java           # Step 1: f(t,d) and Σ f(t',d) per doc (MultipleOutputs)
   ├─ Job2_TF.java                  # Step 2: tf(t,d) via reduce-side join
   ├─ Job3_IDF.java                 # Step 3: idf(t,D) = log(N / Σ tf(t,d))
   ├─ Job4_TFIDF.java               # Step 4: tfidf(t,d,D)
   └─ Job5_TopNByFrequency.java     # Extra: Top-N raw frequency per document
```

---

## 4) Build (WSL)

```bash
cd /home/stratos/A1_BD/code
mvn -q clean package
```

- Produces a shaded, runnable JAR at:
  `target/a1-bd-tfidf-1.0-SNAPSHOT.jar`

- Verify manifest Main‑Class:
  ```bash
  jar xf target/a1-bd-tfidf-1.0-SNAPSHOT.jar META-INF/MANIFEST.MF
  grep -i '^Main-Class' META-INF/MANIFEST.MF
  # Expected: Main-Class: com.stratosdns.a1bd.Driver
  ```

---

## 5) Input Preparation (HDFS)

Verify HDFS input (already present per your setup):

```bash
hadoop fs -ls /A1_BD/input
# If you ever need to re-upload:
# hadoop fs -mkdir -p /A1_BD/input
# hadoop fs -put -f /home/stratos/A1_BD/input/* /A1_BD/input/
```

---

## 6) Run the Pipeline

Important: Use the JAR’s manifest (do NOT specify the main class on the command line).

- Clean any previous outputs:
  ```bash
  hadoop fs -rm -r -f /A1_BD/output
  ```

- Run (standard order: <input> <output> [topN]):
  ```bash
  hadoop jar target/a1-bd-tfidf-1.0-SNAPSHOT.jar /A1_BD/input /A1_BD/output 5
  ```

The Driver also accepts the alternate order `<input> [topN] <output>`:
```bash
# Example (not required if you already use the standard order):
hadoop jar target/a1-bd-tfidf-1.0-SNAPSHOT.jar /A1_BD/input 5 /A1_BD/output
```

On success, the console ends with:
```
Pipeline completed.
Outputs:
 - Step1 (f(t,d) and doc totals): /A1_BD/output/step1
 - Step2 (tf): /A1_BD/output/step2_tf
 - Step3 (idf): /A1_BD/output/step3_idf
 - Step4 (tfidf): /A1_BD/output/step4_tfidf
 - Top5 by frequency per document: /A1_BD/output/top5_freq
```

---

## 7) Outputs and Formats

List subdirectories:
```bash
hadoop fs -ls /A1_BD/output
```

- Step 1: `/A1_BD/output/step1`
  - `tfraw-*` lines: `term<TAB>docId<TAB>f(t,d)`
  - `doctotal-*` lines: `docId<TAB>Σ f(t',d)`
  - Quick peek:
    ```bash
    hadoop fs -cat /A1_BD/output/step1/tfraw* | head
    hadoop fs -cat /A1_BD/output/step1/doctotal* | head
    ```

- Step 2: `/A1_BD/output/step2_tf`
  - Format: `term<TAB>docId<TAB>tf(t,d)`
  - Peek:
    ```bash
    hadoop fs -cat /A1_BD/output/step2_tf/part-* | head
    ```

- Step 3: `/A1_BD/output/step3_idf`
  - Format: `term<TAB>idf(t,D)`
  - Peek:
    ```bash
    hadoop fs -cat /A1_BD/output/step3_idf/part-* | head
    ```

- Step 4: `/A1_BD/output/step4_tfidf`
  - Format: `docId<TAB>term<TAB>tfidf`
  - Peek:
    ```bash
    hadoop fs -cat /A1_BD/output/step4_tfidf/part-* | head
    ```

- Top‑N by raw frequency: `/A1_BD/output/top5_freq` (if N=5)
  - Format: `docId<TAB>rank<TAB>term<TAB>f(t,d)`
  - Show first 15 rows (3 docs × 5):
    ```bash
    hadoop fs -cat /A1_BD/output/top5_freq/part-* | sort -k1,1 -k2,2n | head -n 15
    ```

Sort TF‑IDF descending within each doc:
```bash
hadoop fs -cat /A1_BD/output/step4_tfidf/part-* | sort -k1,1 -k3,3nr | head -30
```

---

## 8) How the Pipeline Works

- Step 1 (Job1_TermCount)
  - Mapper:
    - For each token in document docId, emit:
      - `("T#"+docId+"\t"+term, 1)` to count f(t,d)
      - `("D#"+docId, 1)` to count total tokens per doc
  - Reducer:
    - Sums and writes two named outputs via MultipleOutputs:
      - `tfraw-*`: `term<TAB>docId<TAB>f(t,d)`
      - `doctotal-*`: `docId<TAB>Σ f(t',d)`

- Step 2 (Job2_TF)
  - Reduce‑side join on docId combining `tfraw-*` with `doctotal-*`.
  - Emits: `term<TAB>docId<TAB>tf(t,d)`

- Step 3 (Job3_IDF)
  - Groups all `tf(t,d)` by term; sums to get `Σ_d tf(t,d)`.
  - Counts N (number of documents) by reading lines of Step 1 `doctotal-*`.
  - Emits: `term<TAB>log(N / Σ_d tf(t,d))`.

- Step 4 (Job4_TFIDF)
  - Loads the IDF table into memory (map‑side join).
  - Reads TF from Step 2 and multiplies: `tfidf = tf × idf`.
  - Emits: `docId<TAB>term<TAB>tfidf`.

- Extra (Job5_TopNByFrequency)
  - Reads Step 1 `tfraw-*` and for each doc keeps a min‑heap of size N.
  - Emits Top‑N terms by raw count per document, ranked 1..N.

---

## 9) Tokenization and Stopwords

Tokenizer behavior:
- Lowercases
- Splits on non‑alphanumeric separators (`[^a-z0-9]+`)
- Removes a small stopword set (e.g., a, an, the, …)
- Note: Contractions like “I’d” become `i` and `d`. If you don’t want single‑letter tokens, add a filter (see below).

To disable stopwords:
- Edit `src/main/java/com/stratosdns/a1bd/Tokenizer.java`
  - Comment out: `if (STOP.contains(t)) continue;`
- Rebuild and re‑run.

To filter single‑letter tokens:
- Add before adding the token: `if (t.length() == 1) continue;`
- Rebuild and re‑run.

---

## 10) Re‑running and Cleaning

- The Driver deletes its own step output directories on each run.
- If you want to clear the base output manually:
  ```bash
  hadoop fs -rm -r -f /A1_BD/output
  ```

---

## 11) Troubleshooting

- NumberFormatException for output path:
  - Cause: running `hadoop jar ... com.stratosdns.a1bd.Driver ...` while the JAR already has a manifest Main‑Class. The explicit class name becomes `args[0]`.
  - Fix: don’t specify the main class; run:
    ```bash
    hadoop jar target/a1-bd-tfidf-1.0-SNAPSHOT.jar /A1_BD/input /A1_BD/output 5
    ```

- IDF 0 or missing:
  - Ensure Step 1 produced `doctotal-*` files.
  - Confirm path passed to Job3 is Step 1 directory (Driver already sets it correctly).

- Empty outputs:
  - Check tokenization didn’t filter everything (disable stopwords temporarily).
  - Confirm `/A1_BD/input` exists and has non‑empty files:
    ```bash
    hadoop fs -ls /A1_BD/input
    hadoop fs -cat /A1_BD/input/* | head
    ```

- ClassNotFound/NoSuchMethod on run:
  - Ensure shaded JAR exists and manifest is correct:
    ```bash
    ls -lh target/a1-bd-tfidf-1.0-SNAPSHOT.jar
    jar xf target/a1-bd-tfidf-1.0-SNAPSHOT.jar META-INF/MANIFEST.MF
    cat META-INF/MANIFEST.MF
    ```

- Mixed argument order complaints:
  - Driver accepts both `<input> <output> [topN]` and `<input> [topN] <output>`. Prefer the first form.

---

## 12) Performance Notes (for bigger corpora)

- Increase reducers if needed by setting `job.setNumReduceTasks(k)` in jobs with reducers (Step 1, 2, 3, 5). The current code leaves default parallelism to Yarn.
- Use a larger block size or combiner for Step 1 if necessary.
- Consider adding a partitioner to spread hot terms/documents evenly.

---

## 13) Validation Tips

- Σ tf(t,d) over all terms t in a doc d should be ~1 (up to floating‑point rounding).
- For a common term like “the,” `Σ_d tf(t,d)` tends to be large; therefore `idf(t,D) = log(N / Σ_d tf(t,d))` becomes smaller.
- Spot‑check a small doc manually: count tokens, compute tf for a term, confirm Step 2’s output.

---

## 14) Extending the Assignment

- Top‑N by TF‑IDF per document:
  - Copy Job5 and adapt it to read Step 4 `docId<TAB>term<TAB>tfidf`, heap by tfidf instead of f.
- Persist IDF across runs:
  - Save Step 3 output in a stable path; reuse it for multiple corpora or rolling updates.

---

## 15) Command Quick Reference

```bash
# Build
cd /home/stratos/A1_BD/code
mvn -q clean package

# Run (standard argument order)
hadoop fs -rm -r -f /A1_BD/output
hadoop jar target/a1-bd-tfidf-1.0-SNAPSHOT.jar /A1_BD/input /A1_BD/output 5

# Inspect
hadoop fs -ls /A1_BD/output
hadoop fs -cat /A1_BD/output/step1/tfraw* | head
hadoop fs -cat /A1_BD/output/step1/doctotal* | head
hadoop fs -cat /A1_BD/output/step2_tf/part-* | head
hadoop fs -cat /A1_BD/output/step3_idf/part-* | head
hadoop fs -cat /A1_BD/output/step4_tfidf/part-* | head
hadoop fs -cat /A1_BD/output/top5_freq/part-* | sort -k1,1 -k2,2n | head -n 15
```

---

## 16) Academic Notes

- This pipeline demonstrates a multi‑stage MapReduce workflow: counting, reduce‑side joins, small‑table map‑side joins, and ranking with bounded heaps.
- Tokenization choices (stopwords, case, punctuation) materially affect both TF and Top‑N results. Document your choices.

---

## 17) License

For academic use in your Big Data assignment. Adapt as needed.