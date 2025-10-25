package com.sps.lucene.learnings;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class LuceneBKDTraversalPrefetchBenchmark {

    static Random base = new Random(42);
    static final int MEASURE_ITERS_DEFAULT = 10;   // default number of A/B iterations
    static final int QUERIES_PER_ITER = 10000;     // per your current loops
    static List<int[]> ranges = new ArrayList<>(QUERIES_PER_ITER);

    static {
        for (int i = 0; i < QUERIES_PER_ITER; i++) {
            int a = base.nextInt(100_000_000);
            int b = base.nextInt(100_000_000);
            int lo = Math.min(a, b);
            int hi = Math.max(a, b) + 1; // half-open [lo, hi)
            ranges.add(new int[]{lo, hi});
        }
    }

    // ---------- simple stats container ----------
    static class Stats {
        final List<Long> latencies = new ArrayList<>();
        final List<Long> majfltDeltas = new ArrayList<>();
        long totalVisited = 0;

        void addLatency(long nanos) {
            latencies.add(nanos);
        }

        void addMajfltDelta(long delta) {
            if (delta >= 0) majfltDeltas.add(delta);
        }

        void sort() {
            latencies.sort(null);
            majfltDeltas.sort(null);
        }

        long p50() {
            return percentile(latencies, 50);
        }

        long p90() {
            return percentile(latencies, 90);
        }

        long p99() {
            return percentile(latencies, 99);
        }

        long majMin() {
            return listMin(majfltDeltas);
        }

        long majMax() {
            return listMax(majfltDeltas);
        }

        long majP50() {
            return percentile(majfltDeltas, 50);
        }

        long majP90() {
            return percentile(majfltDeltas, 90);
        }

        long majP99() {
            return percentile(majfltDeltas, 99);
        }

        private static long percentile(List<Long> xs, int p) {
            if (xs == null || xs.isEmpty()) return 0L;
            int idx = (int) Math.floor((p / 100.0) * (xs.size() - 1));
            if (idx < 0) idx = 0;
            if (idx >= xs.size()) idx = xs.size() - 1;
            return xs.get(idx);
        }

        private static long listMin(List<Long> xs) {
            return xs.isEmpty() ? 0L : xs.get(0);
        }

        private static long listMax(List<Long> xs) {
            return xs.isEmpty() ? 0L : xs.get(xs.size() - 1);
        }
    }

    static String DATA = "/home/ec2-user/workspace/bkd-prefetch/lucene-learnings/temp_data";


    private static void clean(String DATA) {
        if (HOT_PATH == true) return;
        vmtouchEvictAll(DATA);     // proactive eviction per-file
        vmtouchReport(DATA);
        dropPageCache();
        runSync();
        runClearScript();
        runSync();
        dropPageCache();
        runFincoreCheck(DATA);
        runClearScript();
        runSync();
    }

    static boolean HOT_PATH = false;

    public static void main(String[] args) throws Exception {

        String tempData = "temp_data";
        Path dirPath = (args.length == 0) ? Paths.get(tempData) : Paths.get(args[0]);

        String mode = (args.length >= 2) ? args[1] : "both"; // prefetch | no_prefetch | ingest | both
        boolean ingest = "ingest".equalsIgnoreCase(mode);
        HOT_PATH = args.length >= 3 && Boolean.parseBoolean(args[2]);
        int iterations = MEASURE_ITERS_DEFAULT;

        // parse optional flags
        for (int i = 2; i < args.length; i++) {
            if ("--iters".equalsIgnoreCase(args[i]) && i + 1 < args.length) {
                iterations = Integer.parseInt(args[++i]);
            }
        }

        Directory dir = FSDirectory.open(dirPath);

        if (ingest) {
            if (DirectoryReader.indexExists(dir) == false) {
                IndexWriterConfig indexWriterConfig = new IndexWriterConfig()
                        .setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND)
                        .setRAMBufferSizeMB(10240);
                try (IndexWriter w = new IndexWriter(dir, indexWriterConfig)) {
                    ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
                    AtomicLong indexed = new AtomicLong(0);
                    for (int task = 0; task < 1000; ++task) {
                        executor.execute(() -> {
                            Random r = ThreadLocalRandom.current();
                            for (int i1 = 0; i1 < 1000; ++i1) {
                                Document doc = new Document();
                                for (int j = 0; j < 10_000; ++j) {
                                    // Keeping your original field type
                                    doc.add(new IntField("pointField", r.nextInt(100_000_000), Field.Store.NO));
                                }
                                try {
                                    w.addDocument(doc);
                                } catch (IOException e) {
                                    throw new UncheckedIOException(e);
                                }
                                final long actualIndexed = indexed.incrementAndGet();
                                if (actualIndexed % 10_000 == 0) {
                                    System.out.println("Indexed: " + actualIndexed);
                                }
                            }
                        });
                    }

                    executor.shutdown();
                    executor.awaitTermination(1, TimeUnit.DAYS);
                    w.commit();
                    System.out.println("Start force merging");
                    w.forceMerge(1);
                    System.out.println("Done force merging");
                    w.commit();
                }
            }
            return;
        }

        // Single-mode quick path
        if ("prefetch".equalsIgnoreCase(mode) || "no_prefetch".equalsIgnoreCase(mode)) {
            clean(DATA);
            Stats s = "prefetch".equalsIgnoreCase(mode) ? searchWithPrefetching(dir) : searchWithoutPrefetching(dir);
            clean(DATA);
            s.sort();
            System.out.println("Visited: " + s.totalVisited);
            System.out.printf("lat   p50=%dns p90=%dns p99=%dns%n", s.p50(), s.p90(), s.p99());
            System.out.printf("majflt min=%d max=%d p50=%d p90=%d p99=%d%n",
                    s.majMin(), s.majMax(), s.majP50(), s.majP90(), s.majP99());
            return;
        }

        // Comparison mode: multiple iterations, each iteration runs BOTH modes with cache clears

        System.err.println("Unknown mode: " + mode + " (use prefetch | no_prefetch | both | ingest)");
    }

    // --- vmtouch helpers ---

    // Evict every file under `root` from the OS page cache using vmtouch -e
// Equivalent to: find root -type f -print0 | xargs -0 -I {} sudo vmtouch -e "{}"
    private static void vmtouchEvictAll(String root) {
        String cmd = "find " + shQuote(root) + " -type f -print0"
                + " | xargs -0 -I {} sudo vmtouch -q -e {}";
        execAndStreamBash(cmd, "[VMTOUCH-EVICT]");
    }

    // Report resident pages for all files under `root`
// Equivalent to: find root -type f -print0 | xargs -0 -n 128 vmtouch
    private static void vmtouchReport(String root) {
        String cmd = "find " + shQuote(root) + " -type f -print0"
                + " | xargs -0 -n 128 vmtouch";
        execAndStreamBash(cmd, "[VMTOUCH-REPORT]");
    }

    // Return true if vmtouch binary exists (so we can skip gracefully)
    private static boolean hasVmtouch() {
        int rc = execAndWait("bash", "-lc", "command -v vmtouch >/dev/null 2>&1");
        return rc == 0;
    }

    // --- generic shell helpers you already have variants of; reuse or add these ---
    private static int execAndWait(String... argv) {
        try {
            Process p = new ProcessBuilder(argv).redirectErrorStream(true).start();
            try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
                while (r.readLine() != null) { /* swallow output */ }
            }
            return p.waitFor();
        } catch (Exception e) {
            System.err.println("[EXEC] " + String.join(" ", argv) + " failed: " + e);
            return -1;
        }
    }

    private static void execAndStreamBash(String command, String tag) {
        try {
            Process p = new ProcessBuilder("bash", "-lc", command)
                    .redirectErrorStream(true).start();
            try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
                String line;
                boolean any = false;
                while ((line = r.readLine()) != null) {
                    any = true;
                    System.out.println(tag + " " + line);
                }
                int rc = p.waitFor();
                if (!any) System.out.println(tag + " (no output)");
                if (rc != 0) System.err.println(tag + " non-zero exit " + rc);
            }
        } catch (Exception e) {
            System.err.println(tag + " error: " + e);
        }
    }

    private static String shQuote(String s) {
        return "'" + s.replace("'", "'\\''") + "'";
    }

    private static void runFincoreCheck(String root) {
        // Shows only files with non-zero cached/resident pages.
        String cmd = String.join(" ",
                "bash", "-lc",
                "'find", escape(root), "-type f -print0",
                "| xargs -0 -n 128 fincore 2>/dev/null",
                "| egrep -i \"cached|resident\"",
                "| egrep -v \"(:|\\s)(0|0 pages|0 cached|resident: 0)\"'",
                ""
        ).trim();
        execAndStream(cmd, "[FINCORE]");
    }

    private static void execAndStream(String command, String tag) {
        try {
            Process p = new ProcessBuilder("bash", "-lc", command)
                    .redirectErrorStream(true)
                    .start();
            try (java.io.BufferedReader r =
                         new java.io.BufferedReader(new java.io.InputStreamReader(p.getInputStream()))) {
                String line;
                boolean printed = false;
                while ((line = r.readLine()) != null) {
                    printed = true;
                    System.out.println(tag + " " + line);
                }
                int rc = p.waitFor();
                if (!printed) {
                    System.out.println(tag + " (no output)");
                }
                if (rc != 0) {
                    System.err.println(tag + " non-zero exit " + rc);
                }
            }
        } catch (Exception e) {
            System.err.println(tag + " error: " + e);
        }
    }

    private static String escape(String s) {
        return s.replace("'", "'\\''"); // minimal shell-safe escaping for single quotes
    }

    private static long median(List<Long> xs) {
        if (xs.isEmpty()) return 0;
        xs.sort(null);
        return xs.get(xs.size() / 2);
    }

    // ---------- Cache drop & script helpers (called before/after each run) ----------
    private static int runCmd(String... argv) throws IOException, InterruptedException {
        Process p = new ProcessBuilder(argv).redirectErrorStream(true).start();
        try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
            //String line = null;
            while (r.readLine() != null) {
                //  System.out.println(r.);/* swallow output */
            }
        }
        return p.waitFor();
    }

    private static void dropPageCache() {
        try {
            int rc = runCmd("sudo", "sh", "-c", "sync; echo 3 > /proc/sys/vm/drop_caches");
            if (rc != 0) System.err.println("[WARN] drop_caches rc=" + rc);
            Thread.sleep(100); // small settle
        } catch (Exception e) {
            System.err.println("[WARN] Failed to drop page cache: " + e);
        }
    }

    private static void runClearScript() {
        try {
            int rc = runCmd("sudo", "sh", "/home/ec2-user/scripts/clear_page_cache.sh");
            if (rc != 0) System.err.println("[WARN] clear_page_cache.sh rc=" + rc);
        } catch (Exception e) {
            System.err.println("[WARN] Failed running clear_page_cache.sh: " + e);
        }
    }

    private static void runSync() {
        try {
            int rc = runCmd("sudo", "sync");
            if (rc != 0) System.err.println("[WARN] sudo sync rc=" + rc);
        } catch (Exception e) {
            System.err.println("[WARN] Failed running sudo sync: " + e);
        }
    }

    // ---------- /proc majflt sampling ----------
    // Reads /proc/self/stat and returns the cumulative major page faults (field 12: majflt).
    private static long readSelfMajflt() {
        // /proc/[pid]/stat: split after the last ')' to avoid spaces in comm
        try (RandomAccessFile raf = new RandomAccessFile("/proc/self/stat", "r")) {
            String line = raf.readLine();
            if (line == null) return -1L;
            int rp = line.lastIndexOf(')');
            if (rp < 0 || rp + 2 > line.length()) return -1L; // require ") "
            String tail = line.substring(rp + 2); // skip ") "
            String[] f = tail.split("\\s+");
            // In tail, fields start at original #3 (state). majflt is original #12.
            // Mapping: original idx -> tail idx = idx - 3
            // So majflt (12) -> tail[9]
            if (f.length < 10) return -1L;
            return Long.parseLong(f[9]);
        } catch (Exception e) {
            return -1L;
        }
    }

    // ---------- SEARCH METHODS (return Stats) ----------
    // Overload accepting IndexReader to reuse across iterations without reopening
    private static Stats search(IndexReader reader) throws IOException {
        Stats stats = new Stats();
        IndexSearcher searcher = new IndexSearcher(reader);
        TotalHitCountCollector thc = new TotalHitCountCollector();
        for (int i = 0; i < QUERIES_PER_ITER; ++i) {
            long[] countHolder = new long[1];
            int[] range = ranges.get(i);
            Query pointQuery = IntPoint.newRangeQuery("pointField", range[0], range[1]);
            long maj0 = readSelfMajflt();
            long t0 = System.nanoTime();
            searcher.search(pointQuery, thc);
            long t1 = System.nanoTime();
            long maj1 = readSelfMajflt();
            stats.addLatency(t1 - t0);
            stats.addMajfltDelta((maj0 >= 0 && maj1 >= 0) ? (maj1 - maj0) : -1L);
            stats.totalVisited += thc.getTotalHits();
        }
        return stats;
    }

    // Convenience wrappers that open/close reader (single-run)
    private static Stats searchWithPrefetching(Directory dir) throws IOException {
        try (IndexReader reader = DirectoryReader.open(dir)) {
            return search(reader);
        }
    }

    private static Stats searchWithoutPrefetching(Directory dir) throws IOException {
        try (IndexReader reader = DirectoryReader.open(dir)) {
            return search(reader);
        }
    }

    private static void reportIteration(Stats pre, Stats base) {
        System.out.println("Visited (prefetch):    " + pre.totalVisited);
        System.out.println("Visited (no-prefetch): " + base.totalVisited);
        if (pre.totalVisited != base.totalVisited) {
            System.out.println("WARNING: Different doc visit counts; timings may not be strictly comparable.");
        }
        System.out.printf("prefetch   lat p50=%dns p90=%dns p99=%dns%n", pre.p50(), pre.p90(), pre.p99());
        System.out.printf("no-pref    lat p50=%dns p90=%dns p99=%dns%n", base.p50(), base.p90(), base.p99());
        System.out.printf("prefetch   majflt min=%d max=%d p50=%d p90=%d p99=%d%n",
                pre.majMin(), pre.majMax(), pre.majP50(), pre.majP90(), pre.majP99());
        System.out.printf("no-pref    majflt min=%d max=%d p50=%d p90=%d p99=%d%n",
                base.majMin(), base.majMax(), base.majP50(), base.majP90(), base.majP99());
    }


}
