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
    static final int QUERIES_PER_ITER = 100;     // per your current loops
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

        void addLatency(long nanos) { latencies.add(nanos); }
        void addMajfltDelta(long delta) { if (delta >= 0) majfltDeltas.add(delta); }

        void sort() {
            latencies.sort(null);
            majfltDeltas.sort(null);
        }

        long p50() { return percentile(latencies, 50); }
        long p90() { return percentile(latencies, 90); }
        long p99() { return percentile(latencies, 99); }

        long majMin() { return listMin(majfltDeltas); }
        long majMax() { return listMax(majfltDeltas); }
        long majP50() { return percentile(majfltDeltas, 50); }
        long majP90() { return percentile(majfltDeltas, 90); }
        long majP99() { return percentile(majfltDeltas, 99); }

        private static long percentile(List<Long> xs, int p) {
            if (xs == null || xs.isEmpty()) return 0L;
            int idx = (int) Math.floor((p / 100.0) * (xs.size() - 1));
            if (idx < 0) idx = 0;
            if (idx >= xs.size()) idx = xs.size() - 1;
            return xs.get(idx);
        }
        private static long listMin(List<Long> xs) { return xs.isEmpty() ? 0L : xs.get(0); }
        private static long listMax(List<Long> xs) { return xs.isEmpty() ? 0L : xs.get(xs.size()-1); }
    }

    public static void main(String[] args) throws Exception {

        String tempData = "temp_data";
        Path dirPath = (args.length == 0) ? Paths.get(tempData) : Paths.get(args[0]);

        String mode = (args.length >= 2) ? args[1] : "both"; // prefetch | no_prefetch | ingest | both
        boolean ingest = "ingest".equalsIgnoreCase(mode);

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
            dropPageCache(); runClearScript();
            Stats s = "prefetch".equalsIgnoreCase(mode) ? searchWithPrefetching(dir) : searchWithoutPrefetching(dir);
            dropPageCache(); runClearScript();

            s.sort();
            System.out.println("Visited: " + s.totalVisited);
            System.out.printf("lat   p50=%dns p90=%dns p99=%dns%n", s.p50(), s.p90(), s.p99());
            System.out.printf("majflt min=%d max=%d p50=%d p90=%d p99=%d%n",
                    s.majMin(), s.majMax(), s.majP50(), s.majP90(), s.majP99());
            return;
        }

        // Comparison mode: multiple iterations, each iteration runs BOTH modes with cache clears
        if ("both".equalsIgnoreCase(mode)) {
            try (IndexReader reader = DirectoryReader.open(dir)) {
                for (int iter = 0; iter < iterations; iter++) {
                    System.out.println("\n=== Iteration " + (iter + 1) + " / " + iterations + " ===");

                    // Alternate order each iteration to avoid order bias
                    boolean prefetchFirst = (iter % 2 == 0);

                    if (prefetchFirst) {
                        dropPageCache(); runClearScript();
                        Stats pre = searchWithPrefetching(reader);
                        dropPageCache(); runClearScript();

                        dropPageCache(); runClearScript();
                        Stats base = searchWithoutPrefetching(reader);
                        dropPageCache(); runClearScript();

                        pre.sort(); base.sort();
                        reportIteration(pre, base);
                    } else {
                        dropPageCache(); runClearScript();
                        Stats base = searchWithoutPrefetching(reader);
                        dropPageCache(); runClearScript();

                        dropPageCache(); runClearScript();
                        Stats pre = searchWithPrefetching(reader);
                        dropPageCache(); runClearScript();

                        pre.sort(); base.sort();
                        reportIteration(pre, base);
                    }
                }
            }
            return;
        }

        System.err.println("Unknown mode: " + mode + " (use prefetch | no_prefetch | both | ingest)");
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
            while (r.readLine() != null) { /* swallow output */ }
        }
        return p.waitFor();
    }

    private static void dropPageCache() {
        try {
            int rc = runCmd("sudo", "sh", "-c", "sync; echo 1 > /proc/sys/vm/drop_caches");
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
    private static Stats searchWithPrefetching(IndexReader reader) throws IOException {
        Stats stats = new Stats();
        for (int i = 0; i < QUERIES_PER_ITER; ++i) {
            long[] countHolder = new long[1];
            int[] range = ranges.get(i);
            int minValue = range[0];
            int maxValue = range[1];
            PointValues.IntersectVisitor intersectVisitor = getIntersectVisitorWithPrefetching(minValue, maxValue, countHolder);

            for (LeafReaderContext lrc : reader.leaves()) {
                PointValues pointValues = lrc.reader().getPointValues("pointField");
                if (pointValues == null) continue;
                PointValues.PointTree pointTree = pointValues.getPointTree();

                long maj0 = readSelfMajflt();
                long t0 = System.nanoTime();

                intersectWithPrefetch(intersectVisitor, pointTree, countHolder);
                pointTree.visitMatchingDocIDs(intersectVisitor);

                long t1 = System.nanoTime();
                long maj1 = readSelfMajflt();

                stats.addLatency(t1 - t0);
                stats.addMajfltDelta((maj0 >= 0 && maj1 >= 0) ? (maj1 - maj0) : -1L);
            }
            stats.totalVisited += countHolder[0];
        }
        return stats;
    }

    private static Stats searchWithoutPrefetching(IndexReader reader) throws IOException {
        Stats stats = new Stats();
        for (int i = 0; i < QUERIES_PER_ITER; ++i) {
            long[] countHolder = new long[1];
            int[] range = ranges.get(i);
            int minValue = range[0];
            int maxValue = range[1];
            PointValues.IntersectVisitor intersectVisitor = getIntersecVisitor(minValue, maxValue, countHolder);

            for (LeafReaderContext lrc : reader.leaves()) {
                PointValues pointValues = lrc.reader().getPointValues("pointField");
                if (pointValues == null) continue;
                PointValues.PointTree pointTree = pointValues.getPointTree();

                long maj0 = readSelfMajflt();
                long t0 = System.nanoTime();

                intersect(intersectVisitor, pointTree, countHolder);

                long t1 = System.nanoTime();
                long maj1 = readSelfMajflt();

                stats.addLatency(t1 - t0);
                stats.addMajfltDelta((maj0 >= 0 && maj1 >= 0) ? (maj1 - maj0) : -1L);
            }
            stats.totalVisited += countHolder[0];
        }
        return stats;
    }

    // Convenience wrappers that open/close reader (single-run)
    private static Stats searchWithPrefetching(Directory dir) throws IOException {
        try (IndexReader reader = DirectoryReader.open(dir)) {
            return searchWithPrefetching(reader);
        }
    }
    private static Stats searchWithoutPrefetching(Directory dir) throws IOException {
        try (IndexReader reader = DirectoryReader.open(dir)) {
            return searchWithoutPrefetching(reader);
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

    // ---------- DO NOT CHANGE BELOW: your visitors & intersect functions ----------
    private static PointValues.IntersectVisitor getIntersectVisitorWithPrefetching(int minValue, int maxValue,
                                                                                   long[] countHolder) {

        return new PointValues.IntersectVisitor() {
            // This version of `visit` gets called when we know that every doc in the current leaf node matches.
            int lastMatchingLeafOrdinal = -1;
            Set<Long> matchingLeafBlocksFPsDocIds = new LinkedHashSet<>();

            @Override
            public void visit(int docID) throws IOException {
                countHolder[0]++;
            }

            @Override
            public void visitAfterPrefetch(int docID) throws IOException {
                //countHolder[0]++;
            }
            // This version of `visit` is called when the current leaf node partially matches the range.
            @Override
            public void visit(int docID, byte[] packedValue) throws IOException {
                int val = IntPoint.decodeDimension(packedValue, 0);
                if (val >= minValue && val < maxValue) {
                    visit(docID);//counter increment automatically
                }
            }

            @Override
            public void setLastMatchingLeafOrdinal(int leafOrdinal) {
                lastMatchingLeafOrdinal = leafOrdinal;
            }

            public int lastMatchingLeafOrdinal() {
                return lastMatchingLeafOrdinal;
            }

            @Override
            public Set<Long> matchingLeafNodesfpDocIds() {
                return matchingLeafBlocksFPsDocIds;
            }

            @Override
            public void matchedLeafFpDocIds(long fp, int count) {
                matchingLeafBlocksFPsDocIds.add(fp);
                countHolder[0] += count;
            };

            @Override
            public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                int minValInPT = IntPoint.decodeDimension(minPackedValue, 0);
                int maxValInPT = IntPoint.decodeDimension(maxPackedValue, 0);
                if (minValInPT >= minValue && maxValInPT <= maxValue) {
                    return PointValues.Relation.CELL_INSIDE_QUERY;
                } else if (minValInPT >= maxValue || maxValInPT <= minValue) {
                    return PointValues.Relation.CELL_OUTSIDE_QUERY;
                }
                return PointValues.Relation.CELL_CROSSES_QUERY;
            }
        };
    }

    private static PointValues.IntersectVisitor getIntersecVisitor(int minValue, int maxValue, long[] countHolder) {
        return new PointValues.IntersectVisitor() {
            // This version of `visit` gets called when we know that every doc in the current leaf node matches.
            @Override
            public void visit(int docID) throws IOException {
                countHolder[0]++;
            }

            // This version of `visit` is called when the current leaf node partially matches the range.
            @Override
            public void visit(int docID, byte[] packedValue) throws IOException {
                int val = IntPoint.decodeDimension(packedValue, 0);
                if (val >= minValue && val < maxValue) {
                    countHolder[0]++;
                }
            }

            @Override
            public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                int minValInPT = IntPoint.decodeDimension(minPackedValue, 0);
                int maxValInPT = IntPoint.decodeDimension(maxPackedValue, 0);
                if (minValInPT >= minValue && maxValInPT <= maxValue) {
                    return PointValues.Relation.CELL_INSIDE_QUERY;
                } else if (minValInPT >= maxValue || maxValInPT <= minValue) {
                    return PointValues.Relation.CELL_OUTSIDE_QUERY;
                }
                return PointValues.Relation.CELL_CROSSES_QUERY;
            }
        };
    }

    public static final void intersectWithPrefetch(PointValues.IntersectVisitor visitor, PointValues.PointTree pointTree, long[] countHolder) throws IOException {
        intersectUptoWithPrefetch(visitor, pointTree, countHolder);
        assert pointTree.moveToParent() == false;
    }

    public static final void intersect(PointValues.IntersectVisitor visitor, PointValues.PointTree pointTree, long[] countHolder) throws IOException {
        intersectUpto(visitor, pointTree, countHolder);
        assert pointTree.moveToParent() == false;
    }

    private static void intersectUpto(PointValues.IntersectVisitor visitor, PointValues.PointTree pointTree, long[] countHolder) throws IOException {
        while (countHolder[0] <= 10_000) {
            PointValues.Relation compare =
                    visitor.compare(pointTree.getMinPackedValue(), pointTree.getMaxPackedValue());
            if (compare == PointValues.Relation.CELL_INSIDE_QUERY) {
                // This cell is fully inside the query shape: recursively add all points in this cell
                // without filtering
                pointTree.visitDocIDs(visitor);
            } else if (compare == PointValues.Relation.CELL_CROSSES_QUERY) {
                // The cell crosses the shape boundary, or the cell fully contains the query, so we fall
                // through and do full filtering:
                if (pointTree.moveToChild()) {
                    continue;
                }
                // TODO: we can assert that the first value here in fact matches what the pointTree
                // claimed?
                // Leaf node; scan and filter all points in this block:
                pointTree.visitDocValues(visitor);
            }
            while (pointTree.moveToSibling() == false) {
                if (pointTree.moveToParent() == false) {
                    return;
                }
            }
        }
    }

    private static void intersectUptoWithPrefetch(PointValues.IntersectVisitor visitor, PointValues.PointTree pointTree, long[] countHolder) throws IOException {
        while (countHolder[0] <= 10_000) {
            PointValues.Relation compare =
                    visitor.compare(pointTree.getMinPackedValue(), pointTree.getMaxPackedValue());
            if (compare == PointValues.Relation.CELL_INSIDE_QUERY) {
                // This cell is fully inside the query shape: recursively add all points in this cell
                // without filtering
                pointTree.prefetchDocIDs(visitor);
            } else if (compare == PointValues.Relation.CELL_CROSSES_QUERY) {
                // The cell crosses the shape boundary, or the cell fully contains the query, so we fall
                // through and do full filtering:
                if (pointTree.moveToChild()) {
                    continue;
                }
                // TODO: we can assert that the first value here in fact matches what the pointTree
                // claimed?
                // Leaf node; scan and filter all points in this block:
                pointTree.visitDocValues(visitor);
            }
            while (pointTree.moveToSibling() == false) {
                if (pointTree.moveToParent() == false) {
                    return;
                }
            }
        }
    }
}
