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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
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
        long totalVisited = 0;
        void addLatency(long nanos) { latencies.add(nanos); }
        long p50() { return latencies.get(latencies.size() / 2); }
        long p90() { return latencies.get(latencies.size() * 9 / 10); }
        long p99() { return latencies.get(latencies.size() * 99 / 100); }
        void sort() { latencies.sort(null); }
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
                                    // NOTE: Keeping your original field type, per your request not to change visitors/intersect usage
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


        // Comparison mode: multiple iterations, each iteration runs BOTH modes with cache clears
        if ("both".equalsIgnoreCase(mode)) {
            List<Long> allP50Pref = new ArrayList<>();
            List<Long> allP50Base  = new ArrayList<>();
            List<Long> allP90Pref = new ArrayList<>();
            List<Long> allP90Base  = new ArrayList<>();
            List<Long> allP99Pref = new ArrayList<>();
            List<Long> allP99Base  = new ArrayList<>();

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
                        allP50Pref.add(pre.p50()); allP50Base.add(base.p50());
                        allP90Pref.add(pre.p90()); allP90Base.add(base.p90());
                        allP99Pref.add(pre.p99()); allP99Base.add(base.p99());
                    } else {
                        dropPageCache(); runClearScript();
                        Stats base = searchWithoutPrefetching(reader);
                        dropPageCache(); runClearScript();

                        dropPageCache(); runClearScript();
                        Stats pre = searchWithPrefetching(reader);
                        dropPageCache(); runClearScript();

                        pre.sort(); base.sort();
                        reportIteration(pre, base);
                        allP50Pref.add(pre.p50()); allP50Base.add(base.p50());
                        allP90Pref.add(pre.p90()); allP90Base.add(base.p90());
                        allP99Pref.add(pre.p99()); allP99Base.add(base.p99());
                    }
                }
            }

            // Final rollup
            System.out.println("\n=== Summary over " + iterations + " iterations ===");
            System.out.printf("prefetch  median(p50)=%dns   no-prefetch median(p50)=%dns%n",
                    median(allP50Pref), median(allP50Base));
            System.out.printf("prefetch  median(p90)=%dns   no-prefetch median(p90)=%dns%n",
                    median(allP90Pref), median(allP90Base));
            System.out.printf("prefetch  median(p99)=%dns   no-prefetch median(p99)=%dns%n",
                    median(allP99Pref), median(allP99Base));
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

    // ---------- SEARCH METHODS (now return Stats) ----------
    // Overload accepting IndexReader to reuse across iterations without reopening
    private static Stats searchWithPrefetching(IndexReader reader) throws IOException {
        Stats stats = new Stats();
        List<Long> latencies = stats.latencies;
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
                long startTime = System.nanoTime();
                intersectWithPrefetch(intersectVisitor, pointTree, countHolder);
                pointTree.visitMatchingDocIDs(intersectVisitor);
                long endTime = System.nanoTime();
                latencies.add(endTime - startTime);
            }
            stats.totalVisited += countHolder[0];
        }
        return stats;
    }

    private static Stats searchWithoutPrefetching(IndexReader reader) throws IOException {
        Stats stats = new Stats();
        List<Long> latencies = stats.latencies;
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
                long startTime = System.nanoTime();
                intersect(intersectVisitor, pointTree, countHolder);
                long endTime = System.nanoTime();
                latencies.add(endTime - startTime);
            }
            stats.totalVisited += countHolder[0];
        }
        return stats;
    }


    private static void reportIteration(Stats pre, Stats base) {
        System.out.println("Visited (prefetch):   " + pre.totalVisited);
        System.out.println("Visited (no-prefetch): " + base.totalVisited);
        if (pre.totalVisited != base.totalVisited) {
            System.out.println("WARNING: Different doc visit counts; timings may not be strictly comparable.");
        }
        System.out.printf("prefetch   p50=%dns p90=%dns p99=%dns%n", pre.p50(), pre.p90(), pre.p99());
        System.out.printf("no-pref    p50=%dns p90=%dns p99=%dns%n", base.p50(), base.p90(), base.p99());
    }


    private static void searchWithPrefetching(Directory dir) throws IOException {
        List<Long> latencies = new ArrayList<>();
//        List<Long> traversalsTimes   = new ArrayList<>();
//        List<Long> visitTimes      = new ArrayList<>();
        try (IndexReader reader = DirectoryReader.open(dir)) {
            Random r = ThreadLocalRandom.current();

            for (int i = 0; i < 1000; ++i) {
                //long start = System.nanoTime();
                long[] countHolder = new long[1];
                int minValue = r.nextInt(1000);
                int maxValue = 1000 + r.nextInt(100_000_000);
                PointValues.IntersectVisitor intersectVisitor = getIntersectVisitorWithPrefetching(minValue, maxValue, countHolder);
                for (LeafReaderContext lrc : reader.leaves()) {

                    PointValues pointValues = lrc.reader().getPointValues("pointField");
                    PointValues.PointTree pointTree = pointValues.getPointTree();
                    long startTime = System.nanoTime();
                    intersectWithPrefetch(intersectVisitor, pointTree, countHolder);
//                    long traversalTime = System.nanoTime() - startTime;
//                    long visitStartTime = System.nanoTime();
                    pointTree.visitMatchingDocIDs(intersectVisitor);
//                    long visitTime = System.nanoTime() - visitStartTime;
                    long endTime = System.nanoTime();
                    latencies.add(endTime - startTime);
//                    traversalsTimes.add(traversalTime);
//                    visitTimes.add(visitTime);
                }
            }
        }

        latencies.sort(null);
//        visitTimes.sort(null);
//        traversalsTimes.sort(null);

        long p50nanos = latencies.get(latencies.size() / 2);
        long p90nanos = latencies.get(latencies.size() * 9 / 10);
        long p99nanos = latencies.get(latencies.size() * 99 / 100);

//        long p50TreeTraversalnanos = traversalsTimes.get(traversalsTimes.size() / 2);
//        long p90TreeTraversalnanos = traversalsTimes.get(traversalsTimes.size() * 9 / 10);
//        long p99TreeTraversalnanos = traversalsTimes.get(traversalsTimes.size() * 99 / 100);
//
//        long p50Visitnanos = visitTimes.get(visitTimes.size() / 2);
//        long p90Visitnanos = visitTimes.get(visitTimes.size() * 9 / 10);
//        long p99Visitnanos = visitTimes.get(visitTimes.size() * 99 / 100);

        System.out.println("p50 :" + p50nanos + " nanos " );
        System.out.println("p90 : " + p90nanos + " nanos ");
        System.out.println("p99 : " + p99nanos + " nanos ");
//        System.out.println("p50Tree traversal : " + p50TreeTraversalnanos + " nanos ");
//        System.out.println("p90Tree traversal : " + p90TreeTraversalnanos + " nanos ");
//        System.out.println("p99Tree traversal : " + p99TreeTraversalnanos + " nanos ");
//        System.out.println("p50Visit : " + p50Visitnanos + " nanos ");
//        System.out.println("p90Visit : " + p90Visitnanos + " nanos ");
//        System.out.println("p99Visit : " + p99Visitnanos + " nanos ");
    }

    private static void searchWithoutPrefetching(Directory dir) throws IOException {
        List<Long> latencies = new ArrayList<>();
        try (IndexReader reader = DirectoryReader.open(dir)) {
            Random r = ThreadLocalRandom.current();
            for (int i = 0; i < 1000; ++i) {
                long[] countHolder = new long[1];
                int minValue = r.nextInt(1000);
                int maxValue = 1000 + r.nextInt(100_000_000);

                PointValues.IntersectVisitor intersectVisitor = getIntersecVisitor(minValue, maxValue, countHolder);
                for (LeafReaderContext lrc : reader.leaves()) {
                    PointValues pointValues = lrc.reader().getPointValues("pointField");
                    PointValues.PointTree pointTree = pointValues.getPointTree();
                   // pointValues.intersect(intersectVisitor);
                    long startTime = System.nanoTime();
                    intersect(intersectVisitor, pointTree, countHolder);
                    long endTime = System.nanoTime();
                    latencies.add(endTime - startTime);
                }
            }
        }
        latencies.sort(null);

        long p50nanos = latencies.get(latencies.size() / 2);
        long p90nanos = latencies.get(latencies.size() * 9 / 10);
        long p99nanos = latencies.get(latencies.size() * 99 / 100);

        long p50ms = NANOSECONDS.toMillis(p50nanos);
        long p90ms = NANOSECONDS.toMillis(p90nanos);
        long p99ms = NANOSECONDS.toMillis(p99nanos);

        System.out.println("P50: " +p50ms+ " ms " + ", p50 :" + p50nanos + " nanos " );
        System.out.println("P90: " + p90ms+ " ms , p90 : " + p90nanos + " nanos ");
        System.out.println("P99: " + p99ms+ " ms , p99 : " + p99nanos + " nanos ");
    }


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


               // System.out.println("list size : " + list.size() + " set size : " + matchingLeafBlocksFPsDocIds.size());
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
