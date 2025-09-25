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

import java.io.IOException;
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

    public static void main(String[] args) throws Exception {

        String tempData  = "temp_data";
        Path dirPath = (args.length==0) ? Paths.get(tempData): Paths.get(args[0]);
        boolean testWithPrefetch = false;
        boolean ingest = false;
        if (args[1].equalsIgnoreCase("prefetch")) {
            testWithPrefetch=true;
        }
        else if (args[1].equalsIgnoreCase("ingest")) {
            ingest=true;
        }

        Directory dir = FSDirectory.open(dirPath);
        if (ingest) {
            if (DirectoryReader.indexExists(dir) == false) {
            IndexWriterConfig indexWriterConfig = new IndexWriterConfig().setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND)
                    .setRAMBufferSizeMB(10240);
                try (IndexWriter w = new IndexWriter(dir, indexWriterConfig)) {
                    ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
                    AtomicLong indexed = new AtomicLong(0);
                    for (int task = 0; task < 1000; ++task) {
                        executor.execute(() -> {
                            Random r = ThreadLocalRandom.current();
                            for (int i = 0; i < 1000; ++i) {
                                Document doc = new Document();
                                for (int j = 0; j < 10_000; ++j) {
                                    doc.add(new IntField("pointField", r.nextInt(100_000_000), Field.Store.NO));
                                }
                                try {
                                    w.addDocument(doc);
                                } catch (IOException e) {
                                    throw new UncheckedIOException(e);
                                }
                                final long actualIndexed = indexed.incrementAndGet();
                                //810_000
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

        }
        if (!testWithPrefetch) {
            searchWithoutPrefetching(dir);
        } else {
            searchWithPrefetching(dir);
        }

    }

    private static void searchWithPrefetching(Directory dir) throws IOException {
        List<Long> latencies = new ArrayList<>();
        try (IndexReader reader = DirectoryReader.open(dir)) {
            Random r = ThreadLocalRandom.current();

            for (int i = 0; i < 10_000; ++i) {
                //long start = System.nanoTime();
                long[] countHolder = new long[1];
                int minValue = r.nextInt(1000);
                int maxValue = 1000 + r.nextInt(100_000_000);
                PointValues.IntersectVisitor intersectVisitor = getIntersectVisitorWithPrefetching(minValue, maxValue, countHolder);
                for (LeafReaderContext lrc : reader.leaves()) {
                    long startTime = System.nanoTime();
                    PointValues pointValues = lrc.reader().getPointValues("pointField");
                    PointValues.PointTree pointTree = pointValues.getPointTree();
                    intersectWithPrefetch(intersectVisitor, pointTree, countHolder);
                    pointTree.visitMatchingDocIDs(intersectVisitor);
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

    private static void searchWithoutPrefetching(Directory dir) throws IOException {
        List<Long> latencies = new ArrayList<>();
        try (IndexReader reader = DirectoryReader.open(dir)) {
            Random r = ThreadLocalRandom.current();
            for (int i = 0; i < 10_000; ++i) {
                long start = System.nanoTime();
                long[] countHolder = new long[1];
                int minValue = r.nextInt(1000);
                int maxValue = 1000 + r.nextInt(100_000_000);

                PointValues.IntersectVisitor intersectVisitor = getIntersecVisitor(minValue, maxValue, countHolder);
                for (LeafReaderContext lrc : reader.leaves()) {
                    long startTime = System.nanoTime();
                    PointValues pointValues = lrc.reader().getPointValues("pointField");
                    PointValues.PointTree pointTree = pointValues.getPointTree();
                   // pointValues.intersect(intersectVisitor);
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
