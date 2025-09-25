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
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class LuceneBKDTraversalPrefetchBenchmark {

    public static void main(String[] args) throws Exception {

        String tempData  = "temp_data";
        Path dirPath = (args.length==0) ? Paths.get(tempData): Paths.get(args[0]);
        //Paths.get(args[0]);
        Directory dir = FSDirectory.open(dirPath);
        if (DirectoryReader.indexExists(dir) == false) {
            TieredMergePolicy mp = new TieredMergePolicy();
            mp.setSegmentsPerTier(100);
            mp.setMaxMergeAtOnce(100);
            mp.setMaxMergedSegmentMB(1024);
            try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig()
                    .setMergePolicy(mp)
                    .setRAMBufferSizeMB(1024))) {
                ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
                AtomicLong indexed = new AtomicLong(0);
                for (int task = 0; task < 2; ++task) {
                    executor.execute(() -> {
                        Random r = ThreadLocalRandom.current();
                        for (int i = 0; i < 10; ++i) {
                            Document doc = new Document();
                            for (int j = 0; j < 10; ++j) {
                                doc.add(new IntField("pointField", r.nextInt(1000_000), Field.Store.NO));
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
        List<Long> latencies = new ArrayList<>();
        try (IndexReader reader = DirectoryReader.open(dir)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            Random r = ThreadLocalRandom.current();
            for (int i = 0; i < 10_000; ++i) {
                long start = System.nanoTime();
                long[] countHolder = new long[1];
                int minValue = r.nextInt(1000);
                int maxValue = r.nextInt(100_000);

                PointValues.IntersectVisitor intersectVisitor = getIntersecVisitor(minValue, maxValue, countHolder);
                for (LeafReaderContext lrc : reader.leaves()) {
                    long startTime = System.nanoTime();
                    lrc.reader().getPointValues("pointField").intersect(intersectVisitor);
                    long endTime = System.nanoTime();
                    latencies.add(endTime - startTime);
                }
            }
        }
        latencies.sort(null);
        System.out.println("P50: " + latencies.get(latencies.size() / 2));
        System.out.println("P90: " + latencies.get(latencies.size() * 9 / 10));
        System.out.println("P99: " + latencies.get(latencies.size() * 99 / 100));
    }



    private static PointValues.IntersectVisitor getIntersectVisitorWithPrefetching(int minValue, int maxValue, long[] countHolder) {

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
}
