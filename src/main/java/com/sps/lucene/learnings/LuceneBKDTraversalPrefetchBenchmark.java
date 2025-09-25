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
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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
            //if (DirectoryReader.indexExists(dir) == false) {
                TieredMergePolicy mp = new TieredMergePolicy();
                mp.setSegmentsPerTier(100);
                mp.setMaxMergeAtOnce(100);
                mp.setMaxMergedSegmentMB(1024);
                try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig()
                        .setMergePolicy(mp)
                        .setRAMBufferSizeMB(1024)
                        .setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND))

                ) {
                    ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
                    AtomicLong indexed = new AtomicLong(0);
                    for (int task = 0; task < 1000; ++task) {
                        executor.execute(() -> {
                            Random r = ThreadLocalRandom.current();
                            for (int i = 0; i < 10000; ++i) {
                                Document doc = new Document();
                                for (int j = 0; j < 10000; ++j) {
                                    doc.add(new IntField("pointField", r.nextInt(100_000_000), Field.Store.NO));
                                }
                                try {
                                    w.addDocument(doc);
                                } catch (IOException e) {
                                    throw new UncheckedIOException(e);
                                }
                                final long actualIndexed = indexed.incrementAndGet();
                                //810000
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
           // }

        }
        if (testWithPrefetch) {
            searchWithPrefetching(dir);
        } else {
            searchWithoutPrefetching(dir);
        }

    }

    private static void searchWithPrefetching(Directory dir) throws IOException {
        List<Long> latencies = new ArrayList<>();
        try (IndexReader reader = DirectoryReader.open(dir)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            Random r = ThreadLocalRandom.current();
            for (int i = 0; i < 10_000; ++i) {
                //long start = System.nanoTime();
                long[] countHolder = new long[1];
                int minValue = r.nextInt(1000);
                int maxValue = r.nextInt(100_000_000);

                PointValues.IntersectVisitor intersectVisitor = getIntersectVisitorWithPrefetching(minValue, maxValue, countHolder);

                for (LeafReaderContext lrc : reader.leaves()) {
                    long startTime = System.nanoTime();
                    PointValues pointValues = lrc.reader().getPointValues("pointField");
                    PointValues.PointTree pointTree = pointValues.getPointTree();
                    pointValues.intersect(intersectVisitor);
                    pointTree.visitMatchingDocIDs(intersectVisitor);
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

    private static void searchWithoutPrefetching(Directory dir) throws IOException {
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


    //return new PointValues.IntersectVisitor() {
    //
    //            DocIdSetBuilder.BulkAdder adder;
    //            Set<Long> matchingLeafBlocksFPsDocIds = new LinkedHashSet<>();
    //            Set<Long> matchingLeafBlocksFPsDocValues = new LinkedHashSet<>();
    //            TreeMap<Integer, Long> leafOrdinalFPDocIds = new TreeMap<>();
    //            TreeMap<Integer, Long> leafOrdinalFPDocValues = new TreeMap<>();
    //            int lastMatchingLeafOrdinal = -1;
    //
    //            boolean firstMatchFound = false;
    //            long firstMatchedFp = -1;
    //
    //            @Override
    //            public void grow(int count) {
    //                adder = result.grow(count);
    //            }
    //
    //            @Override
    //            public void visit(int docID) {
    //                // it is possible that size < 1024 and docCount < size but we will continue to count through all the 1024 docs
    //                adder.add(docID);
    //                docCount[0]++;
    //            }
    //
    //            @Override
    //            public void visit(DocIdSetIterator iterator) throws IOException {
    //                adder.add(iterator);
    //            }
    //
    //            @Override
    //            public void visit(IntsRef ref) {
    //                adder.add(ref);
    //                docCount[0] += ref.length;
    //            }
    //
    //            @Override
    //            public void visit(int docID, byte[] packedValue) {
    //                if (matches(packedValue)) {
    //                    visit(docID);
    //                }
    //            }
    //
    //            @Override
    //            public void visit(DocIdSetIterator iterator, byte[] packedValue) throws IOException {
    //                if (matches(packedValue)) {
    //                    adder.add(iterator);
    //                }
    //            }
    //
    //            @Override
    //            public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
    //                return relate(minPackedValue, maxPackedValue);
    //            }
    //
    //            @Override
    //            public void matchedLeafFpDocIds(long fp, int count) {
    //                matchingLeafBlocksFPsDocIds.add(fp);
    //                docCount[0] += count;
    //            };
    //
    //            @Override
    //            public  Set<Long> matchingLeafNodesfpDocIds() {
    //                return matchingLeafBlocksFPsDocIds;
    //            }
    //
    //            @Override
    //            public void matchedLeafFpDocValues(long fp) {
    //                matchingLeafBlocksFPsDocValues.add(fp);
    //            };
    //
    //            @Override
    //            public  Set<Long> matchingLeafNodesfpDocValues() {
    //                return matchingLeafBlocksFPsDocValues;
    //            }
    //
    //            @Override
    //            public void matchedLeafOrdinalDocIds(int leafOrdinal, long fp, int count) {
    //                leafOrdinalFPDocIds.put(leafOrdinal, fp);
    //            };
    //
    //            @Override
    //            public void matchedLeafOrdinalDocValues(int leafOrdinal, long fp) {
    //                leafOrdinalFPDocValues.put(leafOrdinal, fp);
    //            };
    //
    //            @Override
    //            public Map<Integer,Long> matchingLeafNodesDocValues() {
    //                return leafOrdinalFPDocValues;
    //            }
    //
    //            @Override
    //            public Map<Integer,Long> matchingLeafNodesDocIds() {
    //                return leafOrdinalFPDocIds;
    //            }
    //
    //            @Override
    //            public int lastMatchingLeafOrdinal() {
    //                return lastMatchingLeafOrdinal;
    //            }
    //
    //            @Override
    //            public  void setLastMatchingLeafOrdinal(int leafOrdinal) {
    //                lastMatchingLeafOrdinal = leafOrdinal;
    //            }
    //
    //            @Override
    //            public void visitAfterPrefetch(int docID) throws IOException {
    //                //in.visitAfterPrefetch(docID);
    //                adder.add(docID);
    //            }
    //
    //            @Override
    //            public void visitAfterPrefetch(int docID, byte[] packedValue) throws IOException {
    //                //in.visitAfterPrefetch(docID, packedValue);
    //                if (matches(packedValue)) {
    //                    //visit(docID);
    //                    adder.add(docID);
    //                }
    //            };
    //
    //
    //        };


    private static PointValues.IntersectVisitor getIntersectVisitorWithPrefetching(int minValue, int maxValue,
                                                                                   long[] countHolder) {

        return new PointValues.IntersectVisitor() {
            // This version of `visit` gets called when we know that every doc in the current leaf node matches.
            int lastMatchingLeafOrdinal = -1;
            Set<Long> matchingLeafBlocksFPsDocIds = new HashSet<>();

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
            public  Set<Long> matchingLeafNodesfpDocIds() {
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
}
