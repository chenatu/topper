package com.chenatu.topper;

import com.google.common.collect.ImmutableMap;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author shuo.chen
 */
public class Topper {

    public static void generateURLFile(String filename, long size) {
        long currentSize = 0;
        Path path = Paths.get(filename);
        System.out.println("write file to path: " + path);
        long start = System.currentTimeMillis();
        try (BufferedWriter out = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            while (currentSize < size) {
                String line = generateRandomURL();
                out.write(line);
                out.newLine();
                currentSize = currentSize + line.length() + 1;

            }
        } catch (IOException e) {
            System.out.println("write file error: ");
            e.printStackTrace();
        }
        long end = System.currentTimeMillis();
        System.out.println("write file for " + (end - start) + " ms");
        System.out.println("write file size " + currentSize);
    }

    public static class Count implements Comparable {
        public String url;
        public Integer count = 0;

        public Count(String url, Integer count) {
            this.url = url;
            this.count = count;
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof Count) {
                return this.count.equals(((Count) other).count) && StringUtils.equals(this.url, ((Count) other).url);
            } else {
                return false;
            }
        }

        @Override
        public int compareTo(Object o) {
            Count other = (Count) o;
            if (!this.count.equals(other.count)) {
                return Integer.compare(this.count, other.count);
            } else {
                return this.url.compareTo(other.url);
            }
        }

        @Override
        public String toString() {
            return url + "," + count;
        }
    }

    public static interface CompareFunction<E> {
        int compare(E e1, E e2);
    }

    public static <E> int binarySearchIndex(E search, List<E> array, CompareFunction<E> compareFunction) {
        int start = 0;
        int end = array.size() - 1;

        while (start <= end) {

            int middle = (start + end) / 2;

            if (compareFunction.compare(search, array.get(middle)) < 0) {
                end = middle - 1;
            }

            if (compareFunction.compare(search, array.get(middle)) > 0) {
                start = middle + 1;
            }

            if (compareFunction.compare(search, array.get(middle)) == 0) {
                return middle;
            }
        }
        return -1;
    }

    public static <E> void reorgSortedList(int target, List<E> array, CompareFunction<E> compareFunction) {
        int start = 0;
        int end = array.size() - 1;
        while (target >= start && target <=end) {
            if ((target - 1) >= start) {
                if (compareFunction.compare(array.get(target - 1), array.get(target)) > 0) {
                    swap(array, target - 1, target);
                    target = target - 1;
                    continue;
                }
            }

            if ((target + 1) <= end) {
                if (compareFunction.compare(array.get(target), array.get(target + 1)) > 0) {
                    swap(array, target, target + 1);
                    target = target + 1;
                    continue;
                }
            }
            break;
        }
    }

    public static <E> void quickSort(List<E> array, CompareFunction<E> compareFunction) {
        Queue<Integer> startEnd = new LinkedList<>();
        startEnd.offer(0);
        startEnd.offer(array.size() - 1);
        while (startEnd.peek() != null) {
            Integer start = startEnd.poll();
            Integer end = startEnd.poll();
            int pivot = quickSortPartition(array, start, end, compareFunction);
            if (start < pivot) {
                startEnd.offer(start);
                startEnd.offer(pivot);
            }
            if (pivot + 1 < end) {
                startEnd.offer(pivot + 1);
                startEnd.offer(end);
            }
        }
    }

    private static <E> int quickSortPartition(List<E> array, int start, int end, CompareFunction<E> compareFunction) {
        if (start >= end) {
            return start;
        }

        E pivot = array.get(start);
        int p = start, q = end;
        while (p < q) {
            while (compareFunction.compare(array.get(q), pivot) >=0 && q > p) {
                q--;
            }
            swap(array, p, q);
            while (compareFunction.compare(array.get(p), pivot) < 0 && p < q) {
                p++;
            }
            swap(array, p, q);
        }
        return p;
    }

    private static <E> void swap(List<E> array, int p, int q) {
        E temp = array.get(q);
        array.set(q, array.get(p));
        array.set(p, temp);
    }

    public static void getTopKFromSplitFileIntoCountFile(String filename, int k) throws IOException {
        System.out.println("start to process " + filename);
        Map<String, Integer> counts = new HashMap<>();
        try (BufferedReader in = Files.newBufferedReader(Paths.get(filename))) {
            String line = in.readLine();
            while (line != null) {
                if (!line.isEmpty()) {
                    if (counts.containsKey(line)) {
                        counts.put(line, counts.get(line) + 1);
                    } else {
                        counts.put(line, 1);
                    }
                }
                line = in.readLine();
            }
        }

        System.out.println("read finished: " + filename);

        TreeSet<Count> treeSet = new TreeSet<>();
        counts.forEach((key, value) -> treeSet.add(new Count(key, value)));

        System.out.println("sort finished: " + filename);
        Iterator<Count> it = treeSet.descendingIterator();
        try (BufferedWriter out = Files.newBufferedWriter(Paths.get(filename + ".count"))) {
            for (int i = 0; i < k && it.hasNext(); i++) {
                out.write(it.next().toString());
                out.newLine();
            }
        }
        System.out.println("write finished: " + filename + ".count");
//        CompareFunction<Count> urlComparator = (e1, e2) -> e1.url.compareTo(e2.url);
//        CompareFunction<Count> countComparator = Count::compareTo;
//
//        List<Count> counts = new ArrayList<>();
//        try (BufferedReader in = Files.newBufferedReader(Paths.get(filename))) {
//            String line = in.readLine();
//            while (line != null) {
//                if (!line.isEmpty()) {
//                    Count search = new Count(line, 1);
//                    int targetIndex = binarySearchIndex(search, counts, urlComparator);
//                    if (targetIndex == -1) {
//                        counts.add(search);
//                        reorgSortedList(counts.size() - 1, counts, urlComparator);
//                    } else {
//                        Count target = counts.get(targetIndex);
//                        target.count = target.count + 1;
//                        reorgSortedList(targetIndex, counts, urlComparator);
//                        search = null;
//                    }
//                }
//                line = in.readLine();
//
//
//            }
//        }
//
//        quickSort(counts, countComparator);
//
//
//        try (BufferedWriter out = Files.newBufferedWriter(Paths.get(filename + ".count"))) {
//            for (int i = 0; i < k && i < counts.size(); i++) {
//                out.write(counts.get(counts.size() - i - 1).toString());
//                out.newLine();
//            }
//        }
    }

    public static void splitHashFile(String sourceFileName, String destDir, int mod, int splitThreadCount) throws Exception {
        List<Long> positions = getSeparatePositions(sourceFileName, mod);

        Path destPath = Paths.get(destDir);
        if (Files.exists(destPath)) {
            FileUtils.deleteDirectory(destPath.toFile());
        }
        Files.createDirectory(destPath);
        System.out.println("create split dir: " + destPath.toUri());

        ImmutableMap<Integer, BufferedWriter> writers = initHashWriters(destDir, mod);

        ExecutorService pool = Executors.newFixedThreadPool(splitThreadCount);
        CountDownLatch latch = new CountDownLatch(positions.size() - 1);
        AtomicInteger lineCount = new AtomicInteger(0);
        System.out.println("start to split file into " + (positions.size() - 1) + " hash parts");
        for (int i = 0; i < positions.size() - 1; i++) {
            int finalI = i;
            pool.submit(() -> {
                try {
                    long start = System.currentTimeMillis();
                    hashFileIntoSplits(sourceFileName, positions.get(finalI) + 1, positions.get(finalI + 1), mod, writers, lineCount);
                    long end = System.currentTimeMillis();
                    System.out.println("split " + finalI + " hash file run for " + (end - start) + " ms");
                } catch (Exception e) {
                    System.out.println("split file error " + finalI);
                    e.printStackTrace();
                    throw new RuntimeException(e);
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
        pool.shutdown();
        System.out.println("split line count: " + lineCount.get());
        closeHashWriters(writers);
    }

    public static void getTopKSplitsFiles(String destDir, int k, int splitCount, int splitThreadCount) throws InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(splitThreadCount);
        CountDownLatch latch = new CountDownLatch(splitCount);
        for (int i = 0; i < splitCount; i++) {
            int finalI = i;
            pool.submit(() -> {
                try {
                    System.out.println("start to count split file " + finalI);
                    long start = System.currentTimeMillis();
                    getTopKFromSplitFileIntoCountFile(destDir + "/" + finalI, k);
                    long end = System.currentTimeMillis();
                    System.out.println("count split file " + finalI + " for: " + (end - start) + " ms");
                } catch (Exception e) {
                    System.out.println("get top k of split file fail " + finalI);
                    e.printStackTrace();
                    throw new RuntimeException(e);
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
        pool.shutdown();
    }

    public static class CountReader implements Comparable {
        final BufferedReader reader;

        Count currentCount = null;

        private boolean end = false;

        public CountReader(BufferedReader reader) {
            this.reader = reader;
        }

        public void next() throws IOException {
            if (!end) {
                String line = reader.readLine();
                if (line != null) {
                    if (StringUtils.isNoneEmpty(line)) {
                        int split = line.indexOf(',');
                        currentCount = new Count(line.substring(0, split), Integer.parseInt(line.substring(split + 1)));
                    }
                } else {
                    currentCount = null;
                    end = true;
                }
            }
        }

        // Priority Queue is min heap
        @Override
        public int compareTo(Object o) {
            if (o == null) {
                throw new NullPointerException();
            }
            // nobody is null now
            CountReader other = (CountReader) o;
            if (currentCount == null && other.currentCount == null) {
                return 0;
            }
            if (other.currentCount == null) {
                return -1;
            }
            if (currentCount == null) {
                return 1;
            }
            return other.currentCount.compareTo(this.currentCount);
        }
    }

    public static List<Count> mergeCountFiles(String destDir, int k, int splitCount) throws IOException {
        List<Count> result = new ArrayList<>();
        // prepare multiple readers
        PriorityQueue<CountReader> heap = new PriorityQueue<>();
        for (int i = 0; i < splitCount; i++) {
            CountReader reader = new CountReader(Files.newBufferedReader(Paths.get(destDir + "/" + i + ".count")));
            reader.next();
            heap.offer(reader);
        }

        for (int i = 0; i < k && !heap.isEmpty(); i++) {
            CountReader reader = heap.poll();
            if (reader.currentCount != null) {
                result.add(reader.currentCount);
            }
            reader.next();
            heap.offer(reader);
        }

        // close readers
        while (!heap.isEmpty()) {
            heap.poll().reader.close();
        }
        return result;
    }

    public static void cleanSplitHashFile(String destDir) throws IOException {
        FileUtils.deleteDirectory(Paths.get(destDir).toFile());
    }

    private static ImmutableMap<Integer, BufferedWriter> initHashWriters(String destDir, int mod) throws IOException {
        ImmutableMap.Builder<Integer, BufferedWriter> builder = ImmutableMap.builder();
        int writerCount = 0;
        for (int i = 0; i < mod; i++) {
            BufferedWriter writer = Files.newBufferedWriter(Paths.get(destDir.concat("/").concat(String.valueOf(i))));
            if (writer != null) {
                if (i == 648) {
                    System.out.println("put writer 648");
                }
                builder.put(i, writer);
                writerCount++;
            }
        }
        System.out.println("init " + writerCount + " writers");
        return builder.build();
    }

    private static void closeHashWriters(ImmutableMap<Integer, BufferedWriter> writers) throws IOException {
        for (Map.Entry<Integer, BufferedWriter> entry : writers.entrySet()) {
            entry.getValue().close();
        }
    }

    private static void hashFileIntoSplits(String filename, long startPosInclusive, long endPosExclusive, int mod, ImmutableMap<Integer, BufferedWriter> writers, AtomicInteger lineCount) throws Exception {
        long position = startPosInclusive;
        String lineSeparatorS = System.lineSeparator();
        if (lineSeparatorS.length() > 1) {
            throw new Exception("line separator more than 1 char is not supported");
        }
        char lineSeparator = lineSeparatorS.charAt(0);
        byte[] bs = new byte[32];
        StringBuffer strBuf = new StringBuffer();
        try (FileChannel fc = new RandomAccessFile(filename, "r").getChannel()) {
            ByteBuffer rBuffer = ByteBuffer.allocateDirect(32);
            fc.position(startPosInclusive);
            while (position < endPosExclusive && fc.read(rBuffer) != -1) {
                int rSize = rBuffer.position();
                rBuffer.rewind();
                rBuffer.get(bs);
                rBuffer.clear();
                String tempString = new String(bs, 0, rSize);
                int fromIndex = 0;
                int endIndex = 0;
                while ((endIndex = tempString.indexOf(lineSeparator, fromIndex)) != -1 && position < endPosExclusive) {
                    String line = tempString.substring(fromIndex, endIndex);
                    line = new String(strBuf.toString() + line);
                    // got one line
                    if (!line.isEmpty()) {
                        int hash = simpleHash(line) % mod;
                        if (hash < 0) {
                            hash = - hash;
                        }
                        BufferedWriter writer = writers.get(hash);
                        if (writer != null) {
                            writer.write(line);
                            writer.newLine();
                        } else {
                            System.out.println("can not find writer " + hash);
                            writer = writers.get(hash);
                            if (writer == null) {
                                System.out.println("can not find writer again " + hash);
                            }
                        }
                        position = position + line.length() + 1;
                        lineCount.incrementAndGet();
                    }
                    strBuf.delete(0, strBuf.length());
                    fromIndex = endIndex + 1;
                }

                if (rSize > tempString.length()) {
                    strBuf.append(tempString.substring(fromIndex));
                } else {
                    strBuf.append(tempString, fromIndex, rSize);
                }

            }
        }
    }

    private static HashFunction hf = Hashing.crc32();

    private static int simpleHash(String s) {
        return Math.abs(hf.hashUnencodedChars(s).asInt());
    }


    private static List<Long> getSeparatePositions(String filename, int splitCount) throws Exception {
        FileChannel fc = new RandomAccessFile(filename, "r").getChannel();
        List<Long> positions = new ArrayList<>();
        positions.add(-1L);
        long fileSize = getFileSize(filename);
        long estimatedSplitSize = fileSize / splitCount;
        String lineSeparatorS = System.lineSeparator();
        if (lineSeparatorS.length() > 1) {
            throw new Exception("line separator more than 1 char is not supported");
        }
        char lineSeparator = lineSeparatorS.charAt(0);
        long position = 0;
        while (position < fileSize) {
            position += estimatedSplitSize;
            ByteBuffer bf = ByteBuffer.allocate(128);
            fc.position(position);
            boolean findLine = false;
            while (position < fileSize) {
                fc.read(bf);
                bf.flip();
                while (bf.hasRemaining()) {
                    char currentChar = (char) bf.get();
                    position++;
                    if (currentChar == lineSeparator) {
                        // find line separator
                        positions.add(position);
                        findLine = true;
                        break;
                    }
                }
                bf.clear();
                if (findLine) {
                    break;
                }
            }
        }
        positions.add(fileSize);
        return positions;
    }


    private static long getFileSize(String filename) throws IOException {
        return Files.size(Paths.get(filename));
    }

    private static String generateRandomURL() {
        StringBuffer sb = new StringBuffer();
        int n = RandomUtils.nextInt(1, 4);
        sb.append("http://");
        for (int i = 0; i < n; i++) {
            sb.append(RandomStringUtils.randomAlphabetic(1, 8)).append('/');
        }
        return sb.toString();
    }

}
