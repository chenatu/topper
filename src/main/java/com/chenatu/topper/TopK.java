package com.chenatu.topper;

import java.util.List;

/**
 * @author shuo.chen
 */
public class TopK {
    public static void main(String[] args) throws Exception {
        long totalStart = System.currentTimeMillis();
        String filename = "url.dat";
        String destDir = "splits";
        int splitCount = 100;
        int splitThreadCount = 32;
        int countHashThreadCount = 8;
        int k = 10;

        if (args != null && args.length != 0) {
            filename = args[0];
            destDir = args[1];
            splitCount = Integer.parseInt(args[2]);
            splitThreadCount = Integer.parseInt(args[3]);
            countHashThreadCount = Integer.parseInt(args[4]);
            k = Integer.parseInt(args[5]);
        }
        long splitStart = System.currentTimeMillis();
        Topper.splitHashFile(filename, destDir, splitCount, splitThreadCount);
        long splitEnd = System.currentTimeMillis();
        System.out.println("split hash file for " + (splitEnd - splitStart) + " ms");
        long countStart = System.currentTimeMillis();
        Topper.getTopKSplitsFiles(destDir, k, splitCount, countHashThreadCount);
        long countEnd = System.currentTimeMillis();
        System.out.println("count hash file for " + (countEnd - countStart) + " ms");
        long mergeStart= System.currentTimeMillis();
        List<Topper.Count> result = Topper.mergeCountFiles(destDir, k, splitCount);
        long mergeEnd= System.currentTimeMillis();
        System.out.println("merge result for " + (mergeEnd - mergeStart) + " ms");
        for (Topper.Count count : result) {
            System.out.println(count);
        }
        // Topper.cleanSplitHashFile(destDir);
        long totalEnd = System.currentTimeMillis();
        System.out.println("total last for " + (totalEnd - totalStart) + " ms");
        System.exit(0);
    }
}
