package com.chenatu.topper;

import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @author shuo.chen
 */
@Ignore
public class TopperTest {
    @Test
    public void generateURLFile() {
        Topper.generateURLFile("url.dat", 1000000);
    }

    @Test
    public void splitHashFile() throws Exception {
        Topper.generateURLFile("url.dat", 1000000);
        Topper.splitHashFile("url.dat", "splits", 100, 10);
    }

    @Test
    public void getTopKSplitsFiles() throws Exception {
        Topper.generateURLFile("url.dat", 1000000);
        Topper.splitHashFile("url.dat", "splits", 100, 10);
        Topper.getTopKSplitsFiles("splits", 10, 100, 10);
    }

    @Test
    public void getTopKFromSplitFileIntoCountFile() throws IOException {
        Topper.getTopKFromSplitFileIntoCountFile("splits/6", 10);
    }

    @Test
    public void mergeCountFiles() throws Exception {
        Topper.generateURLFile("url.dat", 10000000);
        Topper.splitHashFile("url.dat", "splits", 1000, 10);
        Topper.getTopKSplitsFiles("splits", 10, 1000, 10);
        List<Topper.Count> result = Topper.mergeCountFiles("splits", 10, 1000);
        for (Topper.Count count : result) {
            System.out.println(count);
        }
    }
}
