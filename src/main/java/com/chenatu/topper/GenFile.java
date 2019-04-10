package com.chenatu.topper;

/**
 * @author shuo.chen
 */
public class GenFile {
    public static void main(String[] args) {
        long size = 100000000;
        String filename = "url.dat";
        if (args != null && args.length != 0) {
            size = Long.parseLong(args[0]);
            filename = args[1];
        }
        System.out.println("generate url file filename " + filename + " size " + size + " bytes");
        Topper.generateURLFile(filename, size);
    }
}
