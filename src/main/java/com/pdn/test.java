package com.pdn;

public class test {
    public static void main(String[] args) {
//        return timestamp - (timestamp - offset + windowSize) % windowSize;
        System.out.println(1599990791000L-(1599990791000L+5000)%5000);
        System.out.println((1599990791000L+5000)%5000);
    }
}
