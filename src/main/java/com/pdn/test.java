package com.pdn;

import com.pdn.flinktraing.beans.TaxiFare;

import java.util.HashMap;

public class test {
    public static void main(String[] args) {
        HashMap<String, String> stringStringHashMap = new HashMap<>();
        stringStringHashMap.put(null,"b");
        stringStringHashMap.put(null,"d");
        System.out.println(stringStringHashMap.get(null));
    }
}
