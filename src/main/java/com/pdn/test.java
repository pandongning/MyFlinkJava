package com.pdn;

import com.pdn.flinktraing.beans.TaxiFare;

import java.util.HashMap;

public class test {
    public static void main(String[] args) {
        HashMap<String, TaxiFare> hashMap = new HashMap<>();
        hashMap.put("a",new TaxiFare("a",1,1L));
        hashMap.put("b",new TaxiFare("b",2,2L));
        hashMap.put("c",new TaxiFare("c",3,3L));

        TaxiFare a = hashMap.get("a");
        if (a != null){
            a.setFares(1.1f);
        }

        System.out.println(hashMap.get("a").getFares());

    }
}
