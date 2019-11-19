package com.sinaif.stream.operator;

import java.io.File;
import java.io.IOException;

import org.roaringbitmap.RoaringBitmap;

import com.sinaif.stream.common.utils.DateUtil;

public class AppTest {
	
	public static void main(String[] args) {
//		System.out.println(DateUtil.YYYY_MM_DD.parseDateTime("2019-05-13 19:05:38".split(" ")[0]).toDate());
//		
//		roaringBitMap();
		
//		System.out.println(Integer.parseInt("12.5f"));
	
		File f = new File("");
		String cf = null;
		     try {
				cf = f.getCanonicalPath();
			} catch (IOException e) {
				e.printStackTrace();
			}
		
		System.out.println(cf);
		
		System.out.println(System.getProperty("user.dir"));
//		System.out.println(System.getenv());
		System.out.println(System.getProperties());
	}
	
	public static void roaringBitMap() {
        RoaringBitmap rr = RoaringBitmap.bitmapOf(1,2,3,1000);
        RoaringBitmap rr2 = new RoaringBitmap();
        rr2.add(4000L,4255L);
        rr.select(3); // would return the third value or 1000
        rr.rank(2); // would return the rank of 2, which is index 1
        rr.contains(1000); // will return true
        rr.contains(7); // will return false

        RoaringBitmap rror = RoaringBitmap.or(rr, rr2);// new bitmap
        rr.or(rr2); //in-place computation
        boolean equals = rror.equals(rr);// true
        if(!equals) throw new RuntimeException("bug");
        // number of values stored?
        long cardinality = rr.getLongCardinality();
        System.out.println(cardinality);
        // a "forEach" is faster than this loop, but a loop is possible:
        for(int i : rr) {
          System.out.println(i);
        }
  }
}