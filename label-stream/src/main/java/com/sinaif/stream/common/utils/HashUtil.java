package com.sinaif.stream.common.utils;

/**
 * @Time : 2019/8/28 14:06
 * @Author : pingping.tu
 * @File : HashUtil.py
 * @Email : flatuer@gmail.com
 * @Description :
 */

public class HashUtil {
    public static int hash(String key ){

        int hashCode = 0;
        for (int i = 0; i < key.length(); i++) {
            hashCode = hashCode * 31 + ((Object)(key.charAt(i))).hashCode();
        }
        return hashCode;
    }

    public static void main(String[] args) {
        String tmp = "3000601";

        System.out.println(HashUtil.hash(tmp));
    }
}
