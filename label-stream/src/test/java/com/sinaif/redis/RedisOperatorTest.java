package com.sinaif.redis;

import com.sinaif.stream.common.utils.AESToLong;
import com.sinaif.stream.redis.RedisPool;

import java.util.HashMap;
import java.util.Map;

/**
 * @File : RedisOperatorTest.java
 * @Author: tupingping
 * @Date : 2019/8/15
 * @Desc :
 */
public class RedisOperatorTest {
    public static void main(String[] args) {
        try {
            String[][] tmp = {
                    {"154819224365028093","18666662520","2005"},
                    {"201705110300000001","18600000001","1002"},
                    {"201705110300000003","18873677183","1001"},
                    {"149475765939545931","13717127772","1003"},
                    {"153364170397066970","13554785302","2001"}
            };
            Map<String, String> values = new HashMap<>();

            for(int i = 0; i < 5; i ++){
                Long customerId = AESToLong.convertToNumber(tmp[i][1]);
                String redisField = tmp[i][0] + "@" + tmp[i][2];
                String redisValue = tmp[i][1] + "@" + customerId;

                values.put(redisField, redisValue);
            }

            RedisPool.hset("sinaif_label_cache", values);

        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
