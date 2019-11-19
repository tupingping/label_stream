package com.sinaif.stream.prepare.redis.cache;

import com.sinaif.stream.common.utils.AESToLong;
import com.sinaif.stream.common.utils.ConfigRespostory;
import com.sinaif.stream.mysql.MysqlPool;
import com.sinaif.stream.redis.RedisPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;


public class RedisCachePrepare {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisCachePrepare.class);

    public static void main(String[] args) {

        try {
            String sql = "select id, username, '1003' as productid from sinaif_easy.t_user_account";
                    //"select id, username, productid from sinaif.t_user_account";
//                    "union select id, username, '1003' as productid from sinaif_easy.t_user_account";
            String key = ConfigRespostory.value("redis.cache.hash.key");
            Long count = 0L;
            Map<String, String> values = new HashMap<>();
            Long sum = 0L;
            LOGGER.info("start query.");
            ResultSet rs = MysqlPool.simpleQuery(sql);

            LOGGER.info("start import.");

            while (rs.next()){
                // sinaif_label_pk_cache:（id@productid: username@customer_pk）[hash]
                // 主键生成

                String id = rs.getString("id");
                String userName = rs.getString("username");
                String productid = rs.getString("productid");

                if(id == null || userName == null || productid == null
                        || id.length() == 0 || userName.length() == 0 || productid.length() == 0 )
                    continue;

                try{

                    String customer_pk = String.valueOf(AESToLong.convertToNumber(userName));
                    if(productid.equals("1001") || productid.equals("1002")){
                        values.put(id + "@" + "1001", userName + "@" + customer_pk);
                        values.put(id + "@" + "1002", userName + "@" + customer_pk);
                    }else{
                        values.put(id + "@" + productid, userName + "@" + customer_pk);
                    }

                    count ++;
                    sum ++;
                    if(count > 10000){
                        RedisPool.hmset(key, values);

                        values.clear();
                        LOGGER.info("sum: {}", sum );
                        count = 0L;
                    }

                }catch (Exception e){
                    LOGGER.error("generate primary key error. message: " + e);
                }
            }

            if(!values.isEmpty()){
                RedisPool.hmset(key, values);
                LOGGER.info("sum: {}", sum );
            }

            LOGGER.info("finish.");

        }catch (SQLException e){
            LOGGER.error("SQLException: " + e);
        }
    }

}
