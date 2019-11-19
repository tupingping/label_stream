package com.sinaif.stream.redis;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;

import com.sinaif.stream.common.utils.ConfigRespostory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
/**
 * redis tool for get-set object
 * 
 * @author simonzhang
 *
 */
public class RedisPool {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisPool.class);

	private static JedisPool REDIS_POOL;
	static {
		init();
	}
	private static void init() {
        JedisPoolConfig jpc = new JedisPoolConfig();
        String max = ConfigRespostory.value("redis.connections.max");
        jpc.setMaxIdle(Integer.parseInt(max));
        jpc.setMaxTotal(Integer.parseInt(max));
        jpc.setMaxWaitMillis(10000);
        String port = ConfigRespostory.value("redis.port");
        REDIS_POOL =  new JedisPool(jpc, ConfigRespostory.value("redis.host"), Integer.parseInt(port), 10000,
                ConfigRespostory.value("redis.user.password"), false);

	}
	
	@SuppressWarnings("unchecked")
	public static <T> T findByKey(String key, Class<T> clazz) {
		byte[] bytes = REDIS_POOL.getResource().get(key.getBytes());
		T t = null;
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        ObjectInputStream sIn;
        try {
            sIn = new ObjectInputStream(in);
            t = (T)sIn.readObject();
            sIn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return t;
		
	}
	
	public static void saveObject(String key, Object payload) {
		byte[] bytes = null;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream sOut;
        try {
            sOut = new ObjectOutputStream(out);
            sOut.writeObject(payload);
            sOut.flush();
            bytes= out.toByteArray();
            sOut.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        REDIS_POOL.getResource().set(key.getBytes(), bytes);
	}


	public static String hmset(String key, Map<String, String> hash){
        Jedis jedis = null;
        try {
            jedis = REDIS_POOL.getResource();
            return jedis.hmset(key, hash);
        } catch (Exception e) {
            LOGGER.error("hmset error key=" + key, e);
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
        return null;
    }

    public static String hget(String key, String field){
        Jedis jedis = null;
        try {
            jedis = REDIS_POOL.getResource();
            return jedis.hget(key, field);
        } catch (Exception e) {
            LOGGER.error("hget error key=" + key, e);
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
        return null;
    }

    public static Long hset(String key, Map<String, String> field){
        Jedis jedis = null;
        try {
            jedis = REDIS_POOL.getResource();
            return jedis.hset(key, field);
        } catch (Exception e) {
            LOGGER.error("hset error key=" + key, e);
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
        return null;
    }
}
