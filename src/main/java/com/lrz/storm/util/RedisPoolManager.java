
package com.lrz.storm.util;

import java.util.ResourceBundle;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * 
 * @Description:
 * @author:DARUI LI
 * @version:1.0.0
 * @Data:2018年11月22日 下午8:51:58
 */
public class RedisPoolManager {

	private static JedisPool pool;

	static {
		ResourceBundle bundle = ResourceBundle.getBundle("redis");
		if (bundle == null)
			throw new IllegalArgumentException("[redis.properties] is not found");

		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxIdle(Integer.valueOf(bundle.getString("redis.pool.maxIdle")));
		config.setTestOnBorrow(Boolean.valueOf(bundle.getString("redis.pool.testOnBorrow")));
		config.setTestOnReturn(Boolean.valueOf(bundle.getString("redis.pool.testOnReturn")));

		pool = new JedisPool(config, bundle.getString("redis.ip"), Integer.valueOf(bundle.getString("redis.port")));
	}

	/**
	 * 获取Jedis实例
	 * 
	 * @return
	 */
	public static Jedis createInstance() {
		try {
			if (pool != null) {
				Jedis resource = pool.getResource();
				return resource;
			} else {
				return null;
			}
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * 释放jedis资源
	 * 
	 * @param jedis
	 */
	@SuppressWarnings("deprecation")
	public static void returnResource(Jedis jedis) {
		if (jedis != null) {
			pool.returnResource(jedis);
			pool.close();
		}
	}
}
