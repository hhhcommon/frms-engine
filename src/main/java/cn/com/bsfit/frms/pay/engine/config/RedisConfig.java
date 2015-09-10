package cn.com.bsfit.frms.pay.engine.config;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.util.StringUtils;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.ShardedJedisPool;
import cn.com.bsfit.frms.base.config.FrmsConfigurable;
import cn.com.bsfit.frms.dd.pojo.Bkhqy;
import cn.com.bsfit.frms.dd.pojo.Pgycs;
import cn.com.bsfit.frms.pay.engine.store.KryoRedisSerializer;

@Configuration
public class RedisConfig implements FrmsConfigurable {

	@Value("${frms.engine.redis.pool.maxTotal:5}")
	private int maxTotal;

	@Value("${frms.engine.redis.pool.maxIdle:5}")
	private int maxIdle;

	@Value("${frms.engine.redis.pool.minIdle:5}")
	private int minIdle;

	@Value("${frms.engine.redis.pool.maxWaitMillis:3000}")
	private long maxWaitMillis;

	@Value("${redis.engine.redis.pool.testOnBorrow:true}")
	private boolean testOnBorrow;
	/**
	 * 多个地址逗号分割
	 */
	@Value("#{T(java.util.Arrays).asList('${frms.engine.redis.address.list:10.100.1.85}')}")
	private List<String> addressList;

	@Bean
	public ShardedJedisPool shardedJedisPool() {
		List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
		String url = null;
		JedisShardInfo jsi = null;
		for (String host : addressList) {
			url = StringUtils.trimWhitespace(host);
			URI uri = URI.create(url);
			if (uri.getScheme() != null && uri.getScheme().equals("redis")) {
				jsi = new JedisShardInfo(uri.getHost(), uri.getPort());
				if (uri.getUserInfo() != null) {
					jsi.setPassword(uri.getUserInfo().split(":", 2)[1]);
				}
			} else {
				jsi = new JedisShardInfo(url, Protocol.DEFAULT_PORT);
			}
			shards.add(jsi);
		}

		JedisPoolConfig jpc = new JedisPoolConfig();
		jpc.setMaxTotal(maxTotal);
		jpc.setMaxIdle(maxIdle);
		jpc.setMinIdle(minIdle);
		jpc.setMaxWaitMillis(maxWaitMillis);

		return new ShardedJedisPool(jpc, shards);
	}

	@Bean
	public JedisPoolConfig jedisPoolConfig() {
		JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
		jedisPoolConfig.setMaxIdle(maxIdle);
		jedisPoolConfig.setMaxTotal(maxTotal);
		jedisPoolConfig.setMaxWaitMillis(maxWaitMillis);
		jedisPoolConfig.setTestOnBorrow(testOnBorrow);
		return jedisPoolConfig;
	}

	@Bean
	public JedisConnectionFactory jedisConnectionFactory(@Qualifier("jedisPoolConfig") JedisPoolConfig jedisPoolConfig) {
		JedisConnectionFactory jedisConnectionFactory = new JedisConnectionFactory();
		jedisConnectionFactory.setHostName(addressList.get(0));
		jedisConnectionFactory.setPort(6379);
		jedisConnectionFactory.setPoolConfig(jedisPoolConfig);
		return jedisConnectionFactory;
	}

	@SuppressWarnings("rawtypes")
	@Bean
	public RedisTemplate redisTemplate(@Qualifier("jedisConnectionFactory") JedisConnectionFactory jedisConnectionFactory) {
		RedisTemplate redisTemplate = new RedisTemplate();
		redisTemplate.setConnectionFactory(jedisConnectionFactory);
		return redisTemplate;
	}

	@Bean(name = "pgycsRedisTemplate")
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public RedisTemplate pgycsRedisTemplate(@Qualifier("jedisConnectionFactory") JedisConnectionFactory jedisConnectionFactory) {
		RedisTemplate redisTemplate = new RedisTemplate();
		redisTemplate.setConnectionFactory(jedisConnectionFactory);
//		redisTemplate.setKeySerializer(redisTemplate.getStringSerializer());
		redisTemplate.setValueSerializer(new KryoRedisSerializer<Pgycs>(Pgycs.class));
		return redisTemplate;
	}
	

	@Bean(name = "bkhqyRedisTemplate")
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public RedisTemplate bkhqyRedisTemplate(@Qualifier("jedisConnectionFactory") JedisConnectionFactory jedisConnectionFactory) {
		RedisTemplate redisTemplate = new RedisTemplate();
		redisTemplate.setConnectionFactory(jedisConnectionFactory);
//		redisTemplate.setKeySerializer(redisTemplate.getStringSerializer());
		redisTemplate.setValueSerializer(new KryoRedisSerializer<Bkhqy>(Bkhqy.class));
		return redisTemplate;
	}	
}
