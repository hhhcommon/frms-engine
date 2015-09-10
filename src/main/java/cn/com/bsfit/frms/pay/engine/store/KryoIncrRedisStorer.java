package cn.com.bsfit.frms.pay.engine.store;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;

import cn.com.bsfit.frms.base.store.CachedItemStorer;
import cn.com.bsfit.frms.obj.MemCachedItem;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class KryoIncrRedisStorer implements CachedItemStorer {
    private static final Logger logger = LoggerFactory.getLogger(KryoIncrRedisStorer.class);

    @Value("${frms.processor.redis.expires:432000}")
    private long expireSeconds;//redis中动态数据的过期时间，默认5天

    @Autowired
    protected RedisTemplate redisTemplate;

    private RedisSerializer<MemCachedItem> redisSerializer = new KryoRedisSerializer<MemCachedItem>(MemCachedItem.class);

    /**
     * 鎵归噺鏂板<br>
     * 
     * @param items
     * @return
     * @throws java.io.IOException
     */
    public void storeMemCachedItem(final Collection<MemCachedItem> items) throws IOException {
        redisTemplate.setValueSerializer(redisSerializer);
        final Map<String, MemCachedItem> itemMap = new HashMap<String, MemCachedItem>();
        for (MemCachedItem item : items) {
            itemMap.put(item.getMemCachedKey(), item);
        }
        long start = System.currentTimeMillis();
        try {
            List<MemCachedItem> oldItems = redisTemplate.executePipelined(new RedisCallback<MemCachedItem>() {
                public MemCachedItem doInRedis(RedisConnection connection) throws DataAccessException {
                    for (String key : itemMap.keySet()) {
                        connection.get(redisTemplate.getKeySerializer().serialize(key));
                    }
                    return null;
                }
            });
            long point1 = System.currentTimeMillis();
            int existCount = 0;
            for (MemCachedItem item : oldItems) {
                if (item != null) {
                    if (logger.isDebugEnabled())
                        logger.debug("original MemCachedItem {}", item.toString());
                    itemMap.get(item.getMemCachedKey()).merge(item);
                    existCount++;
                }
            }
            long point2 = System.currentTimeMillis();
            redisTemplate.executePipelined(new RedisCallback<Void>() {
                public Void doInRedis(RedisConnection connection) throws DataAccessException {
                    for (Entry<String, MemCachedItem> en : itemMap.entrySet()) {
                        connection.setEx(redisTemplate.getKeySerializer().serialize(en.getKey()), 
                        		expireSeconds, 
                        		redisTemplate.getValueSerializer().serialize(en.getValue()));
                    }
                    return null;
                }
            });
            long point3 = System.currentTimeMillis();
            logger.info("t:{}, m:{}. g:[{}ms], m:[{}ms], s:[{}ms].", itemMap.size(), existCount, point1 - start, point2
                    - point1, point3 - point2);
        } catch (Throwable e) {
            logger.warn(e.getMessage(), e);
            throw new IOException(e);
        }
    }
}