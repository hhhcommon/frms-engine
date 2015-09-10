package cn.com.bsfit.frms.pay.engine.loader;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisZSetCommands.Tuple;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;
import redis.clients.jedis.ShardedJedisPool;
import cn.com.bsfit.frms.base.load.DataLoader;
import cn.com.bsfit.frms.obj.MemCachedItem;
import cn.com.bsfit.frms.pay.engine.store.KryoRedisSerializer;
import cn.com.bsfit.frms.pay.engine.utils.IpUtil;
import cn.com.bsfit.frms.sample.pay.pojo.TbIpArea;
import cn.com.bsfit.frms.sample.pay.pojo.TbMobileRegion;

public abstract class BaseNoSqlLoader implements DataLoader {
    private Logger logger = LoggerFactory.getLogger(BaseNoSqlLoader.class);

    protected RedisSerializer<String> keySerializer = new StringRedisSerializer();
    protected RedisSerializer<String> stringSerializer = new StringRedisSerializer();
    protected RedisSerializer<MemCachedItem> valueSerializer = new KryoRedisSerializer<MemCachedItem>(MemCachedItem.class);
    @SuppressWarnings("rawtypes")
    protected RedisSerializer defaultSerializer = new JdkSerializationRedisSerializer();

    public abstract String getBizCode();

    @Autowired
    protected ShardedJedisPool shardedJedisPool;

    @SuppressWarnings("rawtypes")
    @Autowired
    protected RedisTemplate redisTemplate;

    public static boolean isSameDay(Date date1, Date date2) {
        if (date1 == null || date2 == null)
            return false;
        Calendar cal1 = Calendar.getInstance();
        cal1.setTime(date1);
        Calendar cal2 = Calendar.getInstance();
        cal2.setTime(date2);
        return (cal1.get(Calendar.ERA) == cal2.get(Calendar.ERA) && cal1.get(Calendar.YEAR) == cal2.get(Calendar.YEAR) && cal1
                .get(Calendar.DAY_OF_YEAR) == cal2.get(Calendar.DAY_OF_YEAR));

    }

    public MemCachedItem getMemCachedItem(final String keyId) {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = shardedJedisPool.getResource();
            byte[] key = keySerializer.serialize(keyId);
            byte[] value = shardedJedis.get(key);
            if (value == null) {
                return null;
            }
            return (MemCachedItem) valueSerializer.deserialize(value);
        } finally {
            shardedJedisPool.returnResourceObject(shardedJedis);
        }
    }


    public List<MemCachedItem> getMemCachedItem(final Collection<String> keyIds) {
        List<MemCachedItem> items = new ArrayList<MemCachedItem>(keyIds.size());
        ShardedJedis shardedJedis = null;
        ShardedJedisPipeline pipeline = null;
        try {
            shardedJedis = shardedJedisPool.getResource();
            pipeline = shardedJedis.pipelined();
            for (String keyId : keyIds) {
                pipeline.get(keySerializer.serialize(keyId));
            }
            List<Object> list = pipeline.syncAndReturnAll();
            for (Object o : list) {
                items.add(valueSerializer.deserialize((byte[]) o));
            }
        } catch (Exception e) {
            logger.error("Get keys error {}", keyIds, e);
        } finally {
            shardedJedisPool.returnResourceObject(shardedJedis);
        }
        return items;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public TbIpArea ip2City(final String ipString) {
        return (TbIpArea) redisTemplate.execute(new RedisCallback() {
            long timeStart = System.currentTimeMillis();

            public TbIpArea doInRedis(RedisConnection connection) throws DataAccessException {
                String[] ipStringList = ipString.split("\\.");
                if ((ipStringList != null) && (ipStringList.length == 4)) {
                    long ipLong = IpUtil.ipToLong(ipString).longValue();
                    RedisSerializer serializer = redisTemplate.getStringSerializer();
                    RedisSerializer serializerObj = redisTemplate.getDefaultSerializer();
                    byte[] keys = serializer.serialize("iplib");
                    Set<Tuple> set = connection.zRangeByScoreWithScores(keys, ipLong, 4294967295L, 0, 1);
                    String cityKey = "";
                    if ((set != null) && (set.size() > 0)) {
                        Iterator localIterator = set.iterator();
                        if (localIterator.hasNext()) {
                            Tuple tuple = (Tuple) localIterator.next();
                            long score = tuple.getScore().longValue();
                            String tmp = (String) serializer.deserialize(tuple.getValue());
                            if (tmp == null) {
                                return null;
                            }
                            if (tmp.contains("end")) {
                                cityKey = tmp.replace("_end", "");
                            } else if (tmp.contains("start") && score == ipLong) {
                                cityKey = tmp.replace("_start", "");
                            }
                        }

                        if (cityKey.equals("")) {
                            return null;
                        }
                        byte[] key = serializer.serialize("iplib" + cityKey);
                        byte[] values = connection.get(key);
                        if ((values != null) && (values.length > 0)) {
                            TbIpArea tipdata = (TbIpArea) serializerObj.deserialize(values);
                            if (tipdata != null) {
                                logger.info("ip查询城市,从redis查询 object[{}] latency=[{}ms]", tipdata.getProvince(), Long.valueOf(System.currentTimeMillis() - this.timeStart));
                            }
                            return tipdata;
                        }
                    }
                }
                return null;
            }
        });
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public TbMobileRegion mobile2City(final String mobile) {
        return (TbMobileRegion) redisTemplate.execute(new RedisCallback() {
            long timeStart = System.currentTimeMillis();
            public TbMobileRegion doInRedis(RedisConnection connection) throws DataAccessException {
                RedisSerializer serializer = redisTemplate.getStringSerializer();
                RedisSerializer serializerObj = redisTemplate.getDefaultSerializer();
                try{
                    byte[] key = serializer.serialize(mobile);
                    byte[] values = connection.get(key);
                    if ((values != null) && (values.length > 0)) {
                        TbMobileRegion mobileData = (TbMobileRegion) serializerObj.deserialize(values);
                        if (mobileData != null) {
                            logger.debug("mobile查询城市,从redis查询 object[{}] latency=[{}ms]", mobileData.toString(), Long.valueOf(System.currentTimeMillis() - this.timeStart));
                        }
                        return mobileData;
                    }
                    return null;
                }catch(Exception e){
                    logger.error("没有对应的mobile城市");
                    return null;
                }
            }
        });
    }
}