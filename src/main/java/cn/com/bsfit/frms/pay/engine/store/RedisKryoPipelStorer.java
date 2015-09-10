//package cn.com.bsfit.frms.pay.engine.store;
//
//import cn.com.bsfit.frms.ds.pojo.CacheUser;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.beans.factory.annotation.Qualifier;
//import org.springframework.dao.DataAccessException;
//import org.springframework.data.redis.connection.RedisConnection;
//import org.springframework.data.redis.core.RedisCallback;
//import org.springframework.data.redis.core.RedisTemplate;
//
//import java.util.Collection;
//
//@SuppressWarnings({"rawtypes", "unchecked"})
//public class RedisKryoPipelStorer {
//
//    private static final Logger logger = LoggerFactory.getLogger(RedisKryoPipelStorer.class);
//
//    private KryoRedisSerializer<CacheUser> kryoRedisSerializer = new KryoRedisSerializer<CacheUser>(
//            CacheUser.class);
//
//    @Autowired
//    @Qualifier("cacheUserRedisTemplate")
//    protected RedisTemplate redisTemplate;
//
//    public void store(final Collection<CacheUser> CacheUsers) {
//        redisTemplate.setValueSerializer(kryoRedisSerializer);
//        try {
//            redisTemplate.executePipelined(new RedisCallback<Void>() {
//                public Void doInRedis(RedisConnection connection) throws DataAccessException {
//                    try {
//                        for (CacheUser CacheUser : CacheUsers) {
//                            connection.set(redisTemplate.getKeySerializer().serialize(CacheUser.getUserId()),
//                                    redisTemplate.getValueSerializer().serialize(CacheUser));
//                        }
//                    } catch (Exception e) {
//                        logger.error("RedisKryoPipelStorer store error", e);
//                    }
//                    return null;
//                }
//            });
//        } catch (Throwable e) {
//            logger.warn(e.getMessage(), e);
//        }
//    }
//
//    public Collection<CacheUser> get(final Collection<String> userIdList) {
//        redisTemplate.setValueSerializer(kryoRedisSerializer);
//        return redisTemplate.executePipelined(new RedisCallback<CacheUser>() {
//            public CacheUser doInRedis(RedisConnection connection) throws DataAccessException {
//                for (String userId : userIdList) {
//                    connection.get(redisTemplate.getKeySerializer().serialize(userId));
//                }
//                return null;
//            }
//        });
//    }
//
//}
