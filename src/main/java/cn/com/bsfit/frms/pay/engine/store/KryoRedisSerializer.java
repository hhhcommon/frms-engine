package cn.com.bsfit.frms.pay.engine.store;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.SerializationException;

import cn.com.bsfit.frms.obj.MemCachedItem;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;

/**
 * This implementation uses the <a
 * href="http://code.google.com/p/kryo/">Kryo</a> serialization.
 */
public class KryoRedisSerializer<T> implements RedisSerializer<T> {
	private final Class<T> type;
	
	public KryoRedisSerializer(Class<T> paramClass) {
		this.type = paramClass;
	}

    private KryoFactory factory = new KryoFactory() {
        public Kryo create() {
            Kryo kryo = new Kryo();
            kryo.register(MemCachedItem.class, new MemCachedItemSerializer());
            // configure kryo instance, customize settings
            return kryo;
        }
    };

    // Build pool with SoftReferences enabled (optional)
    private KryoPool pool = new KryoPool.Builder(factory).softReferences().build();

	@Override
	public byte[] serialize(T t) throws SerializationException {
        if (t == null) {
            return null;
        }
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output output = new Output(baos);
        Kryo kryo = null;
        try {
            kryo = pool.borrow();
            kryo.writeObject(output, t);
        } catch (Exception e) {
            throw new SerializationException("serialize error", e);
        } finally {
            if (output != null) {
                output.flush();
                output.close();
            }
            if (kryo != null) {
                pool.release(kryo);
            }
        }
        return baos.toByteArray();
	}

	@Override
	public T deserialize(byte[] bytes) throws SerializationException {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
		Input input = new Input(new ByteArrayInputStream(bytes));
        Kryo kryo = null;
        try {
            kryo = pool.borrow();
            return kryo.readObject(input, this.type);
        } catch (Exception e) {
            throw new SerializationException("deserialize error", e);
        } finally {
            if (input != null) {
                input.close();
            }
            if (kryo != null) {
                pool.release(kryo);
            }
        }
	}
}