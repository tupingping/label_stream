package com.sinaif.stream.common.model;

import com.sinaif.stream.common.utils.TypeRefSerialize;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.alibaba.fastjson.JSON;

import java.io.IOException;

/**
 * Convert kafka message to Object class
 * 
 * @author simonzhang
 *
 * @param <T>
 */
public class EventSchema<T> implements DeserializationSchema<T>, SerializationSchema<T> {

	
    public TypeRefSerialize<T> reference;
	
	private Class<T> clazz;
	
	
    public EventSchema(TypeRefSerialize<T> reference, Class<T> clazz) {
		this.reference = reference;
		this.clazz = clazz;
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
    public T deserialize(byte[] bytes) throws IOException {
    	return JSON.parseObject(new String(bytes), reference);
    }

    @Override
    public boolean isEndOfStream(T metricEvent) {
        return false;
    }

    @Override
    public byte[] serialize(T paylod) {
        return JSON.toJSONBytes(paylod);
    }

    @Override
    public TypeInformation<T> getProducedType() {
    	
        return  TypeInformation.of(clazz);
    }

	
}
