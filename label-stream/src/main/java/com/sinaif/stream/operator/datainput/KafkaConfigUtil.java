package com.sinaif.stream.operator.datainput;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import com.sinaif.stream.common.model.Event;
import com.sinaif.stream.common.model.EventSchema;
import com.sinaif.stream.common.model.EventWatermark;
import com.sinaif.stream.common.utils.ConfigRespostory;

/**
 * Init kafka env, receive json data and convert to necessary object structure automatically
 * 
 * @author simonzhang
 *
 */
public class KafkaConfigUtil {

    private static Properties buildKafkaProps(String prefix) {
        Properties props = new Properties();
        props.put("bootstrap.servers", ConfigRespostory.value("kafka.brokers"));
        props.put("zookeeper.connect", ConfigRespostory.value("kafka.zookeeper.connect"));
        props.put("group.id", ConfigRespostory.value(prefix + ConfigRespostory._KAFKA_GROUP));
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");
        return props;
    }
    
    /**
     * @param type
     * @param time 订阅的时间
     * @param clazz
     * @return
     * @throws IllegalAccessException
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <T> FlinkKafkaConsumer011<T> buildSource(String type, Long time, Class<T> clazz) throws IllegalAccessException {
    	String topic_key = type + ConfigRespostory._KAFKA_TOPIC;
    	String topic = ConfigRespostory.value(topic_key);
		FlinkKafkaConsumer011<Event> consumer = new FlinkKafkaConsumer011<>(
                topic,
                new EventSchema(ConfigRespostory.ref(type), clazz),
                buildKafkaProps(type));

        // 重置offset到time时刻
        if (time != 0L) {
            Map<KafkaTopicPartition, Long> partitionOffset = buildOffsetByTime(topic, time);
            consumer.setStartFromSpecificOffsets(partitionOffset);
        }
        return (FlinkKafkaConsumer011<T>) consumer;
    }

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static Map<KafkaTopicPartition, Long> buildOffsetByTime(String topic, Long time) {
        ConfigRespostory.PROPERTIES.setProperty("group.id", "query_time_" + time);
        KafkaConsumer consumer = new KafkaConsumer<Object, Object>(ConfigRespostory.PROPERTIES);
        List<PartitionInfo> partitionsFor = consumer.partitionsFor(topic);
        Map<TopicPartition, Long> partitionInfoLongMap = new HashMap<>();
        for (PartitionInfo partitionInfo : partitionsFor) {
            partitionInfoLongMap.put(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()), time);
        }
        Map<TopicPartition, OffsetAndTimestamp> offsetResult = consumer.offsetsForTimes(partitionInfoLongMap);
        Map<KafkaTopicPartition, Long> partitionOffset = new HashMap<>();
        offsetResult.forEach((key, value) -> partitionOffset.put(new KafkaTopicPartition(key.topic(), key.partition()), value.offset()));

        consumer.close();
        return partitionOffset;
    }

    public static SingleOutputStreamOperator<Event> parseSource(DataStreamSource<Event> dataStreamSource) {
        return dataStreamSource.assignTimestampsAndWatermarks(new EventWatermark());
    }
}
