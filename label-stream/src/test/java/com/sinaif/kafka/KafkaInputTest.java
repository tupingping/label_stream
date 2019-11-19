package com.sinaif.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import com.sinaif.stream.kudu.connector.KuduBatchSink;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sinaif.stream.common.model.business.MysqlChangeLog;
import com.sinaif.stream.common.utils.ExecutionEnvUtil;
import com.sinaif.stream.operator.datainput.KafkaConfigUtil;

/**
 * @File : com.sinaif.kafka.KafkaInputTest.java
 * @Author: tupingping
 * @Date : 2019/8/6
 * @Desc :
 */
public class KafkaInputTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaInputTest.class);
    public static void main(String[] args) throws Exception{
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool();
        final  AtomicInteger a = new AtomicInteger(0);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);

        FlinkKafkaConsumer011<MysqlChangeLog> kafkaConsumer = KafkaConfigUtil.buildSource(args[0],0L, MysqlChangeLog.class);
        DataStreamSource<MysqlChangeLog> source = env.addSource(kafkaConsumer);

        source.filter(new FilterFunction<MysqlChangeLog>() {
            @Override
            public boolean filter(MysqlChangeLog mysqlChangeLog) throws Exception {
                System.out.println(a.incrementAndGet());
                System.out.println("fliter:" + mysqlChangeLog.data.update_dt);
                return true;
            }
        }).addSink(new KuduSinkTest());

        env.execute("test");
    }
}
