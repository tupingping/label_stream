package com.sinaif.stream.operator.datainput;

import com.sinaif.stream.common.model.business.MysqlChangeLog;
import com.sinaif.stream.common.utils.ExecutionEnvUtil;
import com.sinaif.stream.kudu.connector.KuduBatchSink;
import com.sinaif.stream.kudu.connector.KuduEventSaver;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.List;

public class StreamDataProcessHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamDataProcessHandler.class);

    public static void main(String[] args) throws Exception {

        if(args.length == 0){
            LOGGER.error("Please, make sure the number of input parameter more than one.");
        }

        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool();
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        FlinkKafkaConsumer011<MysqlChangeLog> kafkaConsumer = KafkaConfigUtil.buildSource(args[0],0L, MysqlChangeLog.class);
        DataStreamSource<MysqlChangeLog> source = env.addSource(kafkaConsumer);

        source.filter(new FilterFunction<MysqlChangeLog>() {
                    @Override
                    public boolean filter(MysqlChangeLog mysqlChangeLog) throws Exception {
                        Boolean isVaild = false;
                        try{
                            isVaild = mysqlChangeLog.data.filter();
                        }catch (Exception e){
                            LOGGER.error("Data streaming filter exception. message: {}. Exception: {}", mysqlChangeLog, e);
                        }

                        return isVaild;
            }
        }).map(new MapFunction<MysqlChangeLog, List<Tuple2<Object, KuduEventSaver.WriteMode>>>() {

            @Override
            public List<Tuple2<Object, KuduEventSaver.WriteMode>> map(MysqlChangeLog log) throws Exception {
                try{
                    log.data.log = log;
                    return log.data.convert();
                }catch (Exception e){
                    LOGGER.error("Data streaming map exception. message: {}. Exception: {}", log, e);
                }

                return null;
            }
        }).addSink(new KuduBatchSink());


        env.execute("Label data streaming");
    }
}
