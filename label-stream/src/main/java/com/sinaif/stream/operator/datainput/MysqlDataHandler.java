
package com.sinaif.stream.operator.datainput;

import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sinaif.stream.common.model.business.MysqlChangeLog;
import com.sinaif.stream.common.utils.ConfigRespostory;
import com.sinaif.stream.common.utils.ExecutionEnvUtil;
import com.sinaif.stream.kudu.connector.KuduBatchSink;
import com.sinaif.stream.kudu.connector.KuduEventSaver.WriteMode;

/**
 * 
 * TODO refact schedule!!!
 * 
 * use for handle business data stream
 */
public class MysqlDataHandler {

	private static final Logger LOGGER = LoggerFactory.getLogger(MysqlDataHandler.class);

	@SuppressWarnings({ "rawtypes", "serial" })
	public static void main(String[] args) throws Exception {
		//TODO check args parameter, error if some not  right
		
		// parse input arguments
		final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool();

		StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
		
		String topic_type = args[0];

		FlinkKafkaConsumer011<MysqlChangeLog> kafkaConsumer = KafkaConfigUtil.buildSource(topic_type,0L, MysqlChangeLog.class);
		 
		DataStreamSource<MysqlChangeLog> source = env.addSource(kafkaConsumer) ;
		
		final Boolean historyed = Boolean.valueOf(ConfigRespostory.value(topic_type+ConfigRespostory._HISTORY_ENABLE));
		LOGGER.info("start to handle topic {}, and need to save to history: {}", topic_type, historyed);
		
	    source.map(new MapFunction<MysqlChangeLog, List<Tuple2<Object, WriteMode>>>() {
	    	
			@Override
			public List<Tuple2<Object, WriteMode>> map(MysqlChangeLog log) throws Exception {
				//0, init  for data convert
				log.data.log = log;
				
				//1 save to redis if insert action if config
				return log.data.convert();
				
			}
		}).addSink(new KuduBatchSink()).setParallelism(4);


		env.execute("operator data stream");

	}

}
