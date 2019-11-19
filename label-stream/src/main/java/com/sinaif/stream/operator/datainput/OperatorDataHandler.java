/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sinaif.stream.operator.datainput;

import java.util.Calendar;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sinaif.stream.common.model.operator.UserCall;
import com.sinaif.stream.common.utils.ConfigRespostory;
import com.sinaif.stream.common.utils.ExecutionEnvUtil;
import com.sinaif.stream.kudu.connector.KuduEventSaver;
import com.sinaif.stream.kudu.connector.KuduEventSaver.WriteMode;

/**
 * 
 * TODO refact schedule!!!
 * 
 * 
 */
public class OperatorDataHandler {

	private static final Logger LOGGER = LoggerFactory.getLogger(OperatorDataHandler.class);
	/**
	 * shutdown if reach error threshold
	 */
	private static AtomicInteger ERROR_THRESHOLD = new AtomicInteger();

	public static void main(String[] args) throws Exception {
		// TODO check args parameter, error if some not right

		// parse input arguments
		final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool();

		StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);

		String topic = ConfigRespostory.value(args[0]);
		// KafkaConfie
		FlinkKafkaConsumer011<UserCall> kafkaConsumer = KafkaConfigUtil.buildSource(topic, 0L, UserCall.class);

		DataStreamSource<UserCall> source = env.addSource(kafkaConsumer);
		SingleOutputStreamOperator<UserCall> dataStream = source.map((a)->{return a;})
		  .filter((a) -> a != null)
		  .keyBy((a) -> {
			return a;
		}).timeWindow(Time.minutes(1)).reduce((target, event) -> {
			target.use_time += event.use_time;
			target.subtoal += event.subtoal;
			target.callTimes += 1;
			target.analysisByHour(event);
			return target;
		});

		dataStream.addSink(new KuduSaverSink()).name("save_update_kudu") // task name
				.setParallelism(4); // task parallelism, here means 5 threads

		env.execute("operator data stream");

	}

	@SuppressWarnings("serial")
	public static class KuduSaverSink extends RichSinkFunction<UserCall> {

		@Override
		public void invoke(UserCall value, Context context) throws Exception {
			KuduEventSaver.saveOrUpdateRecord(value, WriteMode.UPSERT);
		}
	}

}
