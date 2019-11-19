package com.sinaif.stream.kudu.connector;

import java.util.List;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import com.sinaif.stream.kudu.connector.KuduEventSaver.WriteMode;

public class KuduBatchSink extends RichSinkFunction<List<Tuple2<Object, WriteMode>>> {

	private static final long serialVersionUID = -2395219229676136209L;

	@Override
	public void invoke(List<Tuple2<Object, WriteMode>> values, Context context) throws Exception {
		if(values == null) return;

		for (Tuple2<Object, WriteMode> value : values) {
			KuduEventSaver.saveOrUpdateRecord(value.f0, value.f1);
		}

	}

}