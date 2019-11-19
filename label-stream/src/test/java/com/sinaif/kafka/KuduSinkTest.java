package com.sinaif.kafka;

import com.sinaif.stream.common.model.business.MysqlChangeLog;
import com.sinaif.stream.kudu.connector.KuduEventSaver;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @File : KuduSinkTest.java
 * @Author: tupingping
 * @Date : 2019/8/8
 * @Desc :
 */
public class KuduSinkTest extends RichSinkFunction<MysqlChangeLog>{
    private static final long serialVersionUID = -2395219229676136209L;

    @Override
    public void invoke(MysqlChangeLog value, Context context) throws Exception {
        System.out.println("sink: " + value);
        KuduEventSaver.saveOrUpdateRecord(value.data, KuduEventSaver.WriteMode.INSERT);
    }
}
