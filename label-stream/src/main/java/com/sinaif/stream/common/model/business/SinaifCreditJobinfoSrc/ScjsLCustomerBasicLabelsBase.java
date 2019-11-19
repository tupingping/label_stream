package com.sinaif.stream.common.model.business.SinaifCreditJobinfoSrc;

import com.sinaif.stream.common.model.business.CommonBusiness;
import com.sinaif.stream.common.model.business.ModifyHistory;
import com.sinaif.stream.common.utils.AESToLong;
import com.sinaif.stream.common.utils.ConfigRespostory;
import com.sinaif.stream.kudu.connector.KuduColumn;
import com.sinaif.stream.kudu.connector.KuduColumnIgnore;
import com.sinaif.stream.kudu.connector.KuduDomain;
import com.sinaif.stream.kudu.connector.KuduEventSaver;
import com.sinaif.stream.redis.RedisPool;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @Time : 2019/8/23 17:01
 * @Author : pingping.tu
 * @File : ScjsLCustomerBasicLabelsBase.py
 * @Email : flatuer@gmail.com
 * @Description :
 */
@KuduDomain(table = "impala::stream_test.l_customer_basic_labels")
public class ScjsLCustomerBasicLabelsBase extends CommonBusiness {

    @KuduColumn(property = "ua_phone_num")
    public String username;

    @KuduColumn(property = "credit_incomecode")
    public String incomecode;

    @KuduColumn(property = "credit_marriage")
    public String str1;

    @KuduColumn(property = "credit_email")
    public String email;

    @KuduColumn(property = "credit_degreecode")
    public String degreecode;

    @KuduColumnIgnore
    public String productid;

    @KuduColumnIgnore
    public String userid;


    @Override
    public Boolean filter() {
        if(!(productid.equals("2001") || productid.equals("2005"))){
            return false;
        }

        String field = userid + "@" + productid;
        String value = RedisPool.hget(ConfigRespostory.value("redis.cache.hash.key"), field);
        if(value == null){
            return false;
        }

        String[] values = value.split("@");
        customerId = Long.valueOf(values[1]);
        username = values[0];

        return super.filter();
    }

    @Override
    public void prepare(Object src) {
        super.TypeConventer();

        customerId = AESToLong.convertToNumber(username);
    }

    @Override
    public List<Tuple2<Object, KuduEventSaver.WriteMode>> convert() {
        List<Tuple2<Object, KuduEventSaver.WriteMode>> result = new ArrayList<>();

        // TODO: add data objects

        // b_customer_basic_labels
        result.add(this.get(this));


        if(enableHistory && rawType.toLowerCase().equals("update")){
            ModifyHistory history = ModifyHistory.generate(log, rawType);
            result.add(new Tuple2<>(history, KuduEventSaver.WriteMode.INSERT));
        }

        return result;
    }
}
