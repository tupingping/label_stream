package com.sinaif.stream.common.model.business.SinaifEasyUserAccountSrc;

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

import java.util.*;

/**
 * @Time : 2019/8/21 12:08
 * @Author : pingping.tu
 * @File : SccsBCustomerInfoBase.py
 * @Email : flatuer@gmail.com
 * @Description :
 */
@KuduDomain(table = "impala::stream_test.b_customer_info")
public class SeuasBCustomerInfoBase extends CommonBusiness {

    @KuduColumn(property = "phone_num")
    public String username;

    @KuduColumn(property = "create_time")
    public Date registtime;

    @KuduColumnIgnore
    public String id;

    @KuduColumnIgnore
    public String productid;

    @KuduColumnIgnore
    public String redisField;

    @KuduColumnIgnore
    public String redisValue;

    @Override
    public Boolean filter() {
        if(username == null){
            return false;
        }
        return super.filter();
    }

    @Override
    public void prepare(Object src) {
        rawType = log.type;
        if(log.type.equals("update"))
            log.type = "insert";
        else if(log.type.equals("delete")){
            log.type = "update";
            flag_del = 1;
        }

        customerId = AESToLong.convertToNumber(username);

        redisField = id + "@1003";
        redisValue = username + "@" + customerId;
    }

    @Override
    public List<Tuple2<Object, KuduEventSaver.WriteMode>> convert() {
        List<Tuple2<Object, KuduEventSaver.WriteMode>> result = new ArrayList<>();

        // TODO: add data objects

        // b_customer_info
        result.add(this.get(this));

        // l_customer_basic_labels
        SeuasLCustomerBasicLabels seuasLCustomerBasicLabels = new SeuasLCustomerBasicLabels();
        Tuple2<Object, KuduEventSaver.WriteMode> tmp = seuasLCustomerBasicLabels.get(this);
        if(((SeuasLCustomerBasicLabels)tmp.f0).filter()){
            result.add(tmp);
        }

        // b_customer_action_product
        SeuaBCustomerActionProduct seuaBCustomerActionProduct = new SeuaBCustomerActionProduct();
        Tuple2<Object, KuduEventSaver.WriteMode> tmp1 = seuaBCustomerActionProduct.get(this);
        if(((SeuaBCustomerActionProduct)tmp1.f0).filter()){
            result.add(tmp1);
        }

        // b_customer_product_registe
        SeuaBCustomerProductRegiste seuaBCustomerProductRegiste = new SeuaBCustomerProductRegiste();
        Tuple2<Object, KuduEventSaver.WriteMode> tmp2 = seuaBCustomerProductRegiste.get(this);
        if(((SeuaBCustomerProductRegiste)tmp2.f0).filter()){
            result.add(tmp2);
        }

        if(enableHistory && rawType.toLowerCase().equals("update")){
            ModifyHistory history = ModifyHistory.generate(log, rawType);
            result.add(new Tuple2<>(history, KuduEventSaver.WriteMode.INSERT));
        }

        redisCache();

        return result;
    }

    private void redisCache() {
        if ("insert".equals(log.type) || "update".equals(log.type)) {
            Map<String, String> values = new HashMap<>();
            values.put(redisField, redisValue);
            RedisPool.hset(ConfigRespostory.value("redis.cache.hash.key"),values);
        } else if ("delete".equals(log.type)) {
            //TODO remove it from redis cache?

        }
    }
}
