package com.sinaif.stream.common.model.business.SinaifUserAccountSrc;

/**
 * @Time : 2019/8/20 15:18
 * @Author : pingping.tu
 * @File : SccsBCustomerInfoBase.py
 * @Email : flatuer@gmail.com
 * @Description :
 */

import com.alibaba.fastjson.annotation.JSONField;
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

@KuduDomain(table = "impala::stream_test.b_customer_info")
public class SuasBCustomerInfoBase extends CommonBusiness {

    @KuduColumnIgnore
    public String id;

    @KuduColumn(property = "phone_num")
    public String username;

    @KuduColumn(property = "create_time")
    public Date registtime;

    @KuduColumnIgnore
    public String productid;

    @JSONField(serialize = false, deserialize = false)
    @KuduColumnIgnore
    public String redisField;

    @JSONField(serialize = false, deserialize = false)
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
        super.TypeConventer();

        customerId = AESToLong.convertToNumber(username);

        redisField = id + "@" + productid;
        redisValue = username + "@" + customerId;
    }


    @Override
    public List<Tuple2<Object, KuduEventSaver.WriteMode>> convert() {
        List<Tuple2<Object, KuduEventSaver.WriteMode>> result = new ArrayList<>();

        // TODO: add data objects

        // b_customer_info
        result.add(this.get(this));

        // l_customer_basic_labels
        SuasLCustomerBasicLabels suasLCustomerBasicLabels = new SuasLCustomerBasicLabels();
        Tuple2<Object, KuduEventSaver.WriteMode> tmp = suasLCustomerBasicLabels.get(this);
        if(((SuasLCustomerBasicLabels)tmp.f0).filter()){
            result.add(tmp);
        }

        // b_customer_action_product
        SuasBCustomerActionProduct suasBCustomerActionProduct = new SuasBCustomerActionProduct();
        Tuple2<Object, KuduEventSaver.WriteMode> tmp1 = suasBCustomerActionProduct.get(this);
        if(((SuasBCustomerActionProduct)tmp1.f0).filter()){
            result.add(tmp1);
        }

        // b_customer_product_registe
        SuasBCustomerProductRegiste suasBCustomerProductRegiste = new SuasBCustomerProductRegiste();
        Tuple2<Object, KuduEventSaver.WriteMode> tmp2 = suasBCustomerProductRegiste.get(this);
        if(((SuasBCustomerProductRegiste)tmp2.f0).filter()){
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
            if(productid.equals("1001") || productid.equals("1002")){
                values.put(id + "@" + "1001", redisValue);
                values.put(id + "@" + "1002", redisValue);
            }else{
                values.put(redisField, redisValue);
            }

            RedisPool.hset(ConfigRespostory.value("redis.cache.hash.key"),values);
        } else if ("delete".equals(log.type)) {
            //TODO remove it from redis cache?

        }
    }
}
