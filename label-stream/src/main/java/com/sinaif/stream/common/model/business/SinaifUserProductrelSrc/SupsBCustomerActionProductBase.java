package com.sinaif.stream.common.model.business.SinaifUserProductrelSrc;

import com.sinaif.stream.common.model.business.CommonBusiness;
import com.sinaif.stream.common.utils.ConfigRespostory;
import com.sinaif.stream.common.utils.HashUtil;
import com.sinaif.stream.kudu.connector.KuduColumn;
import com.sinaif.stream.kudu.connector.KuduColumnIgnore;
import com.sinaif.stream.kudu.connector.KuduDomain;
import com.sinaif.stream.kudu.connector.KuduEventSaver;
import com.sinaif.stream.redis.RedisPool;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @Time : 2019/8/23 17:53
 * @Author : pingping.tu
 * @File : SupsBCustomerActionProductBase.py
 * @Email : flatuer@gmail.com
 * @Description :
 */
@KuduDomain(table = "impala::stream_test.b_customer_action_product")
public class SupsBCustomerActionProductBase extends CommonBusiness {

    private static Logger LOGGER = LoggerFactory.getLogger(SupsBCustomerActionProductBase.class);

    public String action = "register";

    public Integer action_code = 1;

    public Long product_code_hash;

    public String product_code = "1";

    @KuduColumn(property = "op_time")
    public Date regtime;

    @KuduColumn(property = "terminal_code")
    public String productid;

    @KuduColumn(property = "channel_id")
    public String channelid;

    @KuduColumnIgnore
    public String accountid;


    @Override
    public Boolean filter() {

        if(!(productid.equals("1001") || productid.equals("1002"))){
            return false;
        }

        String field = accountid + "@" + productid;
        String value = RedisPool.hget(ConfigRespostory.value("redis.cache.hash.key"), field);
        if(value == null){
            field = accountid + "@1003";
            value = RedisPool.hget(ConfigRespostory.value("redis.cache.hash.key"), field);
            if(value == null){
                return false;
            }
        }

        String[] values = value.split("@");
        customerId = Long.valueOf(values[1]);

        return super.filter();
    }

    @Override
    public void prepare(Object src) {
        super.TypeConventer();
        product_code_hash = Long.valueOf(HashUtil.hash(product_code));
    }

    @Override
    public List<Tuple2<Object, KuduEventSaver.WriteMode>> convert() {
        List<Tuple2<Object, KuduEventSaver.WriteMode>> result = new ArrayList<>();

        // TODO: add data objects

        // b_customer_action_product
        result.add(this.get(this));

        // b_customer_product_registe
        SupsBCustomerProductRegiste supsBCustomerProductRegiste = new SupsBCustomerProductRegiste();
        Tuple2<Object, KuduEventSaver.WriteMode> tmp = supsBCustomerProductRegiste.get(this);
        if(((SupsBCustomerProductRegiste)tmp.f0).filter()){
            result.add(tmp);
        }

        return result;
    }
}
