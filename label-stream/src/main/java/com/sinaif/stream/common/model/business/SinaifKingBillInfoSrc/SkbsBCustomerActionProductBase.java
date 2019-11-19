package com.sinaif.stream.common.model.business.SinaifKingBillInfoSrc;

import com.sinaif.stream.common.model.business.CommonBusiness;
import com.sinaif.stream.common.utils.ConfigRespostory;
import com.sinaif.stream.common.utils.HashUtil;
import com.sinaif.stream.kudu.connector.KuduColumn;
import com.sinaif.stream.kudu.connector.KuduColumnIgnore;
import com.sinaif.stream.kudu.connector.KuduDomain;
import com.sinaif.stream.kudu.connector.KuduEventSaver;
import com.sinaif.stream.redis.RedisPool;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @Time : 2019/8/23 18:10
 * @Author : pingping.tu
 * @File : SkbsBCustomerActionProductBase.py
 * @Email : flatuer@gmail.com
 * @Description :
 */
@KuduDomain(table = "impala::stream_test.b_customer_action_product")
public class SkbsBCustomerActionProductBase extends CommonBusiness {

    public String action = "loan";

    @KuduColumn(property = "op_time")
    public Date createtime;

    @KuduColumn(property = "terminal_code")
    public String terminalid;

    @KuduColumn(property = "product_code")
    public String productid;

    @KuduColumn(property = "amount")
    public Double applyamount;

    public Integer action_code = 5;

    public Long product_code_hash;

    @KuduColumnIgnore
    public String userid;

    @KuduColumnIgnore
    public String source;

    @KuduColumnIgnore
    public Integer issync;

    @Override
    public Boolean filter() {
        if(!((source.equals("1") && issync == 0 && terminalid.equals("2001"))||(source.equals("1") && terminalid.equals("2005")))){
            return false;
        }

        String field = userid + "@" + terminalid;
        String value = RedisPool.hget(ConfigRespostory.value("redis.cache.hash.key"), field);
        if(value == null){
            return false;
        }

        String[] values = value.split("@");
        customerId = Long.valueOf(values[1]);

        return super.filter();
    }

    @Override
    public void prepare(Object src) {
        super.TypeConventer();

        product_code_hash = Long.valueOf(HashUtil.hash(productid));
    }

    @Override
    public List<Tuple2<Object, KuduEventSaver.WriteMode>> convert() {
        List<Tuple2<Object, KuduEventSaver.WriteMode>> result = new ArrayList<>();

        // TODO: add data objects

        // b_customer_action_product
        result.add(this.get(this));

        return result;
    }
}
