package com.sinaif.stream.common.model.business.SinaifKingCreditInfoSrc;

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
 * @Time : 2019/8/23 18:12
 * @Author : pingping.tu
 * @File : SkcsBCustomerActionProductBase.py
 * @Email : flatuer@gmail.com
 * @Description :
 */
@KuduDomain(table = "impala::stream_test.b_customer_action_product")
public class SkcsBCustomerActionProductBase extends CommonBusiness {

    public String action = "credit";

    @KuduColumn(property = "op_time")
    public Date createtime;

    @KuduColumn(property = "terminal_code")
    public String terminalid;

    @KuduColumn(property = "product_code")
    public String productid;

    public Integer action_code = 4;

    public Long product_code_hash;

    @KuduColumn(property = "amount")
    public Double totallimit ;

    @KuduColumnIgnore
    public String userid;

    @KuduColumnIgnore
    public Integer issync;

    @KuduColumnIgnore
    public Integer enabled;

    @Override
    public Boolean filter() {

        if(!(enabled >= 0 && enabled  <= 2)) {
            return false;
        }

        if(!(terminalid.equals("2005")||(terminalid.equals("2001") && issync == 0))){
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
