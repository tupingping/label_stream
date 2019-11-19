package com.sinaif.stream.common.model.business.SinaifBocCreditSrc;

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
 * @Time : 2019/8/23 18:09
 * @Author : pingping.tu
 * @File : SbcsBCustomerActionProductBase.py
 * @Email : flatuer@gmail.com
 * @Description :
 */
@KuduDomain(table = "impala::stream_test.b_customer_action_product")
public class SbcsBCustomerActionProductBase extends CommonBusiness {

    public String action = "credit";

    @KuduColumn(property = "op_time")
    public Date createtime;

    @KuduColumn(property = "terminal_code")
    public String productid;

    public Integer action_code = 4;

    public Long product_code_hash;

    @KuduColumnIgnore
    public String captype;

    public String product_code;

    @KuduColumn(property = "amount")
    public Double creditline;

    @KuduColumnIgnore
    public String userid;

    @Override
    public Boolean filter() {
        if(!(productid.equals("1001")||productid.equals("1002")||productid.equals("1005"))){
            return false;
        }

        if(!(captype.equals("1")||captype.equals("3")||captype.equals("5")||captype.equals("8"))){
            return false;
        }

        String field = userid + "@" + productid;
        String value = RedisPool.hget(ConfigRespostory.value("redis.cache.hash.key"), field);
        if(value == null){
            field = userid + "@1003";
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

        if(productid.equals("1001") && captype.equals("1")){
            product_code = "yj001";
        }else if(productid.equals("1001") && captype.equals("5")) {
            product_code = "yj005";
        }else if(productid.equals("1002") && captype.equals("1")) {
            product_code = "yh001";
        }else if(productid.equals("1002") && captype.equals("3")) {
            product_code = "yh003";
        }else if(productid.equals("1002") && captype.equals("8")) {
            product_code = "yh008";
        }else if(productid.equals("1005") && captype.equals("1")) {
            product_code = "lz001";
        }else {
            product_code = "-1";
        }

        product_code_hash = Long.valueOf(HashUtil.hash(product_code));

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
