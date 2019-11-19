package com.sinaif.stream.common.model.business.SinaifCreditBankInfoSrc;


import com.sinaif.stream.common.model.business.CommonBusiness;
import com.sinaif.stream.common.utils.BankUtil;
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
 * @Time : 2019/8/29 16:36
 * @Author : pingping.tu
 * @File : SbcbsLCustomerFinanceLabelsBase.py
 * @Email : flatuer@gmail.com
 * @Description :
 */
@KuduDomain(table = "impala::stream_test.l_customer_finance_labels")
public class SbcbsLCustomerFinanceLabelsBase extends CommonBusiness {

    @KuduColumn(property = "bank_code")
    public String bankcode;

    @KuduColumnIgnore
    public String banktype;

    @KuduColumnIgnore
    public String userid;

    @KuduColumnIgnore
    public String productid;

    @Override
    public Boolean filter() {

        String field = userid + "@" + productid;
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

        if(banktype.equals("2")){
            bankcode = BankUtil.getBank(bankcode);
        }
    }

    @Override
    public List<Tuple2<Object, KuduEventSaver.WriteMode>> convert() {
        List<Tuple2<Object, KuduEventSaver.WriteMode>> result = new ArrayList<>();

        // TODO: add data objects

        // l_customer_finance_labels
        result.add(this.get(this));

        return result;
    }
}
