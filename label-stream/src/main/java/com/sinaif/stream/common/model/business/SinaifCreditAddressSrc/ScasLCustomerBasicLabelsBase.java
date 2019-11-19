package com.sinaif.stream.common.model.business.SinaifCreditAddressSrc;

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
 * @Time : 2019/8/23 17:27
 * @Author : pingping.tu
 * @File : ScasLCustomerBasicLabelsBase.py
 * @Email : flatuer@gmail.com
 * @Description :
 */
@KuduDomain(table = "impala::stream_test.l_customer_basic_labels")
public class ScasLCustomerBasicLabelsBase extends CommonBusiness {

    @KuduColumn(property = "credit_graduationyear")
    public Integer graduationyear;

    @KuduColumn(property = "ua_phone_num")
    public String username;

    @KuduColumn(property = "credit_graduationschool")
    public String graduationschool;

    @KuduColumn(property = "credit_learningform")
    public String learningform;

    @KuduColumn(property = "credit_home_province")
    public String addrprovince;

    @KuduColumn(property = "credit_home_city")
    public String addrcity;

    @KuduColumn(property = "credit_home_area")
    public String addrarea;

    @KuduColumn(property = "credit_home_addr")
    public String fulladdr;

    @KuduColumnIgnore
    public Integer addrtype;

    @KuduColumnIgnore
    public String productid;

    @KuduColumnIgnore
    public String userid;

    @KuduColumnIgnore
    public String companyname;

    @KuduColumnIgnore
    public String companyzone;

    @KuduColumnIgnore
    public String comptelephone;

    @KuduColumnIgnore
    public String compextensionnum;


    @Override
    public Boolean filter() {
        if(addrtype != 1 && addrtype != 2){
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

        // l_customer_basic_labels
        Tuple2<Object, KuduEventSaver.WriteMode> tmp = this.get(this);
        if(addrtype == 1)
            result.add(tmp);
        else if(addrtype == 2){
            ScasLCustomerBasicLabelsBase001 suasLCustomerBasicLabels = new ScasLCustomerBasicLabelsBase001();
            Tuple2<Object, KuduEventSaver.WriteMode> tmp1 = suasLCustomerBasicLabels.get(this);
            if(((ScasLCustomerBasicLabelsBase001)tmp1.f0).filter()){
                result.add(tmp1);
            }
        }


        if(enableHistory && rawType.toLowerCase().equals("update")){
            ModifyHistory history = ModifyHistory.generate(log, rawType);
            result.add(new Tuple2<>(history, KuduEventSaver.WriteMode.INSERT));
        }

        return result;
    }
}
