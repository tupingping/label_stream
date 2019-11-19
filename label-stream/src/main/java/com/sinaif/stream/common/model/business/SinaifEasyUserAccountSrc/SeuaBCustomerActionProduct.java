package com.sinaif.stream.common.model.business.SinaifEasyUserAccountSrc;

import com.sinaif.stream.common.model.business.CommonBusiness;
import com.sinaif.stream.common.utils.HashUtil;
import com.sinaif.stream.kudu.connector.KuduColumn;
import com.sinaif.stream.kudu.connector.KuduColumnIgnore;
import com.sinaif.stream.kudu.connector.KuduDomain;

import java.util.Date;

/**
 * @Time : 2019/8/23 18:02
 * @Author : pingping.tu
 * @File : SeuaBCustomerActionProduct.py
 * @Email : flatuer@gmail.com
 * @Description :
 */
@KuduDomain(table = "impala::stream_test.b_customer_action_product")
public class SeuaBCustomerActionProduct extends CommonBusiness {

    @KuduColumnIgnore
    public String username;

    public String action = "register";

    public Integer action_code = 1;

    public Long product_code_hash;

    public String product_code = "1";

    @KuduColumn(property = "op_time")
    public Date registtime;

    public String terminal_code = "1003";

    @Override
    public Boolean filter() {
        if(username == null){
            return false;
        }
        return super.filter();
    }

    @Override
    public void prepare(Object src) {
        SeuasBCustomerInfoBase seuasBCustomerInfoBase = (SeuasBCustomerInfoBase)src;
        this.log = seuasBCustomerInfoBase.log;

        flag_del = seuasBCustomerInfoBase.flag_del;
        customerId = seuasBCustomerInfoBase.customerId;
        product_code_hash = Long.valueOf(HashUtil.hash(product_code));
        username = seuasBCustomerInfoBase.username;
        registtime = seuasBCustomerInfoBase.registtime;

    }
}
