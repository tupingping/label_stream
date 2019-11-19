package com.sinaif.stream.common.model.business.SinaifEasyDeviceSyncSrc;

import com.sinaif.stream.common.model.business.CommonBusiness;
import com.sinaif.stream.common.utils.HashUtil;
import com.sinaif.stream.kudu.connector.KuduColumn;
import com.sinaif.stream.kudu.connector.KuduColumnIgnore;
import com.sinaif.stream.kudu.connector.KuduDomain;

import java.util.Date;

/**
 * @Time : 2019/8/23 18:03
 * @Author : pingping.tu
 * @File : SedssBCustomerActionProduct.py
 * @Email : flatuer@gmail.com
 * @Description :
 */
@KuduDomain(table = "impala::stream_test.b_customer_action_product")
public class SedssBCustomerActionProduct extends CommonBusiness {

    @KuduColumnIgnore
    public String phone;

    public String action = "register";

    public Integer action_code = 1;

    public Long product_code_hash;

    public String product_code = "1";

    @KuduColumn(property = "op_time")
    public Date createtime;

    public String terminal_code = "1003";


    @Override
    public Boolean filter() {
        if(phone == null){
            return false;
        }
        return super.filter();
    }

    @Override
    public void prepare(Object src) {
        SedssBCustomerInfoBase sedssBCustomerInfoBase = (SedssBCustomerInfoBase)src;
        this.log = sedssBCustomerInfoBase.log;
        flag_del = sedssBCustomerInfoBase.flag_del;
        customerId = sedssBCustomerInfoBase.customerId;
        product_code_hash = Long.valueOf(HashUtil.hash(product_code));
        phone = sedssBCustomerInfoBase.phone;
        createtime = sedssBCustomerInfoBase.createtime;
    }
}
