package com.sinaif.stream.common.model.business.SinaifLoanProgressSrc;

import com.sinaif.stream.common.model.business.CommonBusiness;
import com.sinaif.stream.kudu.connector.KuduColumn;
import com.sinaif.stream.kudu.connector.KuduColumnIgnore;
import com.sinaif.stream.kudu.connector.KuduDomain;

import java.util.Date;

/**
 * @Time : 2019/8/27 14:12
 * @Author : pingping.tu
 * @File : SlpsBCustomerActionProductBase001.py
 * @Email : flatuer@gmail.com
 * @Description :
 */
@KuduDomain(table = "impala::stream_test.b_customer_action_product")
public class SlpsBCustomerActionProductBase001 extends CommonBusiness {

    public String action = "send";

    @KuduColumn(property = "op_time")
    public Date createtime;

    @KuduColumn(property = "terminal_code")
    public String productid;

    public Integer action_code = 3;

    public Long product_code_hash;

    public String product_code;

    @KuduColumnIgnore
    public Integer operationtype;


    @Override
    public Boolean filter() {
        if(operationtype != 9){
            return false;
        }
        return super.filter();
    }

    @Override
    public void prepare(Object src) {
        SlpsBCustomerActionProductBase slpsBCustomerActionProductBase = (SlpsBCustomerActionProductBase)src;
        this.log = slpsBCustomerActionProductBase.log;
        flag_del = slpsBCustomerActionProductBase.flag_del;

        customerId = slpsBCustomerActionProductBase.customerId;
        createtime = slpsBCustomerActionProductBase.createtime;
        productid = slpsBCustomerActionProductBase.productid;
        operationtype = slpsBCustomerActionProductBase.operationtype;
        product_code = slpsBCustomerActionProductBase.product_code;
        product_code_hash = slpsBCustomerActionProductBase.product_code_hash;
    }
}
