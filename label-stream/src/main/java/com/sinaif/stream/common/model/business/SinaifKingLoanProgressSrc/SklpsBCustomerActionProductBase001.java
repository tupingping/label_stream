package com.sinaif.stream.common.model.business.SinaifKingLoanProgressSrc;

import com.sinaif.stream.common.model.business.CommonBusiness;
import com.sinaif.stream.kudu.connector.KuduColumn;
import com.sinaif.stream.kudu.connector.KuduDomain;

import java.util.Date;

/**
 * @Time : 2019/8/27 14:38
 * @Author : pingping.tu
 * @File : SklpsBCustomerActionProductBase001.py
 * @Email : flatuer@gmail.com
 * @Description :
 */
@KuduDomain(table = "impala::stream_test.b_customer_action_product")
public class SklpsBCustomerActionProductBase001 extends CommonBusiness {

    public String action = "send";

    @KuduColumn(property = "op_time")
    public Date createtime;

    @KuduColumn(property = "terminal_code")
    public String terminalid;

    public Integer action_code = 3;

    public Long product_code_hash;

    @KuduColumn(property = "product_code")
    public String productid;

    @Override
    public Boolean filter() {
        return super.filter();
    }

    @Override
    public void prepare(Object src) {
        SklpsBCustomerActionProductBase sklpsBCustomerActionProductBase = (SklpsBCustomerActionProductBase)src;
        this.log = sklpsBCustomerActionProductBase.log;
        flag_del = sklpsBCustomerActionProductBase.flag_del;
        customerId = sklpsBCustomerActionProductBase.customerId;
        createtime = sklpsBCustomerActionProductBase.createtime;
        terminalid = sklpsBCustomerActionProductBase.terminalid;
        productid = sklpsBCustomerActionProductBase.productid;
        product_code_hash = sklpsBCustomerActionProductBase.product_code_hash;
    }
}
