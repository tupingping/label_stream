package com.sinaif.stream.common.model.business.SinaifUserAccountSrc;

import com.sinaif.stream.common.model.business.CommonBusiness;
import com.sinaif.stream.common.utils.HashUtil;
import com.sinaif.stream.kudu.connector.KuduColumn;
import com.sinaif.stream.kudu.connector.KuduDomain;

import java.util.Date;

/**
 * @Time : 2019/8/23 18:02
 * @Author : pingping.tu
 * @File : SuasBCustomerActionProduct.py
 * @Email : flatuer@gmail.com
 * @Description :
 */
@KuduDomain(table = "impala::stream_test.b_customer_action_product")
public class SuasBCustomerActionProduct extends CommonBusiness {

    public String action = "register";

    public Integer action_code = 1;

    public Long product_code_hash;

    public String product_code = "1";

    @KuduColumn(property = "op_time")
    public Date registtime;

    @KuduColumn(property = "terminal_code")
    public String productid;

    @Override
    public Boolean filter() {
        if(!(productid.equals("1005") || productid.equals("2001") || productid.equals("2005"))){
            return false;
        }

        return super.filter();
    }

    @Override
    public void prepare(Object src) {
        SuasBCustomerInfoBase suasBCustomerInfoBase = (SuasBCustomerInfoBase)src;
        this.log = suasBCustomerInfoBase.log;

        flag_del = suasBCustomerInfoBase.flag_del;
        customerId = suasBCustomerInfoBase.customerId;
        product_code_hash = Long.valueOf(HashUtil.hash(product_code));
        productid = suasBCustomerInfoBase.productid;
        registtime = suasBCustomerInfoBase.registtime;

    }
}
