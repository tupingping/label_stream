package com.sinaif.stream.common.model.business.SinaifUserAccountSrc;

import com.sinaif.stream.common.model.business.CommonBusiness;
import com.sinaif.stream.kudu.connector.KuduColumn;
import com.sinaif.stream.kudu.connector.KuduDomain;

import java.util.Date;

/**
 * @Time : 2019/8/29 17:14
 * @Author : pingping.tu
 * @File : SuasBCustomerProductRegiste.py
 * @Email : flatuer@gmail.com
 * @Description :
 */
@KuduDomain(table = "impala::stream_test.b_customer_product_registe")
public class SuasBCustomerProductRegiste extends CommonBusiness {

    @KuduColumn(property = "terminal_code")
    public String productid;

    @KuduColumn(property = "terminal_customer_id")
    public String id;

    @KuduColumn(property = "register_dt")
    public Date registtime;

    @Override
    public Boolean filter() {
        if(!(productid.equals("1005")||productid.equals("2001")||productid.equals("2005"))){
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
        productid = suasBCustomerInfoBase.productid;
        id = suasBCustomerInfoBase.id;
        registtime = suasBCustomerInfoBase.registtime;
    }
}
