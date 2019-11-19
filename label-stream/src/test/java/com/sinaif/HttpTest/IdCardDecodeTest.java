package com.sinaif.HttpTest;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sinaif.stream.common.utils.ConfigRespostory;
import com.sinaif.stream.common.utils.DateUtil;
import com.sinaif.stream.common.utils.HttpUtil;
import com.sinaif.stream.common.utils.IdCardDecode;
import com.sinaif.stream.kudu.connector.KuduEventSaver;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @Time : 2019/8/20 19:46
 * @Author : pingping.tu
 * @File : IdCardDecodeTest.py
 * @Email : flatuer@gmail.com
 * @Description :
 */

public class IdCardDecodeTest {

    private static final Logger LOG = LoggerFactory.getLogger(IdCardDecodeTest.class);

    public static void main(String[] args) {


        LOG.info("test");
        LOG.info("test1");

//        try{
//            SimpleDateFormat format =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//
//
//            String idcard = "UM4xE3av84iH/jOmgWwQNsitJkiEu3fE";
//            List<NameValuePair> d = new ArrayList<>();
//            NameValuePair keyvalue = new BasicNameValuePair("params", "{\"idCardNum\":\"" + idcard + "\"}");
//            d.add(keyvalue);
//
//            String response = HttpUtil.doPostString(ConfigRespostory.value("idcard.decode.url"), d);
//
//            JSONObject data = JSON.parseObject(response);
//
//            System.out.println(response);
//        }catch (Exception e){
//            e.printStackTrace();
//        }

    }
}
