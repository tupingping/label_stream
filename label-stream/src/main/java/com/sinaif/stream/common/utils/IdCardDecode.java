package com.sinaif.stream.common.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * @Time : 2019/8/20 21:01
 * @Author : pingping.tu
 * @File : IdCardDecode.py
 * @Email : flatuer@gmail.com
 * @Description :
 */

public class IdCardDecode {

    private static Logger LOGGER = LoggerFactory.getLogger(IdCardDecode.class);

    public static String getIdCard(String ciphertext, String productid){
        String rs = ciphertext;
        if(productid.equals("2001") || productid.equals("2005"))
        {
            try{
                List<NameValuePair> d = new ArrayList<>();
                NameValuePair keyvalue = new BasicNameValuePair("params", "{\"idCardNum\":\"" + ciphertext + "\"}");
                d.add(keyvalue);
                String response = HttpUtil.doPostString(ConfigRespostory.value("idcard.decode.url"), d);
                JSONObject data = JSON.parseObject(response);
                if(data.getString("msg").equals("ok")){
                    rs = data.getString("data");
                }
            }catch (Exception e){
                LOGGER.error(e.toString());
            }
        }

        return rs;
    }

    public static Integer getBirthday(String idcard){
        String year = idcard.substring(6,10);
        String month = idcard.substring(10,12);
        String day = idcard.substring(12,14);
        return Integer.valueOf(year + month + day);
    }

    public static Short getSex(String idcard){
        Short d = Short.valueOf(idcard.substring(16,17));
        if(d%2 == 0){
            return 2;
        }else{
            return 1;
        }

    }

    public static Integer getAge(String idcard){
        Integer y = Integer.valueOf(idcard.substring(6,10));
        Integer m = Integer.valueOf(idcard.substring(10,12));
        Integer d = Integer.valueOf(idcard.substring(12,14));

        Calendar now = Calendar.getInstance();
        Integer year = now.get(Calendar.YEAR);
        Integer month = now.get(Calendar.MONTH) + 1;
        Integer day = now.get(Calendar.DATE);

        Integer yearM = year - y;
        Integer monthM = month - m;
        Integer dayM = day - d;
        Integer age = yearM;
        if(yearM == 0){
            age = 0;
        }else {
            if(monthM < 0){
                age = age - 1;
            }else if(monthM == 0){
                if(dayM < 0){
                    age = age - 1;
                }
            }
        }

        return age;
    }

    public static String getChineseZodiac(String idcard){
        String[] tmp = {"猴","鸡","狗","猪","鼠","牛","虎","兔","龙","蛇","马","羊"};
        return tmp[Integer.valueOf(idcard.substring(6,10))%12];
    }

    public static String getZodiac(String idcard){
        Integer[] dayArr = {20, 19, 21, 20, 21, 22, 23, 23, 23, 24, 23, 22};
        String[] tmp = {"摩羯座", "水瓶座", "双鱼座", "白羊座", "金牛座", "双子座",
                        "巨蟹座", "狮子座", "处女座", "天秤座", "天蝎座", "射手座"};

        Integer day = Integer.valueOf(idcard.substring(12,14));
        Integer month = Integer.valueOf(idcard.substring(10,12));

        return day < dayArr[month-1] ? tmp[month-1] : tmp[month];

    }

    public static String getAreaCode(String idcard){
        return idcard.substring(0,6);
    }

    public static void main(String[] args) {
        String id = "360121199008253970";
        System.out.println(IdCardDecode.getChineseZodiac(id));
        System.out.println(IdCardDecode.getAreaCode(id));
        System.out.println(IdCardDecode.getAge(id));
    }

}


