package com.sinaif.stream.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * @Time : 2019/8/29 17:43
 * @Author : pingping.tu
 * @File : BankUtil.py
 * @Email : flatuer@gmail.com
 * @Description :
 */

public class BankUtil {
    private static Logger LOGGER = LoggerFactory.getLogger(BankUtil.class);
    private static Properties BANK_MAP = new Properties();

    static {
        try{
            InputStream in = BankUtil.class.getResourceAsStream("/bankMap.properties");
            BufferedReader bf = new BufferedReader(new InputStreamReader(in));
            BANK_MAP.load(bf);
        }catch (Exception e){
            LOGGER.error(e.toString());
        }
    }

    public static String getBank(String bankcode){
        String bank = bankcode;
        if(bank.matches("[0-9]{1,}")){
            if(BANK_MAP.containsKey(bank)){
                bank = BANK_MAP.getProperty(bankcode);
            }
        }

        return bank;
    }


}
