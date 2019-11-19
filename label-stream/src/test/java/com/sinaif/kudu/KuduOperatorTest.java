package com.sinaif.kudu;

import com.sinaif.stream.common.utils.ConfigRespostory;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


/**
 * @File : com.sinaif.kudu.KuduOperatorTest.java
 * @Author: tupingping
 * @Date : 2019/8/8
 * @Desc :
 */
public class KuduOperatorTest {
    private static final Logger LOG = LoggerFactory.getLogger(KuduOperatorTest.class);

    public static void main(String[] payload) {
        KuduOperator kuduOperator = new KuduOperator();
//        kuduOperator.CreateTable();
//        kuduOperator.CreateTable1();
//        kuduOperator.CreateTable2();
//        kuduOperator.CreateTable3();
//        kuduOperator.CreateTable4();
//        kuduOperator.CreateTable5();
        kuduOperator.ScanRow();
        kuduOperator.ScanRow1();
        kuduOperator.ScanRow2();
        kuduOperator.ScanRow3();
        kuduOperator.ScanRow4();
        kuduOperator.ScanRow5();
    }

}
