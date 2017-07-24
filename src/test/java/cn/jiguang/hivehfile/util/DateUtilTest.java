package cn.jiguang.hivehfile.util;

import org.junit.Test;

import java.text.ParseException;

/**
 * Created by: fitz
 * <p>
 * Date: 2017/7/19
 * <p>
 * Description:
 */
public class DateUtilTest {
    @Test
    public void testGenerateUniqTimeStamp() throws ParseException {
        for (Long i=0L; i< 100000; i++){
            Long ts = DateUtil.generateUniqTimeStamp("hdfs://nameservice1/user/hive/warehouse/anti_fraud.db/expand_blacklist_output_r/data_date=20170430/list=expand", "yyyyMMdd", "data_date=(\\d{8})");
            if (ts < 1493481600000L || ts >= 1493568000000L){
                System.out.println(ts);
                break;
            }
        }
    }
}
