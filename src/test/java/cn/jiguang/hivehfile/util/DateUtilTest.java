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
        Long ts = DateUtil.generateUniqTimeStamp("data_date=20170718", "yyyyMMdd", "data_date=(\\d{8})");
    }
}
