package cn.jiguang.hivehfile.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jiguang
 * Date: 2017/4/20
 */
public class DateUtil {

    /**
     * 将日期从含有日期的字符串中提取出来，并转换为Unix Time
     * @param inputString 含有日期的字符串
     * @param dateFormat    日期格式
     * @param regex     提取日期的正则表达式
     * @return
     * @throws ParseException
     */
    public static Long convertStringToUnixTime(String inputString, String dateFormat, String regex) throws ParseException {
        SimpleDateFormat dateFormattor = new SimpleDateFormat(dateFormat);
        Matcher matcher = Pattern.compile(regex).matcher(inputString);
        String mStr = null;
        if(matcher.find()) {
            mStr = matcher.group(1);
        }
        Long ts = dateFormattor.parse(mStr).getTime();  // data_date=yyyyMMdd
        return ts;
    }

    /**
     * 将yyyyMMdd格式的字符串转换为UnixTime
     * @param date
     * @return
     * @throws ParseException
     */
    public static Long convertDateToUnixTime(String date) throws ParseException {
        SimpleDateFormat dateFormattor = new SimpleDateFormat("yyyyMMdd");
        Long ts = dateFormattor.parse(date).getTime();  // data_date=yyyyMMdd
        return ts;
    }
}
