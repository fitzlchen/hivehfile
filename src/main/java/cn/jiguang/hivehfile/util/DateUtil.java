package cn.jiguang.hivehfile.util;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by fitz
 * Date: 2017/4/20
 */
public class DateUtil {
    private static final Logger logger = LogManager.getLogger(DateUtil.class);

    /**
     * 将日期从含有日期的字符串中提取出来，并转换为Unix Time
     * 如果输入字符串中不含有日期，则默认采用系统当前时间
     *
     * @param inputString 含有日期的字符串
     * @param dateFormat  日期格式
     * @param regex       提取日期的正则表达式
     * @return
     * @throws ParseException
     */
    public static Long convertStringToUnixTime(String inputString, String dateFormat, String regex) throws ParseException {
        SimpleDateFormat dateFormattor = new SimpleDateFormat(dateFormat);
        Matcher matcher = Pattern.compile(regex).matcher(inputString);
        String mStr = null;
        Long ts = dateFormattor.parse(dateFormattor.format(new Date())).getTime();
        if (matcher.find()) {
            mStr = matcher.group(1);
            ts = dateFormattor.parse(mStr).getTime();  // data_date=yyyyMMdd
        }
        return ts;
    }

    /**
     * 将日期从含有日期的字符串中提取出来，并根据下面的生成规则，生成尽量不重复的时间戳（重复率0.5%）
     * 小时制分钟+3位随机数+纳秒的末3位数
     * 如果输入字符串中不含有日期，则默认采用系统当前时间
     *
     * @param inputString 含有日期的字符串
     * @param dateFormat  日期格式
     * @param regex       提取日期的正则表达式
     * @return
     * @throws ParseException
     */
    public static Long generateUniqTimeStamp(String inputString, String dateFormat, String regex) throws ParseException {
        SimpleDateFormat dateFormattor = new SimpleDateFormat(dateFormat);
        Matcher matcher = Pattern.compile(regex).matcher(inputString);
        String mStr = null;
        Long ts = dateFormattor.parse(dateFormattor.format(new Date())).getTime();
        if (matcher.find()) {
            mStr = matcher.group(1);
            ts = dateFormattor.parse(mStr).getTime();  // data_date=yyyyMMdd
        }
        // 当前纳秒的末3位
        String nano = String.valueOf(System.nanoTime()).substring(12);
        // 生成3位随机数
        double rand = Math.random() * 1000;
        String randStr = null;
        if (rand >= 100 && rand < 1000) {
            randStr = String.valueOf(Math.floor(rand)).substring(0, 3);
        } else if ( rand >= 10 && rand <100 ){
            randStr = "0" + String.valueOf(Math.floor(rand)).substring(0,2);
        } else if (rand < 10 && rand > 0) {
            randStr = "00" + String.valueOf(Math.floor(rand)).substring(0, 1);
        } else {
            randStr = "000";
        }
        SimpleDateFormat minuteFormatter = new SimpleDateFormat("mm");
        String min = minuteFormatter.format(new Date());
        ts = ts + Long.parseLong(min + randStr + nano);
        return ts;
    }

    /**
     * 将yyyyMMdd格式的字符串转换为UnixTime
     *
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
