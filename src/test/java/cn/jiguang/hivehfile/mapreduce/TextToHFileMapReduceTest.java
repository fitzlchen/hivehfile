package cn.jiguang.hivehfile.mapreduce;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import static  cn.jiguang.hivehfile.mapreduce.TextToHFileMapReduce.*;
/**
 * Created by jiguang
 * Date: 2017/4/20
 */
public class TextToHFileMapReduceTest {
    MapDriver<LongWritable,Text,ImmutableBytesWritable,KeyValue> mapDriver;
    @Before
    public void setup(){
        HFileMapper mapper = new HFileMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

//    @Test
    public void testArrayPrint(){
        ArrayList<String> list = new ArrayList<String>();
        list.add("jjs");
        list.add("uuii");
        list.add("282");
        Assert.assertEquals("jjs,uuii,282",list.toString().substring(1,list.toString().length()-1).replace(" ",""));
    }

//    @Test
    public void testDateExtract() throws ParseException {
        String inputString="/user/hive/warehouse/fosunapp.db/fosun_fonova/data_date=20170310/001070_0";
        SimpleDateFormat dateFormattor = new SimpleDateFormat("yyyyMMdd");
        Matcher matcher = Pattern.compile("data_date=(\\d{8})").matcher(inputString);
        String mStr = null;
        if(matcher.find()) {
            System.out.println(matcher.group(1));
            mStr = matcher.group(1);
        }
        Long ts = dateFormattor.parse(mStr).getTime();  // data_date=yyyyMMdd
        System.out.println(ts.toString());
    }

//    @Test
    public void testMapper(){
        ImmutableBytesWritable expectedRowKey = new ImmutableBytesWritable(Bytes.toBytes("28e46edd676870dd"));
        KeyValue expectedKv = new KeyValue(Bytes.toBytes("28e46edd676870dd"),Bytes.toBytes("A"),Bytes.toBytes("重庆市"));
    }

}
