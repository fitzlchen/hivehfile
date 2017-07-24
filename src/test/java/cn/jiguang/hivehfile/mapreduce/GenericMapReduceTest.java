package cn.jiguang.hivehfile.mapreduce;

import cn.jiguang.hivehfile.model.MappingInfo;
import cn.jiguang.hivehfile.util.DateUtil;
import cn.jiguang.hivehfile.util.PrintUtil;
import cn.jiguang.hivehfile.util.XmlUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.io.SAXReader;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by fitz on 2017/5/7.
 */
public class GenericMapReduceTest {
    MapDriver<LongWritable, Text, ImmutableBytesWritable, Text> mapDriver;

    @Before
    public void setup() {
        mapDriver = MapDriver.newMapDriver(new GenericMapper());
    }


    //        @Test
    public void testReadSomeConfigFromHdfs() {
        HashMap<String, String> expected = new HashMap<String, String>();
        expected.put("input-path", "hdfs://nameservice1/user/hive/warehouse/dmp.db/rt_jid_v2");
        expected.put("output-path", "hdfs://nameservice1/tmp/user-profile/CID_JID");
        expected.put("htable-name", "bt_iaudience");

        String inStr = "    <input-path>hdfs://nameservice1/user/hive/warehouse/dmp.db/rt_jid_v2<</input-path>\n" +
                "    <output-path>hdfs://nameservice1/tmp/user-profile/CID_JID</output-path>\n" +
                "    <htable-name>bt_iaudience</htable-name>";
        HashMap<String, String> actual = new HashMap<String, String>();
        Matcher inputPathMatcher = Pattern.compile("<input-path>(.+)</input-path>").matcher(inStr);
        if (inputPathMatcher.find())
            actual.put("input-path", inputPathMatcher.group(1));
        Matcher outputPathMatcher = Pattern.compile("<output-path>(.+)</output-path>").matcher(inStr);
        if (outputPathMatcher.find())
            actual.put("output-path", outputPathMatcher.group(1));
        Matcher htableNameMatcher = Pattern.compile("<htable-name>(.+)</htable-name>").matcher(inStr);
        if (htableNameMatcher.find())
            actual.put("htable-name", htableNameMatcher.group(1));
        assertEquals(expected, actual);
    }

    @Test
    public void testGenericMapReduce() throws ParseException, IOException {
        HashMap<String, String> expected = new HashMap<String, String>();
        expected.put("rowKey", "0000");
        expected.put("columnFamily", "A");
        expected.put("column", "CID_JID");
        expected.put("ts", "1494518400000");
        expected.put("value", "lion");
        mapDriver.withInput(new LongWritable(0), new Text("0000\u0001lion"))
                .withOutput(new ImmutableBytesWritable(Bytes.toBytes("0000")), new Text(expected.toString()))
                .runTest();
    }

    //    @Test
    public void testUriParser() throws URISyntaxException {
        URI uri = new URI("hdfs://nameservice1/tmp/test-hfile-config/hiveConfig.txt");
        System.out.println(new Path(uri.getPath()).getName().toString());
    }
}

class GenericMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Text> {
    private cn.jiguang.hivehfile.Configuration selfDefinedConfig = null;

    @Override
    public void setup(Context context) throws IOException {
        selfDefinedConfig = XmlUtil.generateConfigurationFromXml(context.getConfiguration(), "mr-config.xml");
    }

    @Override
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String inputString = "861845033162435\\N0.02\\N20170718\\NFIN_Payment_General";
        // 获取数据文件的路径
        String dataFilePath = "hdfs://nameservice1/user/hive/warehouse/anti_fraud.db/jietiao_factor_data_r/";
        String[] values = inputString.split(selfDefinedConfig.getDelimiterCollection().get("field-delimiter"));
        // 获取当前 MappingInfo
        MappingInfo currentMappingInfo = XmlUtil.extractCurrentMappingInfo(dataFilePath ,selfDefinedConfig.getMappingInfoList());
        // 在每一行数据中，rowkey 和 timestamp 都固定不变
        ImmutableBytesWritable rowkey = new ImmutableBytesWritable(Bytes.toBytes(values[XmlUtil.extractRowkeyIndex(currentMappingInfo)]));
        Long ts = 0L;
        /*
         * 解析数据文件路径，获取数据日期 data_date
         * 当数据文件路径中不含有 data_date 时，默认使用当前时间
         */
        try {
            ts = DateUtil.convertStringToUnixTime(dataFilePath,"yyyyMMdd","data_date=(\\d{8})");
        } catch (ParseException e) {
            System.exit(-1);    // 异常直接退出
        }
            /* 开始装配HFile
             * 所需参数：
             * RowKey
             * ColumnFamily
             * ColumnQualifier
             * TimeStamp
             * Value
             */
        for (int i = 0; i < values.length; i++) {
            HashMap<String,String> kv = null;
            if (i != XmlUtil.extractRowkeyIndex(currentMappingInfo)
                    && currentMappingInfo.getColumnMappingList().get(i).get("hbase-column-family") != null
                    && currentMappingInfo.getColumnMappingList().get(i).get("hbase-column-qualifier") != null
                    ) {  // 只遍历非 Rowkey 且 需要写入 HBase 的字段
                kv = new HashMap<String, String>();
                kv.put("rowKey",values[XmlUtil.extractRowkeyIndex(currentMappingInfo)]);
                kv.put("columnFamily",currentMappingInfo.getColumnMappingList().get(i).get("hbase-column-family"));
                kv.put("column",currentMappingInfo.getColumnMappingList().get(i).get("hbase-column-qualifier"));
                kv.put("ts",String.valueOf(ts));
                kv.put("value",PrintUtil.escapeConnotation(values[i]));
                System.out.println(kv);
            }
            if (kv != null) context.write(rowkey, new Text(kv.toString()));
        }
    }
}

