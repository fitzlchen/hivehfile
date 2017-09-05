package cn.jiguang.hivehfile.util;

import cn.jiguang.hivehfile.Configuration;
import cn.jiguang.hivehfile.model.MappingInfo;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.io.SAXReader;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.*;
/**
 * Created by jiguang
 * Date: 2017/4/25
 */
public class XmlUtilTest {
    SAXReader saxReader = new SAXReader();
    Document document = null;
    Configuration configuration = null;
    @Before
    public void setup() throws DocumentException, IOException {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("user.defined.parameters","{'inPath':'hdfs://nameservice1/user/hive/warehouse/dmp.db/rt_jid_v2','outPath':'hdfs://nameservice1/tmp/user-profile/CID_JID','partition':'data_date=20170507,data_date=20170508,data_date=20170509,data_date=20170510','hive-column-name':'value','hive-column-type':'string'}' ");
        document = saxReader.read("mr-config.xml");
        configuration = XmlUtil.generateConfigurationFromXml(conf,"mr-config.xml");
    }

//    @Test
    public void testExractMapppingInfoList(){
        XmlUtil.extractMappingInfoList(document).get(1).getPartition();
        ArrayList<MappingInfo>  expected = new ArrayList<MappingInfo>();
        MappingInfo mappingInfo1 = new MappingInfo();
        MappingInfo mappingInfo2 = new MappingInfo();
        mappingInfo1.setPartition("feature=black_wifi_cnt/data_date=20170420,feature=black_wifi_cnt/data_date=20170421,feature=black_wifi_cnt/data_date=20170425");
        mappingInfo2.setPartition("feature=install_pkg_cnt/data_date=20170423,feature=install_pkg_cnt/data_date=20170424,feature=install_pkg_cnt/data_date=20170425");
        ArrayList<HashMap<String,String>> columnMappingList1 = new ArrayList<HashMap<String, String>>(),
                columnMappingList2 = new ArrayList<HashMap<String, String>>();
        HashMap<String,String>
                expectedMap1_1 = new HashMap<String, String>()
                ,expectedMap1_2 = new HashMap<String, String>()
                ,expectedMap2_1 = new HashMap<String, String>()
                ,expectedMap2_2 = new HashMap<String, String>();
        expectedMap1_1.put("hive-column-name","imei");
        expectedMap1_1.put("hive-column-type","string");
        expectedMap1_1.put("rowkey","true");
        expectedMap1_2.put("hive-column-name","value");
        expectedMap1_2.put("hive-column-type","string");
        expectedMap1_2.put("hbase-column-family","A");
        expectedMap1_2.put("hbase-column-qualifier","relationship_risk");
        columnMappingList1.add(expectedMap1_1);
        columnMappingList1.add(expectedMap1_2);
        mappingInfo1.setColumnMappingList(columnMappingList1);
        expectedMap2_1.put("hive-column-name","imei");
        expectedMap2_1.put("hive-column-type","string");
        expectedMap2_1.put("rowkey","true");
        expectedMap2_2.put("hive-column-name","value");
        expectedMap2_2.put("hive-column-type","string");
        expectedMap2_2.put("hbase-column-family","A");
        expectedMap2_2.put("hbase-column-qualifier","total_app_cnt");
        columnMappingList2.add(expectedMap2_1);
        columnMappingList2.add(expectedMap2_2);
        mappingInfo2.setColumnMappingList(columnMappingList2);
        expected.add(mappingInfo1);
        expected.add(mappingInfo2);
        assertEquals(expected,XmlUtil.extractMappingInfoList(document));
    }

//    @Test
    public void testExtractHtableName(){
//      assertEquals("fraud_feature_nor",XmlUtil.extractHtableName(document));
        String htableName = XmlUtil.extractHtableName(document);
        System.out.println(htableName.split(":")[0]+":"+htableName.split(":")[1]);
        assertEquals("tags:xhqb_tag",htableName.split(":")[0]+":"+htableName.split(":")[1]);
    }

    @Test
    public void testGetAllInputpath(){
        System.out.println(configuration.getOutputPath());
        System.out.println(configuration.getInputPath());
        assertEquals("hdfs://nameservice1/user/hive/warehouse/tmp.db/hfile_rt_career"
                ,configuration.getAllInputPath());
    }

//    @Test
    public void testExtractHbaseQuorem(){
        assertEquals("192.168.254.71,192.168.254.72,192.168.254.73",XmlUtil.extractHbaseQuorum(document));
    }

//    @Test
    public void testExtractRowkeyIndex(){
        // 获取当前 MappingInfo
        MappingInfo currentMappingInfo = XmlUtil.extractCurrentMappingInfo("hdfs://nameservice1/tmp/test-hfile-input/feature=install_pkg_cnt/data_date=20170425" ,configuration.getMappingInfoList());
        assertEquals(0,XmlUtil.extractRowkeyIndex(currentMappingInfo));
    }

//    @Test
    public void testExtractRowkeyName(){
        // 获取当前 MappingInfo
        MappingInfo currentMappingInfo = XmlUtil.extractCurrentMappingInfo("hdfs://nameservice1/tmp/test-hfile-input/feature=install_pkg_cnt/data_date=20170425" ,configuration.getMappingInfoList());
        assertEquals("imei",XmlUtil.extractRowKeyColumnName(currentMappingInfo));
    }

//    @Test
    public void testExtractDelimiterCollection(){
        HashMap<String,String> expected = new HashMap<String, String>();
        expected.put("field-delimiter","0001");
        expected.put("collection-item-delimiter",",");
        HashMap<String,String> actual = XmlUtil.extractDelimiterCollection(document);
        assertEquals("0001",actual.get("field-delimiter"));
    }

    //    @Test
    public void testRegex(){
        String str = "${adsads},${sadsadas}";
        Matcher matcher = Pattern.compile("\\$\\{.+?\\}").matcher(str);
        while(matcher.find())
            System.out.println(matcher.group());
    }

//    @Test
    public void testDom4jConvert() throws DocumentException {
        String str = "<root><element>1</element></root>";
        Document doc = DocumentHelper.parseText(str);
        System.out.println(doc.getRootElement().elementText("element"));
    }

//    @Test
    public void testVariableReplacement() throws DocumentException {
        SAXReader reader = new SAXReader();
        Document actual = reader.read(XmlUtilTest.class.getResourceAsStream("/test-config.xml"));
        actual = XmlUtil.variableReplacement(actual,"{'inPath':'hdfs://nameservice1/user/hive/warehouse/dmp.db/rt_jid_v2','outPath':'hdfs://nameservice1/tmp/user-profile/CID_JID','partition':'data_date=20170507,data_date=20170508,data_date=20170509,data_date=20170510','hive-column-name':'value','hive-column-type':'string'}'");
        assertEquals(document, actual);
    }

}
