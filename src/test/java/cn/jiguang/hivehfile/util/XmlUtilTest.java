package cn.jiguang.hivehfile.util;

import avro.shaded.com.google.common.collect.ImmutableMap;
import avro.shaded.com.google.common.collect.Maps;
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
 * Created by fitz
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
        ArrayList<MappingInfo> arr = XmlUtil.extractMappingInfoList(document);
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

    @Test
    public void testExtractCurrentMappingInfo(){
        String dataFilePath = "hdfs://nameservice1/tmp/chenlin/dmp/rt_career/data_date=20171211/type=A";
        ArrayList<MappingInfo> mappingInfos = XmlUtil.extractMappingInfoList(document);
        MappingInfo currentMappingInfo = XmlUtil.extractCurrentMappingInfo(dataFilePath, mappingInfos);
    }

//    @Test
    public void testExtractHtableName(){
//      assertEquals("fraud_feature_nor",XmlUtil.extractHtableName(document));
        String htableName = XmlUtil.extractHtableName(document);
        System.out.println(htableName.split(":")[0]+":"+htableName.split(":")[1]);
        assertEquals("tags:xhqb_tag",htableName.split(":")[0]+":"+htableName.split(":")[1]);
    }

//    @Test
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
        expected.put("field-delimiter","\u0001");
        expected.put("collection-item-delimiter",",");
        HashMap<String,String> actual = XmlUtil.extractDelimiterCollection(document);
        assertEquals(expected.get("field-delimiter"),actual.get("field-delimiter"));
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

    @Test
    public void testVariableReplacement() throws DocumentException {
        SAXReader reader = new SAXReader();
        Document doc = reader.read(XmlUtilTest.class.getResourceAsStream("/test-config.xml"));
        doc = XmlUtil.variableReplacement(doc,"{'HFILE_POS':'hdfs://nameservice1/tmp/user-profile/hfile/chenyh2/tags__shuce_tag/A/hbase_col', 'partition':'data_date=20180503'}");
        String actual = doc.asXML();
        String expect = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<config>\n" +
                "    <!-- Global Settings -->\n" +
                "    <input-path>hdfs://nameservice1/user/hive/warehouse/tmp.db/chenyh2_shuce_custom_hbase_temp_auto</input-path>\n" +
                "    <output-path>hdfs://nameservice1/tmp/user-profile/hfile/chenyh2/tags__shuce_tag/A/hbase_col</output-path>\n" +
                "    <htable-name>tags:shuce_tag</htable-name>\n" +
                "    <field-delimiter>\\u0001</field-delimiter>\n" +
                "    <collection-item-delimiter>,</collection-item-delimiter>\n" +
                "    <hbase.zookeeper.quorum>192.168.254.86,192.168.254.96,192.168.254.107</hbase.zookeeper.quorum>\n" +
                "    <hbase.zookeeper.property.clientPort>2181</hbase.zookeeper.property.clientPort>\n" +
                "    <hbase.zookeeper.property.maxClientCnxns>400</hbase.zookeeper.property.maxClientCnxns>\n" +
                "    <hbase.znode.parent>/hbase</hbase.znode.parent>\n" +
                "\n" +
                "    <!-- Local Settings -->\n" +
                "    <mapping-info>\n" +
                "        <partition>\n" +
                "            data_date=20180503\n" +
                "        </partition>\n" +
                "        <column-mapping>\n" +
                "            <hive-column-name>imei</hive-column-name>\n" +
                "            <hive-column-type>string</hive-column-type>\n" +
                "            <rowkey>true</rowkey>\n" +
                "        </column-mapping>\n" +
                "\n" +
                "        <column-mapping>\n" +
                "            <hive-column-name>info</hive-column-name>\n" +
                "            <hive-column-type>string</hive-column-type>\n" +
                "            <hbase-column-family>A</hbase-column-family>\n" +
                "            <hbase-column-qualifier>#hbase_col#</hbase-column-qualifier>\n" +
                "        </column-mapping>\n" +
                "\n" +
                "        <column-mapping>\n" +
                "            <hive-column-name>hbase_col</hive-column-name>\n" +
                "            <hive-column-type>string</hive-column-type>\n" +
                "        </column-mapping>\n" +
                "\n" +
                "    </mapping-info>\n" +
                "</config>";
        assertEquals(expect, actual);
    }

    @Test
    public void testExtractDynamicColumn() throws IOException {
        ImmutableMap<String,Integer> expected = ImmutableMap.of("feature",2,"family",3);
        MappingInfo mappingInfo = configuration.getMappingInfoList().get(0);
        assertEquals(expected,mappingInfo.getDynamicFillColumnRela());
    }

}
