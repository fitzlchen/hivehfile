package cn.jiguang.hivehfile.util;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import static org.junit.Assert.*;
/**
 * Created by jiguang
 * Date: 2017/4/25
 */
public class XmlUtilTest {
    SAXReader reader = null;
    Document document = null;

    @Before
    public void setup() throws DocumentException {
        File config = new File("mr-config.xml");
        reader = new SAXReader();
        document = reader.read(config);
    }

//    @Test
    public void testExractHiveStruct(){
        ArrayList<HashMap<String,String>>  expectedArrayList = new ArrayList<HashMap<String, String>>();
        HashMap<String,String>
                expectedMap1 = new HashMap<String, String>()
                ,expectedMap2 = new HashMap<String, String>();
        expectedMap1.put("hive-column-name","imei");
        expectedMap1.put("hive-column-type","string");
        expectedMap1.put("rowkey","true");
        expectedMap2.put("hive-column-name","value");
        expectedMap2.put("hive-column-type","string");
        expectedMap2.put("hbase-column-family","A");
        expectedMap2.put("hbase-column-qualifier","anti-fraud");
        expectedArrayList.add(expectedMap1);
        expectedArrayList.add(expectedMap2);

        ArrayList<HashMap<String,String>> actualArrayList = XmlUtil.extractMappingInfo(document);
        assertEquals(expectedArrayList,actualArrayList);
    }

//    @Test
    public void testExtractHtableName(){
      assertEquals("fosun_tag_bak",XmlUtil.extractHtableName(document));
    }

//    @Test
    public void testExtractDate() throws ParseException {
        Long expected = 1493049600000L;
        assertEquals(expected,DateUtil.convertDateToUnixTime(XmlUtil.extractDate(document)));
    }

//    @Test
    public void testExtractInputpath(){
        assertEquals("hdfs://nameservice1/user/hive/warehouse/fosunapp.db/fosun_fonova_active/data_date=20170425",XmlUtil.extractInputPath(document));
    }

//    @Test
    public void testExtractHbaseColumnFamily(){
        assertEquals("A",XmlUtil.extractHbaseColumnFamily(document));
    }

//    @Test
    public void testSaxParser(){
        Document doc = DocumentHelper.createDocument();
        doc.add(DocumentHelper.createElement("config"));
        if(doc.getRootElement().element("aa")==null)
            System.out.println(true);
    }

//    @Test
    public void testExtractHbaseQuorem(){
        assertEquals("192.168.254.71,192.168.254.72,192.168.254.73",XmlUtil.extractHbaseQuorum(document));
    }

    @Test
    public void testExtractRowkeyIndex(){
        assertEquals(0,XmlUtil.extractRowkeyIndex(document));
    }

//    @Test
    public void testExtractDelimiterCollection(){
        HashMap<String,String> expected = new HashMap<String, String>();
        expected.put("field-delimiter","\u0001");
        expected.put("collection-item-delimiter",",");
        HashMap<String,String> actual = XmlUtil.extractDelimiterCollection(document);
        assertEquals("\u0001",actual.get("field-delimiter"));
    }

//    @Test
    public void testExtractMappingInfo(){
        HashMap<String,String> expected = new HashMap<String, String>();

        assertEquals(expected,XmlUtil.extractMappingInfo(document));
    }
}
