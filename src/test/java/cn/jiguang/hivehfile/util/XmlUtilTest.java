package cn.jiguang.hivehfile.util;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.io.SAXReader;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
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
        ArrayList<Map<String,String>>  expectedArrayList = new ArrayList<Map<String, String>>();
        HashMap<String,String>
                expectedMap1 = new HashMap<String, String>()
                ,expectedMap2 = new HashMap<String, String>();
        expectedMap1.put("name","imei");
        expectedMap1.put("type","string");
        expectedMap2.put("name","value");
        expectedMap2.put("type","string");
        expectedArrayList.add(expectedMap1);
        expectedArrayList.add(expectedMap2);

        ArrayList<Map<String,String>> actualArrayList = XmlUtil.extractHiveStruct(document);
        assertEquals(expectedArrayList,actualArrayList);
    }

//    @Test
    public void testExtractHtableName(){
      assertEquals("fosun_tag_bak",XmlUtil.extractHtableName(document));
    }

//    @Test
    public void testExtractDate(){
        assertEquals("20170425",XmlUtil.extractDate(document));
    }

//    @Test
    public void testExtractInputpath(){
        assertEquals("hdfs://nameservice1/user/hive/warehouse/fosunapp.db/fosun_fonova_active/data_date=20170425",XmlUtil.extractInputPath(document));
    }

    @Test
    public void testExtractHbaseColumnFamily(){
        assertEquals("A",XmlUtil.extractHbaseColumnFamily(document));
    }
}
