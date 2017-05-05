package cn.jiguang.hivehfile.util;

import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.Node;

import javax.print.Doc;
import java.util.*;

/**
 * Created by jiguang
 * Date: 2017/4/25
 */
public class XmlUtil {
    /**
     * 根据DOM，生成Hive表的字段数组（字段名、字段类型）
     * @param document
     * @return
     */
    public static ArrayList<Map<String,String>> extractHiveStruct(Document document){
        ArrayList<Map<String,String>> result = new ArrayList<Map<String, String>>();
        Iterator<Element> iterator = document.getRootElement().element("hive-struct").elementIterator();
        while (iterator.hasNext()){
            Element e = iterator.next();
            HashMap<String,String> columnMap = new HashMap<String, String>();
            columnMap.put("name",e.elementText("column-name"));
            columnMap.put("type",e.elementText("column-type"));
            result.add(columnMap);
        }
        return result;
    }

    /**
     * 解析DOM，获取htable名称
     * @param document
     * @return
     */
    public static String extractHtableName(Document document){
       return  document.getRootElement().elementText("htable-name");
    }

    /**
     * 解析DOM，获取HBase字段的Date信息，方便后续Date => Unix Time
     * @param document
     * @return
     */
    public static String extractDate(Document document){
        return document.getRootElement().elementText("date");
    }

    /**
     * 解析DOM，获取MR输入路径
     * @param document
     * @return
     */
    public static String extractInputPath(Document document){
        return document.getRootElement().elementText("input-path");
    }

    /**
     * 解析DOM，获取MR输出路径
     * @param document
     * @return
     */
    public static String extractOutputPath(Document document){
        return document.getRootElement().elementText("output-path");
    }

    /**
     * 解析DOM，获取HBase配置
     * @param document
     * @return
     */
    public static String extractHbaseQuorum(Document document){
        return document.getRootElement().elementText("hbase.zookeeper.quorum");
    }

    /**
     * 解析DOM，获取HBase配置
     * @param document
     * @return
     */
    public static String extractHbaseClientPort(Document document){
        return document.getRootElement().elementText("hbase.zookeeper.property.clientPort");
    }

    /**
     * 解析DOM，获取HBase配置
     * @param document
     * @return
     */
    public static String extractHbaseMaxClientCnxns(Document document){
        return document.getRootElement().elementText("hbase.zookeeper.property.maxClientCnxns");
    }

    /**
     * 解析DOM，获取HBase配置
     * @param document
     * @return
     */
    public static String extractHbaseParent(Document document){
        return document.getRootElement().elementText("hbase.znode.parent");
    }

    /**
     * 解析DOM，获取HBase列族信息
     * @param document
     * @return
     */
    public static String extractHbaseColumnFamily(Document document){
        return document.getRootElement().elementText("htable-columnfamily");
    }
}
