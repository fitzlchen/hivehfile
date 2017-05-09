package cn.jiguang.hivehfile.util;

import cn.jiguang.hivehfile.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

import javax.print.Doc;
import java.io.IOException;
import java.util.*;

/**
 * Created by jiguang
 * Date: 2017/4/25
 */
public class XmlUtil {
    private static Logger logger = LogManager.getLogger(XmlUtil.class);

    /**
     * 根据DOM，生成Hive表的字段数组（字段名、字段类型）
     *
     * @param document
     * @return
     */
    public static ArrayList<HashMap<String, String>> extractMappingInfo(Document document) {
        ArrayList<HashMap<String, String>> result = new ArrayList<HashMap<String, String>>();
        Iterator<Element> iterator = document.getRootElement().element("mapping-info").elementIterator();
        while (iterator.hasNext()) {
            Element e = iterator.next();
            /*
             * 字段映射时，Hive字段名和字段类型必填
             * Hive字段可能没有对应的hbase字段，也没有rowkey
             */
            HashMap<String, String> columnMap = new HashMap<String, String>();
            columnMap.put("hive-column-name", e.elementText("hive-column-name"));
            columnMap.put("hive-column-type", e.elementText("hive-column-type"));
            if (e.element("hbase-column-family") != null && !"".equals(e.elementText("hbase-column-family").trim())) {
                columnMap.put("hbase-column-family", e.elementText("hbase-column-family"));
            }
            if (e.element("hbase-column-qualifier") != null && !"".equals(e.elementText("hbase-column-qualifier").trim())) {
                columnMap.put("hbase-column-qualifier", e.elementText("hbase-column-qualifier"));
            }
            if (e.element("rowkey") != null && "true".equals(e.elementText("rowkey").trim().toLowerCase())) {
                columnMap.put("rowkey", "true");
            }
            result.add(columnMap);
        }
        return result;
    }


    /**
     * 获取rowkey字段的名称，当存在多个rowkey时返回null
     *
     * @param document
     * @return
     */
    public static String extractRowKeyColumnName(Document document) {
        ArrayList<String> rowkeyList = new ArrayList<String>();
        ArrayList<HashMap<String, String>> mappingInfo = extractMappingInfo(document);
        for (HashMap<String, String> $m : mappingInfo) {
            if ($m.containsKey("rowkey")) {
                rowkeyList.add($m.get("hive-column-name"));
            }
        }
        if (rowkeyList.size() != 1) {
            return null;
        } else {
            return rowkeyList.get(0);
        }
    }

    /**
     * 解析DOM，获取htable名称
     *
     * @param document
     * @return
     */
    public static String extractHtableName(Document document) {
        return document.getRootElement().elementText("htable-name");
    }

    /**
     * 解析DOM，获取HBase字段的Date信息，方便后续Date => Unix Time
     *
     * @param document
     * @return
     */
    public static String extractDate(Document document) {
        return document.getRootElement().elementText("data-date");
    }

    /**
     * 解析DOM，获取MR输入路径
     *
     * @param document
     * @return
     */
    public static String extractInputPath(Document document) {
        return document.getRootElement().elementText("input-path");
    }

    /**
     * 解析DOM，获取MR输出路径
     *
     * @param document
     * @return
     */
    public static String extractOutputPath(Document document) {
        return document.getRootElement().elementText("output-path");
    }

    /**
     * 解析DOM，获取HBase配置
     *
     * @param document
     * @return
     */
    public static String extractHbaseQuorum(Document document) {
        return document.getRootElement().elementText("hbase.zookeeper.quorum");
    }

    /**
     * 解析DOM，获取HBase配置
     *
     * @param document
     * @return
     */
    public static String extractHbaseClientPort(Document document) {
        return document.getRootElement().elementText("hbase.zookeeper.property.clientPort");
    }

    /**
     * 解析DOM，获取HBase配置
     *
     * @param document
     * @return
     */
    public static String extractHbaseMaxClientCnxns(Document document) {
        return document.getRootElement().elementText("hbase.zookeeper.property.maxClientCnxns");
    }

    /**
     * 解析DOM，获取HBase配置
     *
     * @param document
     * @return
     */
    public static String extractHbaseParent(Document document) {
        return document.getRootElement().elementText("hbase.znode.parent");
    }

    /**
     * 解析DOM，获取HBase列族信息
     *
     * @param document
     * @return
     */
    public static String extractHbaseColumnFamily(Document document) {
        return document.getRootElement().elementText("htable-columnfamily");
    }

    /**
     * 解析DOM，获取分隔符设定信息
     *
     * @param document
     * @return
     */
    public static HashMap<String, String> extractDelimiterCollection(Document document) {
        HashMap<String, String> result = new HashMap<String, String>();
        result.put("field-delimiter", String.valueOf((char)Integer.parseInt(document.getRootElement().elementText("field-delimiter").substring(2))));
        result.put("collection-item-delimiter", document.getRootElement().elementText("collection-item-delimiter"));
        return result;
    }

    /**
     * 获取Rowkey字段的索引
     *
     * @param document
     * @return
     */
    public static int extractRowkeyIndex(Document document) {
        Iterator<Element> itera = document.getRootElement().element("mapping-info").elementIterator();
        String rowkeyName = extractRowKeyColumnName(document);
        int index = -1;
        if (rowkeyName == null)
            return -1;  // 异常返回-1
        while (itera.hasNext()) {
            index++;
            if (rowkeyName.equals(itera.next().elementText("hive-column-name"))) {
                break;
            }
        }
        return index;
    }


    public static Configuration generateConfigurationFromXml(org.apache.hadoop.conf.Configuration conf, String configFilePath) throws IOException {
        FileSystem fileSystem = FileSystem.get(conf);
        if (!fileSystem.exists(new Path(configFilePath))) {
            logger.fatal("HDFS中不存在目标文件！请检查路径：" + configFilePath);
            System.exit(-1);    // 直接异常退出
        }
        // 解析 XML 文件并进行装配
        SAXReader saxReader = new SAXReader();
        Document document = null;
        try {
            document = saxReader.read(fileSystem.open(new Path(configFilePath)));
        } catch (DocumentException e) {
            logger.fatal("解析配置文件时发生错误，请检查文件填写内容！\n" + e.getMessage());
            System.exit(-1); // 解析配置文件失败，直接退出程序
        }
        Configuration selfDefinedConfig = new Configuration();
        selfDefinedConfig.setInputPath(XmlUtil.extractInputPath(document));
        selfDefinedConfig.setOutputPath(XmlUtil.extractOutputPath(document));
        selfDefinedConfig.setHtableName(XmlUtil.extractHtableName(document));
        selfDefinedConfig.setDelimiterCollection(XmlUtil.extractDelimiterCollection(document));
        selfDefinedConfig.setRowkey(XmlUtil.extractRowKeyColumnName(document));
        selfDefinedConfig.setRowkeyIndex(XmlUtil.extractRowkeyIndex(document));
        selfDefinedConfig.setMappingInfo(XmlUtil.extractMappingInfo(document));
        selfDefinedConfig.setHtableName(XmlUtil.extractHtableName(document));
        selfDefinedConfig.setDataDate(XmlUtil.extractDate(document));
        selfDefinedConfig.setHbaseZookeeperQuorum(XmlUtil.extractHbaseQuorum(document));
        selfDefinedConfig.setHbaseZookeeperPropertyClientPort(XmlUtil.extractHbaseClientPort(document));
        selfDefinedConfig.setHbaseZookeeperPropertyMaxClientCnxns(XmlUtil.extractHbaseMaxClientCnxns(document));
        selfDefinedConfig.setHbaseZnodeParent(XmlUtil.extractHbaseParent(document));
        return selfDefinedConfig;
    }
}
