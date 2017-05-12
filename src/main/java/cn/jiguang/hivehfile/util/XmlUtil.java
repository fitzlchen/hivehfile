package cn.jiguang.hivehfile.util;

import cn.jiguang.hivehfile.Configuration;
import cn.jiguang.hivehfile.model.MappingInfo;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.IOException;
import java.util.*;

/**
 * Created by jiguang
 * Date: 2017/4/25
 */
public class XmlUtil {
    private static Logger logger = LogManager.getLogger(XmlUtil.class);

    /**
     * 解析DOM，生成对应的MappingInfo列表
     *
     * @param document
     * @return
     */
    public static ArrayList<MappingInfo> extractMappingInfoList(Document document) {
        ArrayList<MappingInfo> result = new ArrayList<MappingInfo>();
        List<Element> mappingInfoList = document.getRootElement().elements("mapping-info");
        for (Element $e : mappingInfoList) {
            MappingInfo mappingInfo = new MappingInfo();
            // MappingInfo中可能不含有partition信息
            if($e.element("partition")!=null){
                mappingInfo.setPartition($e.elementText("partition").replaceAll("\\s",""));
            }
            Iterator<Element> iterator = $e.elements("column-mapping").iterator();
            while (iterator.hasNext()) {
                Element $$e = iterator.next();
            /*
             * 字段映射时，Hive字段名和字段类型必填
             * Hive字段可能没有对应的hbase字段，也没有rowkey
             */
                HashMap<String, String> columnMap = new HashMap<String, String>();
                columnMap.put("hive-column-name", $$e.elementText("hive-column-name"));
                columnMap.put("hive-column-type", $$e.elementText("hive-column-type"));
                if ($$e.element("hbase-column-family") != null && !"".equals($$e.elementText("hbase-column-family").trim())) {
                    columnMap.put("hbase-column-family", $$e.elementText("hbase-column-family"));
                }
                if ($$e.element("hbase-column-qualifier") != null && !"".equals($$e.elementText("hbase-column-qualifier").trim())) {
                    columnMap.put("hbase-column-qualifier", $$e.elementText("hbase-column-qualifier"));
                }
                if ($$e.element("rowkey") != null && "true".equals($$e.elementText("rowkey").trim().toLowerCase())) {
                    columnMap.put("rowkey", "true");
                }
                mappingInfo.getColumnMappingList().add(columnMap);
            }
            result.add(mappingInfo);
        }
        return result;
    }

    /**
     * 获取当前数据文件对应的 MappingInfo
     * @param dataFilePath
     * @param mappingInfoList
     * @return
     */
    public static MappingInfo extractCurrentMappingInfo(String dataFilePath, ArrayList<MappingInfo> mappingInfoList) {
        MappingInfo result = null;
        for (MappingInfo $map : mappingInfoList) {
            // 检查数据文件路径中是否含有分区信息，以定位所需使用的MappingInfo。
            for(String $partitionSegment : $map.getPartition().split(",")){
                if(dataFilePath.indexOf($partitionSegment)!=-1){    // 找到对应 MappingInfo
                    result = $map;
                    break;
                }
            }
            if( result != null)break;
        }
        return result;
    }

    /**
     * 返回当前MappingInfo的rowkey字段名
     * @param mappingInfo
     * @return
     */
    public static String extractRowKeyColumnName(MappingInfo mappingInfo) {
        ArrayList<String> rowkeyList = new ArrayList<String>();
        ArrayList<HashMap<String, String>> columnMapping = mappingInfo.getColumnMappingList();
        for (HashMap<String, String> $m : columnMapping) {
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
     * 解析DOM，获取分隔符设定信息
     *
     * @param document
     * @return
     */
    public static HashMap<String, String> extractDelimiterCollection(Document document) {
        HashMap<String, String> result = new HashMap<String, String>();
        result.put("field-delimiter", String.valueOf((char) Integer.parseInt(document.getRootElement().elementText("field-delimiter").substring(2))));
        result.put("collection-item-delimiter", document.getRootElement().elementText("collection-item-delimiter"));
        return result;
    }

    /**
     * 获取Rowkey字段的索引
     *
     * @param mappingInfo
     * @return
     */
    public static int extractRowkeyIndex(MappingInfo mappingInfo) {
        Iterator<HashMap<String, String>> itera = mappingInfo.getColumnMappingList().iterator();
        String rowkeyName = extractRowKeyColumnName(mappingInfo);
        int index = -1;
        if (rowkeyName == null)
            return -1;  // 异常返回-1
        while (itera.hasNext()) {
            index++;
            if (rowkeyName.equals(itera.next().get("hive-column-name"))) {
                break;
            }
        }
        return index;
    }

    /**
     * 将配置文件实例化成一个Configuration对象
     * @param conf Hadoop Configuration
     * @param configFilePath 配置文件的HDFS路径
     * @return
     * @throws IOException
     */
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
        selfDefinedConfig.setMappingInfoList(XmlUtil.extractMappingInfoList(document));
        selfDefinedConfig.setHtableName(XmlUtil.extractHtableName(document));
        selfDefinedConfig.setHbaseZookeeperQuorum(XmlUtil.extractHbaseQuorum(document));
        selfDefinedConfig.setHbaseZookeeperPropertyClientPort(XmlUtil.extractHbaseClientPort(document));
        selfDefinedConfig.setHbaseZookeeperPropertyMaxClientCnxns(XmlUtil.extractHbaseMaxClientCnxns(document));
        selfDefinedConfig.setHbaseZnodeParent(XmlUtil.extractHbaseParent(document));
        return selfDefinedConfig;
    }
}
