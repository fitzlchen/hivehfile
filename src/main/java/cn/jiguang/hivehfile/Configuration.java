package cn.jiguang.hivehfile;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by fitz on 2017/5/7.
 * Description:
 * 存储配置文件解析内容，方便作业调用
 */
public class Configuration {
    private String
            htableName, dataDate,hbaseZookeeperQuorum,
            hbaseZookeeperPropertyClientPort,
            hbaseZookeeperPropertyMaxClientCnxn, hbaseZnodeParent,
            inputPath,outputPath,
            rowkey;

    private ArrayList<HashMap<String,String>> mappingInfo;

    private HashMap<String,String> delimiterCollection;

    public HashMap<String, String> getDelimiterCollection() {
        return delimiterCollection;
    }

    public void setDelimiterCollection(HashMap<String, String> delimiterCollection) {
        this.delimiterCollection = delimiterCollection;
    }

    public String getRowkey() {
        return rowkey;
    }

    public void setRowkey(String rowkey) {
        this.rowkey = rowkey;
    }

    public String getInputPath() {
        return inputPath;
    }

    public void setInputPath(String inputPath) {
        this.inputPath = inputPath;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    public String getDataDate() {
        return dataDate;
    }

    public void setDataDate(String dataDate) {
        this.dataDate = dataDate;
    }

    public ArrayList<HashMap<String, String>> getMappingInfo() {
        return mappingInfo;
    }

    public void setMappingInfo(ArrayList<HashMap<String, String>> mappingInfo) {
        this.mappingInfo = mappingInfo;
    }

    public String getHtableName() {
        return htableName;
    }

    public void setHtableName(String htableName) {
        this.htableName = htableName;
    }

    public String getHbaseZookeeperQuorum() {
        return hbaseZookeeperQuorum;
    }

    public void setHbaseZookeeperQuorum(String hbaseZookeeperQuorum) {
        this.hbaseZookeeperQuorum = hbaseZookeeperQuorum;
    }

    public String getHbaseZookeeperPropertyClientPort() {
        return hbaseZookeeperPropertyClientPort;
    }

    public void setHbaseZookeeperPropertyClientPort(String hbaseZookeeperPropertyClientPort) {
        this.hbaseZookeeperPropertyClientPort = hbaseZookeeperPropertyClientPort;
    }

    public String getHbaseZookeeperPropertyMaxClientCnxn() {
        return hbaseZookeeperPropertyMaxClientCnxn;
    }

    public void setHbaseZookeeperPropertyMaxClientCnxn(String hbaseZookeeperPropertyMaxClientCnxn) {
        this.hbaseZookeeperPropertyMaxClientCnxn = hbaseZookeeperPropertyMaxClientCnxn;
    }

    public String getHbaseZnodeParent() {
        return hbaseZnodeParent;
    }

    public void setHbaseZnodeParent(String hbaseZnodeParent) {
        this.hbaseZnodeParent = hbaseZnodeParent;
    }
}

