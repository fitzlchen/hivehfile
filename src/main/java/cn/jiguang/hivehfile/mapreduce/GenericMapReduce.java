package cn.jiguang.hivehfile.mapreduce;

import cn.jiguang.hivehfile.util.XmlUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.io.SAXReader;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jiguang
 * Date: 2017/4/25
 */
public class GenericMapReduce implements Tool{
    static Logger logger = LogManager.getLogger(GenericMapReduce.class);
    static Configuration configuration = new Configuration();
    private static String configPath = null;

    public static class GenericMapper extends Mapper<LongWritable,Text,ImmutableBytesWritable,KeyValue>{
        private SAXReader saxReader = null;
        private Document document = null;
        private cn.jiguang.hivehfile.Configuration selfDefinedConfig = new cn.jiguang.hivehfile.Configuration();

        // 读取用户配置文件，进行参数装配
        @Override
        public void setup(Context context) throws IOException {
            String configFileName = new Path(configPath).getName();
            URI[] uris = Job.getInstance(configuration).getCacheFiles();
            for(URI $u : uris){
                if(configFileName.equals($u)){
                    // 解析 XML 文件并进行装配
                    saxReader = new SAXReader();
                    try {
                        document = saxReader.read($u.toURL());
                    } catch (DocumentException e) {
                        logger.fatal("解析配置文件时发生错误，请检查文件填写内容！\n"+e.getMessage());
                        System.exit(-1); // 解析配置文件失败，直接退出程序
                    }
                    selfDefinedConfig.setDelimiterCollection(XmlUtil.extractDelimiterCollection(document));
                    selfDefinedConfig.setRowkey(XmlUtil.extractRowKeyColumnName(document));
                    selfDefinedConfig.setMappingInfo(XmlUtil.extractMappingInfo(document));
                    selfDefinedConfig.setHtableName(XmlUtil.extractHtableName(document));
                    selfDefinedConfig.setDataDate(XmlUtil.extractDate(document));
                    selfDefinedConfig.setHbaseZookeeperQuorum(XmlUtil.extractHbaseQuorum(document));
                    selfDefinedConfig.setHbaseZnodeParent(XmlUtil.extractHbaseParent(document));
                    selfDefinedConfig.setHbaseZookeeperPropertyClientPort(XmlUtil.extractHbaseClientPort(document));
                    selfDefinedConfig.setHbaseZookeeperPropertyMaxClientCnxn(XmlUtil.extractHbaseMaxClientCnxns(document));
                    configuration.set("hbase.zookeeper.quorum", selfDefinedConfig.getHbaseZookeeperQuorum());
                    configuration.set("hbase.zookeeper.property.clientPort", selfDefinedConfig.getHbaseZookeeperPropertyClientPort());
                    configuration.set("hbase.zookeeper.property.maxClientCnxns", selfDefinedConfig.getHbaseZookeeperPropertyMaxClientCnxn());
                    configuration.set("zookeeper.znode.parent", selfDefinedConfig.getHbaseZnodeParent());
                    logger.info("Successfully read distributedcache file:");
                    break;
                }
            }


        }

        @Override
        public void map(LongWritable key, Text value, Context context){
            String inputString = value.toString();
            String[] values = inputString.split(selfDefinedConfig.getDelimiterCollection().get("field-delimiter"));
            ArrayList<HashMap<String,String>> mappingInfo = selfDefinedConfig.getMappingInfo();
            // 读取数据文件，填充到对应的Hive字段
            for(int i=0; i<values.length;i++){
                String columnType = mappingInfo.get(i).get("hive-column-type");
                Object columnValue = null;
                if(columnType.equals("string")){
                    columnValue = values[i];
                }else if(columnType.equals("bigint")){
                    columnValue = Long.parseLong(values[i]);
                } else if(columnType.equals("array")){
                    String[] subString = values[i].split(selfDefinedConfig.getDelimiterCollection().get("collection-item-delimiter"));
                    ArrayList<String> arr = new ArrayList<String>();
                    for(String $s:subString){
                        if( !$s.equals("") ){
                            arr.add($s);
                        }
                    }
                    columnValue = arr;
                }
                i++;
            }

        }
    }

    /**
     *  运行通用MR的入口
     * @param args
     * args[0]  :   配置文件的HDFS路径
     * args[1]  :   InputPath
     * args[2]  :   OutputPath
     * @return
     * @throws Exception
     */
    public int run(String[] args) throws Exception {
        configPath = args[0];
        FileSystem fileSystem = FileSystem.get(configuration);
        if(!fileSystem.exists(new Path(configPath))){
            logger.fatal("HDFS中不存在目标文件！请检查路径："+configPath);
            System.exit(-1);    // 直接异常退出
        }
        HashMap<String,String> someConfigs = readSomeConfigFromHdfs(fileSystem, new Path(configPath));
        String inputPath = null, outputPath = null, htableName = null;
        if(someConfigs!=null
                && someConfigs.containsKey("input-path")
                && someConfigs.containsKey("output-key")
                && someConfigs.containsKey("htable-name"))
        {
            inputPath = someConfigs.get("input-key");
            outputPath = someConfigs.get("output-key");
            htableName = someConfigs.get("htable-name");

        }else{
            logger.fatal("无法获取配置文件中的输入路径或输出路径或HBase表名！");
            System.exit(-1);
        }
        Job job = new Job(configuration);
        job.addCacheFile(new Path(args[0]).toUri());
        job.setJarByClass(FonovaMapReduce.class);
        job.setMapperClass(FonovaMapReduce.HFileMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
        job.setInputFormatClass(TextInputFormat.class);
        for (String p : inputPath.split(",")) {
            Path path = new Path(p);
            FileInputFormat.addInputPath(job, path);
        }
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        Configuration hbaseConf = HBaseConfiguration.create(configuration);
        Connection hbaseConnection = ConnectionFactory.createConnection(hbaseConf);
        Table table = hbaseConnection.getTable(TableName.valueOf(htableName));
        RegionLocator regionLocator = hbaseConnection.getRegionLocator(TableName.valueOf(htableName));
        HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);
        if (!job.waitForCompletion(true)) {
            logger.error("Failed, input:" + inputPath + ", output:" + outputPath);
            return -1;
        }else{
            logger.info("Success, input:" + inputPath + ", output:" + outputPath);
            return 0;
        }

    }

    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }

    public Configuration getConf() {
        return configuration;
    }

    /**
     * 从HDFS读取配置文件，并获取其中的input-path和output-path和htable-name路径
     * @param fs
     * @param path
     * @return
     * @throws IOException
     */
    private HashMap<String,String> readSomeConfigFromHdfs(FileSystem fs, Path path) throws IOException {
        FSDataInputStream in = fs.open(path);
        byte[] block = new byte[1024];
        int length = 0;
        StringBuffer sb = new StringBuffer();
        while((length = in.read(block))>0){
            sb.append(new String(block,"UTF-8"));
        }
        HashMap<String,String> result = new HashMap<String, String>();
        Matcher inputPathMatcher = Pattern.compile("<input-path>(.+)</input-path>").matcher(sb.toString());
        if(inputPathMatcher.find())
            result.put("input-path",inputPathMatcher.group(1));
        Matcher outputPathMatcher = Pattern.compile("<output-path>(.+)</output-path>").matcher(sb.toString());
        if(outputPathMatcher.find())
            result.put("output-path",outputPathMatcher.group(1));
        Matcher htableNameMatcher = Pattern.compile("<htable-name>(.+)</htable-name>").matcher(sb.toString());
        if(htableNameMatcher.find())
            result.put("htable-name",htableNameMatcher.group(1));
        return result;
    }
}
