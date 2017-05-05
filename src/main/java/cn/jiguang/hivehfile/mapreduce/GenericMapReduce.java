package cn.jiguang.hivehfile.mapreduce;

import cn.jiguang.hivehfile.util.XmlUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.io.SAXReader;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Map;

/**
 * Created by jiguang
 * Date: 2017/4/25
 */
public class GenericMapReduce implements Tool{
    static Logger logger = LogManager.getLogger(GenericMapReduce.class);
    static Configuration configuration = new Configuration();

    public static class GenericMapper extends Mapper<LongWritable,Text,ImmutableBytesWritable,KeyValue>{
        private ArrayList<Map<String,String>>  cachedConfig = new ArrayList<Map<String, String>>();
        private SAXReader saxReader = null;
        private Document document = null;
        private String
                htableName = null
                ,date = null
                ,inputPath = null
                ,outputPath = null
                ,hbaseQuorum = null
                ,hbaseClientPort = null
                ,hbaseMaxClientCnxns = null
                ,hbaseParent = null;

        // 读取用户配置文件，进行参数装配
        @Override
        public void setup(Context context) throws IOException {
            URI[] configURI = context.getCacheFiles();
            if(configURI.length != 1){
                logger.fatal("读取配置文件时发生异常！\n URI:" + configURI.toString());
            }
            // 解析 XML 文件并进行装配
             saxReader = new SAXReader();
            try {
                document = saxReader.read(configURI[0].toURL());
            } catch (DocumentException e) {
                logger.fatal("解析配置文件时发生错误，请检查文件填写内容！\n"+e.getMessage());
                System.exit(1); // 解析配置文件失败，直接退出程序
            }
            cachedConfig = XmlUtil.extractHiveStruct(document);
            htableName = XmlUtil.extractHtableName(document);
            date = XmlUtil.extractDate(document);
            inputPath = XmlUtil.extractInputPath(document);
            outputPath  = XmlUtil.extractOutputPath(document);
            hbaseQuorum = XmlUtil.extractHbaseQuorum(document);
            hbaseClientPort = XmlUtil.extractHbaseClientPort(document);
            hbaseMaxClientCnxns = XmlUtil.extractHbaseMaxClientCnxns(document);
            hbaseParent = XmlUtil.extractHbaseParent(document);
            configuration.set("hbase.zookeeper.quorum", hbaseQuorum);
            configuration.set("hbase.zookeeper.property.clientPort", hbaseClientPort);
            configuration.set("hbase.zookeeper.property.maxClientCnxns", hbaseMaxClientCnxns);
            configuration.set("zookeeper.znode.parent", hbaseParent);
            // 读取分布式缓存文件
            URI[] uris = Job.getInstance(configuration).getCacheFiles();
            for(URI $u : uris){
                if("".equals($u)){

                    logger.info("Successfully read distributedcache file:");
                    break;
                }
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context){
            String inputString = value.toString();


        }
    }

    public int run(String[] args) throws Exception {
        Job job  = new Job();
        job.addCacheFile(new Path(args[0]).toUri());
        job.setJarByClass(GenericMapReduce.class);
        job.setMapperClass(GenericMapper.class);

        return 0;
    }

    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }

    public Configuration getConf() {
        return configuration;
    }

}
