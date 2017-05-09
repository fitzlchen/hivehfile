package cn.jiguang.hivehfile.mapreduce;

import cn.jiguang.hivehfile.util.DateUtil;
import cn.jiguang.hivehfile.util.PrintUtil;
import cn.jiguang.hivehfile.util.XmlUtil;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.hbase.util.Bytes;
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

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by jiguang
 * Date: 2017/4/25
 */
public class GenericMapReduce implements Tool {
    static Logger logger = LogManager.getLogger(GenericMapReduce.class);
    static Configuration configuration = new Configuration();
    private String configFilePath = null;

    public static class GenericMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
        private cn.jiguang.hivehfile.Configuration selfDefinedConfig = null;
        @Override
        public void setup(Context context) throws IOException {
            selfDefinedConfig = XmlUtil.generateConfigurationFromXml(context.getConfiguration(),context.getConfiguration().get("config.file.path"));
        }

        @Override
        public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            String inputString = value.toString();
            String[] values = inputString.split(selfDefinedConfig.getDelimiterCollection().get("field-delimiter"));
            ArrayList<HashMap<String, String>> mappingInfo = selfDefinedConfig.getMappingInfo();
            // 在每一行数据中，rowkey 和 timestamp 都固定不变
            ImmutableBytesWritable rowkey = new ImmutableBytesWritable(Bytes.toBytes(values[selfDefinedConfig.getRowkeyIndex()]));
            Long ts = 0L;
            try {
                ts = DateUtil.convertDateToUnixTime(selfDefinedConfig.getDataDate());
            } catch (ParseException e) {
                logger.error(e.getMessage());
                System.exit(-1);    // 无法解析强制退出
            }
            /* 开始装配HFile
             * 所需参数：
             * RowKey
             * ColumnFamily
             * ColumnQualifier
             * TimeStamp
             * Value
             */
            for (int i = 0; i < values.length; i++) {
                KeyValue kv = null;
                if (i != selfDefinedConfig.getRowkeyIndex()) {
                    kv = new KeyValue(Bytes.toBytes(values[selfDefinedConfig.getRowkeyIndex()]),
                            Bytes.toBytes(mappingInfo.get(i).get("hbase-column-family")),
                            Bytes.toBytes(mappingInfo.get(i).get("hbase-column-qualifier")),
                            ts,
                            Bytes.toBytes(PrintUtil.escapeConnotation(values[i]))
                    );
                }
                if (kv != null) context.write(rowkey, kv);
                i++;
            }
        }
    }

    /**
     * 运行通用MR的入口
     *
     * @param args :  配置文件的HDFS绝对路径
     * @return
     * @throws Exception
     */
    public int run(String[] args) throws Exception {
        configFilePath = args[0];
        cn.jiguang.hivehfile.Configuration selfDefinedConfig = XmlUtil.generateConfigurationFromXml(configuration,configFilePath);
        String inputPath = selfDefinedConfig.getInputPath(),
                outputPath = selfDefinedConfig.getOutputPath(),
                htableName = selfDefinedConfig.getHtableName();
        configuration.set("hbase.zookeeper.quorum", selfDefinedConfig.getHbaseZookeeperQuorum());
        configuration.set("hbase.zookeeper.property.clientPort", selfDefinedConfig.getHbaseZookeeperPropertyClientPort());
        configuration.set("hbase.zookeeper.property.maxClientCnxns", selfDefinedConfig.getHbaseZookeeperPropertyMaxClientCnxns());
        configuration.set("zookeeper.znode.parent", selfDefinedConfig.getHbaseZnodeParent());
        configuration.set("config.file.path",configFilePath);
        Job job = Job.getInstance(configuration);
        job.addCacheFile(new Path(args[0]).toUri());
        job.setJarByClass(GenericMapReduce.class);
        job.setMapperClass(GenericMapReduce.GenericMapper.class);
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
        } else {
            logger.info("Success, input:" + inputPath + ", output:" + outputPath);
            return 0;
        }
    }

    public void setConf(Configuration config) {
        configuration = config;
    }

    public Configuration getConf() {
        return configuration;
    }
}
