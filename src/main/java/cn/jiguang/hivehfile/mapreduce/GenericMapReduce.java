package cn.jiguang.hivehfile.mapreduce;

import cn.jiguang.hivehfile.model.MappingInfo;
import cn.jiguang.hivehfile.util.DateUtil;
import cn.jiguang.hivehfile.util.MapUtil;
import cn.jiguang.hivehfile.util.PrintUtil;
import cn.jiguang.hivehfile.util.XmlUtil;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.ParseException;

/**
 * Created by jiguang
 * Date: 2017/4/25
 */
public class GenericMapReduce implements Tool {
    static Logger logger = LogManager.getLogger(GenericMapReduce.class);
    static Configuration configuration = null;
    private String configFilePath = null;

    public static class GenericMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
        private cn.jiguang.hivehfile.Configuration selfDefinedConfig = null;

        @Override
        public void setup(Context context) throws IOException {
            // 读取HDFS配置文件，并将其封装成对象
            selfDefinedConfig = XmlUtil.generateConfigurationFromXml(context.getConfiguration(), context.getConfiguration().get("config.file.path"));
        }

        @Override
        public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            String inputString = value.toString();
            // 获取数据文件的路径
            String dataFilePath = ((FileSplit) context.getInputSplit()).getPath().toString();
            String[] values = inputString.split(selfDefinedConfig.getDelimiterCollection().get("field-delimiter"));
            // 获取当前 MappingInfo
            MappingInfo currentMappingInfo = XmlUtil.extractCurrentMappingInfo(dataFilePath, selfDefinedConfig.getMappingInfoList());
            // 在每一行数据中，rowkey 和 timestamp 都固定不变
            ImmutableBytesWritable rowkey = new ImmutableBytesWritable(Bytes.toBytes(values[XmlUtil.extractRowkeyIndex(currentMappingInfo)]));
            Long ts = 0L;
            /*
             * 解析数据文件路径，获取数据日期 data_date
             * 当数据文件路径中不含有 data_date 时，默认使用当前时间
             */
            try {
                ts = DateUtil.convertStringToUnixTime(dataFilePath, "yyyyMMdd", "data_date=(\\d{8})");
            } catch (ParseException e) {
                logger.fatal("无法解析数据日期，请检查InputPath和Partition的填写！");
                System.exit(-1);    // 异常直接退出
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
                if (i != XmlUtil.extractRowkeyIndex(currentMappingInfo)
                        && currentMappingInfo.getColumnMappingList().get(i).get("hbase-column-family") != null
                        && currentMappingInfo.getColumnMappingList().get(i).get("hbase-column-qualifier") != null
                        ) {  // 只遍历非 Rowkey 且 需要写入 HBase 的字段
                    try {
                        String transformedValue = PrintUtil.escapeConnotation(values[i]);
                        // 字段取值可能为空，将所有空值 \\N 转换为空串
                        if ("\\N".equals(transformedValue)) {
                            transformedValue = "";
                        }
                        kv = new KeyValue(Bytes.toBytes(values[XmlUtil.extractRowkeyIndex(currentMappingInfo)]),
                                Bytes.toBytes(currentMappingInfo.getColumnMappingList().get(i).get("hbase-column-family")),
                                Bytes.toBytes(currentMappingInfo.getColumnMappingList().get(i).get("hbase-column-qualifier")),
                                ts,
                                Bytes.toBytes(transformedValue)
                        );
                    } catch (Exception e) {
                        logger.error("异常数据：" + values[XmlUtil.extractRowkeyIndex(currentMappingInfo)] + ":" +
                                PrintUtil.escapeConnotation(values[i]));
                        logger.error(e.getMessage());
                    }
                }
                if (kv != null) context.write(rowkey, kv);
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
        if (args.length == 0) {
            logger.fatal("缺少配置文件路径，请检查传递的参数！");
            System.exit(-1);
        }
        configFilePath = args[0];
        // 如果含有两个参数，则将第二个参数视为XML变量字典
        if (args.length == 2) {
            configuration.set("user.defined.parameters", args[1]);
            logger.info("接收到的字典字符串：" + args[1]);
            if (MapUtil.convertStringToMap(args[1]) == null) {
                logger.fatal("传入的字符串无法解析成字典，请检查输入参数！");
                System.exit(-1);    // 直接异常退出
            }
        }
        cn.jiguang.hivehfile.Configuration selfDefinedConfig = XmlUtil.generateConfigurationFromXml(configuration, configFilePath);
        // 将 InputPath 与所有 partition 拼接
        String inputPath = selfDefinedConfig.getAllInputPath();
        String outputPath = selfDefinedConfig.getOutputPath(),
                htableName = selfDefinedConfig.getHtableName();
        configuration.set("hbase.zookeeper.quorum", selfDefinedConfig.getHbaseZookeeperQuorum());
        configuration.set("hbase.zookeeper.property.clientPort", selfDefinedConfig.getHbaseZookeeperPropertyClientPort());
        configuration.set("hbase.zookeeper.property.maxClientCnxns", selfDefinedConfig.getHbaseZookeeperPropertyMaxClientCnxns());
        configuration.set("zookeeper.znode.parent", selfDefinedConfig.getHbaseZnodeParent());
        configuration.set("config.file.path", configFilePath);
        Job job = Job.getInstance(configuration);
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
        logger.info("HTABLE INFO===>TableName:" + htableName);
        Table table = hbaseConnection.getTable(TableName.valueOf(htableName));
        RegionLocator regionLocator = hbaseConnection.getRegionLocator(TableName.valueOf(htableName));
        logger.info("START INCREMENTALLOAD...");
        logger.info("VALIDATE NAMESPACE:" + table.getTableDescriptor().getNameAsString());
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
