package cn.jiguang.hivehfile.mapreduce;

import cn.jiguang.hivehfile.exception.FileAlreadyExistsException;
import cn.jiguang.hivehfile.mapreduce.mapper.ParquetMapper;
import cn.jiguang.hivehfile.mapreduce.mapper.TextMapper;
import cn.jiguang.hivehfile.util.HdfsUtil;
import cn.jiguang.hivehfile.util.MapUtil;
import cn.jiguang.hivehfile.util.XmlUtil;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.avro.AvroParquetInputFormat;

import java.util.List;

/**
 * Created by jiguang
 * Date: 2017/4/25
 */
public class GenericMapReduce extends Configured implements Tool {
    static Logger logger = LogManager.getLogger(GenericMapReduce.class);
    private String configFilePath = null;
    private Job job = null;

    /**
     * 运行通用MR的入口
     *
     * @param args :  配置文件的HDFS绝对路径
     *             -config 配置文件路径
     *             -dict 字典参数
     *             -format 数据文件格式
     *             -unique 是否生成唯一的时间戳
     *             -name 自定义任务名称
     * @return
     * @throws Exception
     */
    public int run(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("config", true, "配置文件的HDFS路径");
        options.addOption("dict", true, "字典参数");
        options.addOption("format", true, "数据文件格式");
        options.addOption("unique", true, "键值对的时间戳是否唯一");
        options.addOption("name",true,"作业名称");
        CommandLine cmd = new BasicParser().parse(options, args);
        if (!cmd.hasOption("config")) {
            logger.fatal("缺少配置文件路径，请检查传递的参数！");
            System.exit(-1);
        }
        configFilePath = cmd.getOptionValue("config");
        Configuration configuration = getConf();
        if (cmd.hasOption("dict")) {
            configuration.set("user.defined.parameters", cmd.getOptionValue("dict"));
            logger.info("接收到的字典字符串：" + cmd.getOptionValue("dict"));
            if (MapUtil.convertStringToMap(cmd.getOptionValue("dict")) == null) {
                logger.fatal("传入的字符串无法解析成字典，请检查输入参数！");
                System.exit(-1);    // 直接异常退出
            }
        }
        /* 第三个参数默认是数据文件的格式
         * 默认格式为 TextFile，支持 Parquet
         */
        String fileFormat = null;
        if (cmd.hasOption("format")) {
            fileFormat = cmd.getOptionValue("format");
        }
        // 解析unique命令行参数
        if (cmd.hasOption("unique")) {
            configuration.set("user.defined.parameter.unique", cmd.getOptionValue("unique"));
            logger.info("是否生成唯一的键值对时间戳： " + cmd.getOptionValue("unique"));
        }
        cn.jiguang.hivehfile.Configuration selfDefinedConfig = XmlUtil.generateConfigurationFromXml(configuration, configFilePath);
        // 将 InputPath 与所有 partition 拼接
        String inputPath = selfDefinedConfig.getAllInputPath();
        String outputPath = selfDefinedConfig.getOutputPath(),
                htableName = selfDefinedConfig.getHtableName();
        logger.info("[TEST]mapreduce.reduce.memory.mb=" + configuration.get("mapreduce.reduce.memory.mb"));
        // 检出输出目录是否存在
        FileSystem fs = FileSystem.get(configuration);
        if (fs.exists(new Path(outputPath))){
            String errMsg = String.format("输出目录已经存在:%s", outputPath);
            throw new FileAlreadyExistsException(errMsg);
        }
        configuration.set("hbase.zookeeper.quorum", selfDefinedConfig.getHbaseZookeeperQuorum());
        configuration.set("hbase.zookeeper.property.clientPort", selfDefinedConfig.getHbaseZookeeperPropertyClientPort());
        configuration.set("hbase.zookeeper.property.maxClientCnxns", selfDefinedConfig.getHbaseZookeeperPropertyMaxClientCnxns());
        configuration.set("zookeeper.znode.parent", selfDefinedConfig.getHbaseZnodeParent());
        configuration.set("config.file.path", configFilePath);
        configuration.setLong("hbase.hregion.max.filesize",307374182400L);
        if (cmd.hasOption("name")){
            job = Job.getInstance(configuration, cmd.getOptionValue("name"));
        } else {
            job = Job.getInstance(configuration);
        }
        job.setJarByClass(GenericMapReduce.class);
        // 根据指定的文件类型，使用不同的 Mapper
        if (fileFormat != null && "parquet".equalsIgnoreCase(fileFormat)) {
            job.setMapperClass(ParquetMapper.class);
            job.setInputFormatClass(AvroParquetInputFormat.class);
        } else {
            job.setMapperClass(TextMapper.class);
            job.setInputFormatClass(TextInputFormat.class);
        }
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
        for (String _path : inputPath.split(",")) {
            if (HdfsUtil.exists(_path)) {
                List<String> filePathList = HdfsUtil.getAllFilePaths(_path, ".+$(?<!\\.tmp)");
                for (String _fpath : filePathList) {
                    Path  filePath = new Path(_fpath);
                    FileInputFormat.addInputPath(job, filePath);
                }
            }
        }
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        Configuration hbaseConf = HBaseConfiguration.create(configuration);
        Connection hbaseConnection = ConnectionFactory.createConnection(hbaseConf);
        logger.info("HTABLE INFO===>TableName:" + htableName);
        Table table = hbaseConnection.getTable(TableName.valueOf(htableName));
        RegionLocator regionLocator = hbaseConnection.getRegionLocator(TableName.valueOf(htableName));
        logger.info("START INCREMENTALLOAD...");
        HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);
        if (!job.waitForCompletion(true)) {
            logger.error("Failed, input:" + inputPath + ", output:" + outputPath);
            return -1;
        } else {
            logger.info("Success, input:" + inputPath + ", output:" + outputPath);
            return 0;
        }
    }
}
