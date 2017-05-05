package cn.jiguang.hivehfile.mapreduce;

import cn.jiguang.hivehfile.exception.ColumnNumMismatchException;
import cn.jiguang.hivehfile.struct.FonovaActiveStruct;
import cn.jiguang.hivehfile.util.DateUtil;
import cn.jiguang.hivehfile.util.StructConstructor;
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
 * Date: 2017/4/26
 */
public class FonovaActiveMapReduce implements Tool {
    static Logger logger = LogManager.getLogger(FonovaActiveMapReduce.class);
    private Configuration conf = new Configuration();

    /**
     * 运行MapReduce的入口
     *
     * @param args 第一个参数是目标 HBase 表名；第二个参数是 HDFS 读取路径； 第三个参数是 HDFS 写入路径
     * @return
     * @throws Exception
     */
    public int run(String[] args) throws Exception {
        String tableName = args[0];
        String input = args[1];
        String output = args[2];
        conf.set("hbase.zookeeper.quorum", "192.168.254.71,192.168.254.72,192.168.254.73");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.property.maxClientCnxns", "400");
        conf.set("zookeeper.znode.parent", "/hbase");

        Job job = Job.getInstance(conf);
        job.setJarByClass(FonovaActiveMapReduce.class);
        job.setMapperClass(HFileMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
        job.setInputFormatClass(TextInputFormat.class);
        for (String p : input.split(",")) {
            Path path = new Path(p);
            FileInputFormat.addInputPath(job, path);
        }
        FileOutputFormat.setOutputPath(job, new Path(output));
        Configuration hbaseConf = HBaseConfiguration.create(conf);
        Connection hbaseConnection = ConnectionFactory.createConnection(hbaseConf);
        Table table = hbaseConnection.getTable(TableName.valueOf(tableName));
        RegionLocator regionLocator = hbaseConnection.getRegionLocator(TableName.valueOf(tableName));
        HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);
        if (!job.waitForCompletion(true)) {
            logger.error("Failed, input:" + input + ", output:" + output);
            return -1;
        } else {
            logger.info("Success, input:" + input + ", output:" + output);
            return 0;
        }

    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public Configuration getConf() {
        return conf;
    }

    private static class HFileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
        public void map(LongWritable key, Text value, Context context) {
            String stringValue = value.toString();
            // 解析对象的字段名称和字段类型
            String struct = "imei:string,value:string";
            FonovaActiveStruct fonovaActiveStruct = null;
            try {
                fonovaActiveStruct = (FonovaActiveStruct) StructConstructor.parse(stringValue,
                        "cn.jiguang.hivehfile.struct.FonovaActiveStruct", StructConstructor.assemblyColumnList(struct));
            } catch (ClassNotFoundException e) {
                logger.error(e.getMessage());
            } catch (IllegalAccessException e) {
                logger.error(e.getMessage());
            } catch (InstantiationException e) {
                logger.error(e.getMessage());
            } catch (ColumnNumMismatchException e) {
                logger.error(e.getMessage());
            }
            Long ts = 0L;
            if (null != fonovaActiveStruct) {
                String splitPath = ((FileSplit) context.getInputSplit()).getPath().toString();
                if (splitPath.indexOf("data_date=") == -1) {
                    logger.fatal("Input file path does not contain data_date keyword.  Please check input file path!");
                    System.exit(-1);    // 如果文件路径不含有 data_date 关键字则直接退出 MapReduce
                }
                try {
                    ts = DateUtil.convertStringToUnixTime(splitPath, "yyyyMMdd", "data_date=(\\d{8})");  // data_date=yyyyMMdd
                    if (ts == 0L)
                        logger.fatal("Can not generate timestamp. Please check input file path!");
                } catch (ParseException e) {
                    logger.error(e.getMessage());
                }
                // 开始装配HFile
                /* RowKey 固定为 imei
                 * ColumnFamily 固定为 A
                 * ColumnQualifier 固定为 columnName
                 * TimeStamp 固定为 数据日期，即data_date
                 * Value 固定为 columnValue
                 */
                ImmutableBytesWritable rowKey = new ImmutableBytesWritable(Bytes.toBytes(fonovaActiveStruct.getImei()));
                try {
                    for (String field : StructConstructor.getStructFields("cn.jiguang.hivehfile.struct.FonovaActiveStruct")) {
                        KeyValue kv = null;
                        /*
                         * imei为空的不写入
                         */
                        if (fonovaActiveStruct.getImei() == null || fonovaActiveStruct.getImei().trim().equals("")) {
                            break;
                        }
                        if (field.equals("value")) {
                            kv = new KeyValue(Bytes.toBytes(fonovaActiveStruct.getImei()),Bytes.toBytes("A"),Bytes.toBytes("app_class_active")
                                    ,ts,Bytes.toBytes(fonovaActiveStruct.getValue()));
                        }
                        if (kv != null) context.write(rowKey, kv);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
