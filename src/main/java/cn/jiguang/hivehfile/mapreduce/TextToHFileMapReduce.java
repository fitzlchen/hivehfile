package cn.jiguang.hivehfile.mapreduce;


import cn.jiguang.hivehfile.exception.ColumnNumMismatchException;
import cn.jiguang.hivehfile.struct.FonovaStruct;
import cn.jiguang.hivehfile.util.ArrayUtil;
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
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import static cn.jiguang.hivehfile.util.StructConstructor.invokeGet;

/**
 * Created by jiguang
 * Date: 2017/4/19
 */
public class TextToHFileMapReduce implements Tool {
    static Logger logger  = LogManager.getLogger(TextToHFileMapReduce.class);
    private Configuration conf = new Configuration();

    /**
     * 运行MapReduce的入口
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
        job.setJarByClass(TextToHFileMapReduce.class);
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
        }else{
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

    public static class HFileMapper extends Mapper<LongWritable,Text,ImmutableBytesWritable, KeyValue >{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String stringValue = value.toString();
            // 解析对象的字段名称和字段类型
            String struct = "imei:string,city_list:array,country_list:array,app_count:bigint,app_tour_count:bigint" +
                    ",app_hotel_count:bigint,sleep_loc:array,app_fly_count:bigint,app_train_count:bigint" +
                    ",visit_times_car:bigint,dur_all_car:bigint,visit_times_medical_instit:bigint" +
                    ",dur_all_medical_instit:bigint,visit_times_hospital_compre:bigint,dur_all_hospital_compre:bigint" +
                    ",visit_times_hospital_profess:bigint,dur_all_hospital_profess:bigint,visit_times_clinic:bigint" +
                    ",dur_all_clinic:bigint";
            FonovaStruct fonava = null;
            try {
                fonava = (FonovaStruct) StructConstructor.parse(stringValue,
                        "cn.jiguang.hivehfile.struct.FonovaStruct",StructConstructor.assemblyColumnList(struct));
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
            if(null != fonava){
                String splitPath = ((FileSplit)context.getInputSplit()).getPath().toString();
                if(splitPath.indexOf("data_date=")==-1){
                    logger.fatal("Input file path does not contain data_date keyword.  Please check input file path!");
                    System.exit(-1);    // 如果文件路径不含有 data_date 关键字则直接退出 MapReduce
                }
                try {
                    ts = DateUtil.convertStringToUnixTime(splitPath,"yyyyMMdd","data_date=(\\d{8})");  // data_date=yyyyMMdd
                    if(ts==0L)
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
                KeyValue kv = null;
                ImmutableBytesWritable rowKey = new ImmutableBytesWritable(Bytes.toBytes(fonava.getImei()));
                try {
                    for(String field:StructConstructor.getStructFields("cn.jiguang.hivehfile.struct.FonovaStruct")){
                        /*
                         * imei为空和imei以iPhone开头的不写入
                         */
                        if(fonava.getImei()==null || fonava.getImei()=="" || fonava.getImei().startsWith("iPhone")){
                            continue;
                        }
                        /*
                         * array为[]不写入
                         */
                        if( invokeGet(fonava,field) instanceof List){
                           if( ((List) invokeGet(fonava,field)).size() == 0 ){
                                   continue;
                             }else{
                               kv = new KeyValue(Bytes.toBytes(fonava.getImei()),Bytes.toBytes("A")
                                       ,Bytes.toBytes(field),ts, Bytes.toBytes(ArrayUtil.printArrayListElements((List) invokeGet(fonava,field))));
                           }
                        }
                        /*
                         * int为0的不写入
                         */
                        if( invokeGet(fonava,field) instanceof Long){
                            if( (Long) invokeGet(fonava,field) == 0){
                                continue;
                            }else{
                                kv = new KeyValue(Bytes.toBytes(fonava.getImei()),Bytes.toBytes("A")
                                        ,Bytes.toBytes(field),ts, Bytes.toBytes((invokeGet(fonava,field)).toString()));
                            }
                        }
                        if(kv != null) context.write(rowKey,kv);
                    }
                } catch (ClassNotFoundException e) {
                    logger.error(e.getMessage());
                } catch (IllegalAccessException e) {
                    logger.error(e.getMessage());
                } catch (InvocationTargetException e) {
                    logger.error(e.getMessage());
                }
            }
        }
    }

}
