package cn.jiguang.hivehfile.mapreduce;


import cn.jiguang.hivehfile.exception.ColumnNumMismatchException;
import cn.jiguang.hivehfile.struct.FonovaStruct;
import cn.jiguang.hivehfile.util.ArrayUtil;
import cn.jiguang.hivehfile.util.DateUtil;
import cn.jiguang.hivehfile.util.StructConstructor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
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
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import static cn.jiguang.hivehfile.util.StructConstructor.invokeGet;

/**
 * Created by jiguang
 * Date: 2017/4/19
 */
public class TextToHFileMapReduce implements Tool {
    static Logger logger  = LogManager.getLogger(TextToHFileMapReduce.class);
    private static SimpleDateFormat dateFormattor = new SimpleDateFormat("yyyyMMdd");
    private Configuration conf = new Configuration();
    private Properties prop = new Properties();

    public int run(String[] args) throws Exception {
        FileInputStream ins = new FileInputStream("config.properties");
        prop.load(ins);
        String input = args[0];
        String output = args[1];
        Job job = Job.getInstance(conf);
        job.setJarByClass(TextToHFileMapReduce.class);
        job.setMapperClass(HFileMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setNumReduceTasks(0);
        for (String p : input.split(",")) {
            Path path = new Path(p);
            FileInputFormat.addInputPath(job, path);
        }
        FileOutputFormat.setOutputPath(job, new Path(output));

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
                FonovaStruct fonova = (FonovaStruct) StructConstructor.parse(stringValue,
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
                String splitPath = ((FileSplit)context.getInputSplit()).getPath().getName();
                try {
                    ts = DateUtil.convertStringToUnixTime(splitPath,"yyyyMMdd","data_date=(\\d{8})");  // data_date=yyyyMMdd
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
                        if( invokeGet(fonava,field) instanceof Array){
                           if( ((ArrayList) invokeGet(fonava,field)).size() == 0 ){
                                   continue;
                             }else{
                               kv = new KeyValue(Bytes.toBytes(fonava.getImei()),Bytes.toBytes("A")
                                       ,Bytes.toBytes(field),ts, Bytes.toBytes(ArrayUtil.printArrayListElements((ArrayList) invokeGet(fonava,field))));
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
                                        ,Bytes.toBytes(field),ts, Bytes.toBytes((Long)invokeGet(fonava,field)));
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
