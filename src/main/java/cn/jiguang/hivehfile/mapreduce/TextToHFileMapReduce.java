package cn.jiguang.hivehfile.mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.FileInputStream;
import java.text.SimpleDateFormat;
import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

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
        String tableName = args[0];
        String type = args[1];
        String day = args[2];
        String input = args[3];
        String output = args[4];
        logger.info(String.format("%s, %s, %s, %s, %s", tableName, type, day, input, output));
        conf.set("type", type);
        conf.setLong("ts", getTimestamp(day));
        conf.set("hbase.zookeeper.quorum",prop.getProperty("hbase.zookeeper.quorum"));
        conf.set("hbase.zookeeper.property.clientPort",prop.getProperty("hbase.zookeeper.property.clientPort"));
        conf.set("hbase.zookeeper.property.maxClientCnxns",prop.getProperty("hbase.zookeeper.property.maxClientCnxns"));
        conf.set("zookeeper.znode.parent",prop.getProperty("zookeeper.znode.parent"));
        Job job = Job.getInstance(conf, "userprofile-hfile-" + type + "-" + day);
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
        //hbase
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

    public long getTimestamp(String day) {
        long ts = 0;
        try {
            ts =dateFormattor .parse(day).getTime();
        } catch (Exception e) {
            logger.error("get timestamp error", e);
        }
        return ts;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public Configuration getConf() {
        return conf;
    }

    private static class HFileMapper extends Mapper<LongWritable,Text,ImmutableBytesWritable, KeyValue >{


    }

}
