package cn.jiguang.hivehfile.mapreduce.mapper;

import cn.jiguang.hivehfile.counter.RowkeyCounter;
import cn.jiguang.hivehfile.model.MappingInfo;
import cn.jiguang.hivehfile.util.DateUtil;
import cn.jiguang.hivehfile.util.PrintUtil;
import cn.jiguang.hivehfile.util.XmlUtil;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by: fitz
 * <p>
 * Date: 2017/6/24
 * <p>
 * Description:
 */
public class TextMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
    private Logger logger = LogManager.getLogger(TextMapper.class);
    private cn.jiguang.hivehfile.Configuration selfDefinedConfig = null;
    private String
            unique = null, delimiter = null, plugins = null, dataFilePath = null;
    private Map<String, String> partitionInfo = null;
    private MappingInfo currentMappingInfo = null;

    @Override
    public void setup(Context context) throws IOException {
        // 读取HDFS配置文件，并将其封装成对象
        selfDefinedConfig = XmlUtil.generateConfigurationFromXml(context.getConfiguration(), context.getConfiguration().get("config.file.path"));
        unique = context.getConfiguration().get("user.defined.parameter.unique");
        delimiter = selfDefinedConfig.getDelimiterCollection().get("field-delimiter");
        plugins = context.getConfiguration().get("user.defined.plugins");
        // extract partition info from hive managed table
        partitionInfo = Maps.newHashMap();
        // find out the file name of the input split
        dataFilePath = ((FileSplit) context.getInputSplit()).getPath().getParent().toString();
        Matcher matcher = Pattern.compile("((\\w+)=(\\w+))").matcher(dataFilePath);
        while (matcher.find()) {
            partitionInfo.put(matcher.group(2), matcher.group(3));
        }
        // get current MappingInfo
        currentMappingInfo = XmlUtil.extractCurrentMappingInfo(dataFilePath, selfDefinedConfig.getMappingInfoList());
        if (currentMappingInfo == null){
            logger.error("CurrentMappingInfo is null! Killing container...");
            System.exit(-1);
        }
    }

    @Override
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String inputString = value.toString();
        ArrayList<String> values = Lists.newArrayList(Splitter.on(delimiter).split(inputString));
        // check weather the number of ColumnMapping is equals to the number of hive columns
        if (!currentMappingInfo.isColumnMatch(values.size())) {
            throw new InterruptedException("配置文件校验失败，配置文件的column-mapping数目与数据文件不匹配！异常内容：" + inputString);
        }
        if (Strings.isNullOrEmpty(values.get(XmlUtil.extractRowkeyIndex(currentMappingInfo)))) {
            logger.error("异常数据，ROWKEY 为空");
            return;
        }

        ImmutableBytesWritable rowkey = new ImmutableBytesWritable(Bytes.toBytes(values.get(XmlUtil.extractRowkeyIndex(currentMappingInfo))));
        // use customized filter plugin
        if (plugins != null) {
            Splitter.on(plugins).split(",");
        }

        Long ts = 0L;
        /*
         * 解析数据文件路径，获取数据日期 data_date
         * 当数据文件路径中不含有 data_date 时，默认使用当前时间
         */
        try {
            if ("true".equalsIgnoreCase(unique))
                ts = DateUtil.generateUniqTimeStamp(dataFilePath, "yyyyMMdd", "data_date=(\\d{8})");
            else
                ts = DateUtil.convertStringToUnixTime(dataFilePath, "yyyyMMdd", "data_date=(\\d{8})");
        } catch (ParseException e) {
            logger.fatal("无法解析数据日期，请检查InputPath和Partition的填写！");
            System.exit(-1);    // 异常直接退出
        }

        HashMap<String, Integer> dynamicFillColumnRela;

        /* 开始装配HFile
         * 所需参数：
         * RowKey
         * ColumnFamily
         * ColumnQualifier
         * TimeStamp
         * Value
         */
        for (int i = 0; i < values.size(); i++) {
            KeyValue kv = null;
            String columnFamily = null, columnQualifier = null, pureColumnFamily = null, pureColumnQualifier = null;
            // 只遍历非 Rowkey 且 需要写入 HBase 的字段
            if (i != XmlUtil.extractRowkeyIndex(currentMappingInfo)
                    && currentMappingInfo.getColumnMappingList().get(i).get("hbase-column-family") != null
                    && currentMappingInfo.getColumnMappingList().get(i).get("hbase-column-qualifier") != null) {
                String transformedValue = PrintUtil.escapeConnotation(values.get(i));
                // 字段取值可能为空，将所有空值 \\N 转换为空串
                if ("\\N".equals(transformedValue)) {
                    transformedValue = "";
                }

                // 判断是否使用了字段动态填充功能
                columnFamily = currentMappingInfo.getColumnMappingList().get(i).get("hbase-column-family");
                columnQualifier = currentMappingInfo.getColumnMappingList().get(i).get("hbase-column-qualifier");
                pureColumnFamily = columnFamily.replace("#", "");
                pureColumnQualifier = columnQualifier.replace("#", "");
                if (currentMappingInfo.isDynamicFill()) {
                    dynamicFillColumnRela = currentMappingInfo.getDynamicFillColumnRela();
                    if (columnFamily.indexOf("#") != -1) {
                        // 判断使用分区字段还是使用列字段
                        if (partitionInfo.containsKey(pureColumnFamily)) {
                            columnFamily = partitionInfo.get(pureColumnFamily);
                        } else {
                            columnFamily = values.get(dynamicFillColumnRela.get(pureColumnFamily));
                        }
                    }
                    if (columnQualifier.indexOf("#") != -1) {
                        // 判断使用分区字段还是使用列字段
                        if (partitionInfo.containsKey(pureColumnQualifier)) {
                            columnQualifier = partitionInfo.get(pureColumnQualifier);
                        } else {
                            columnQualifier = values.get(dynamicFillColumnRela.get(pureColumnQualifier));
                        }
                    }
                }
                try {
                    // 限制 value 占用空间小于 10MB
                    if (transformedValue.getBytes().length > 10 * 1024 * 1024) {
                        continue;
                    }
                    kv = new KeyValue(Bytes.toBytes(values.get(XmlUtil.extractRowkeyIndex(currentMappingInfo))),
                            Bytes.toBytes(columnFamily),
                            Bytes.toBytes(columnQualifier),
                            ts,
                            Bytes.toBytes(transformedValue)
                    );
                } catch (Exception e) {
                    logger.error("异常数据：" + values.get(XmlUtil.extractRowkeyIndex(currentMappingInfo)) + ":" +
                            PrintUtil.escapeConnotation(values.get(i)));
                    logger.error(e.getMessage());
                }
            }
            if (kv != null) context.write(rowkey, kv);
        }
    }
}
