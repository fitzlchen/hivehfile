package cn.jiguang.hivehfile.mapreduce;

import cn.jiguang.hivehfile.mapreduce.mapper.TextMapper;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;

/**
 * Created by fitz on 2017/5/7.
 */
public class GenericMapReduceTest {
    MapDriver<LongWritable, Text, ImmutableBytesWritable, KeyValue> mapDriver;

    @Before
    public void setup() {
        mapDriver = MapDriver.newMapDriver(new TextMapper());
    }

    @Test
    public void testGenericMapReduce() {

    }
}



