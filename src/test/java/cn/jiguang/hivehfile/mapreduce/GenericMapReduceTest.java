package cn.jiguang.hivehfile.mapreduce;

import org.junit.Test;
import static org.junit.Assert.*;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by fitz on 2017/5/7.
 */
public class GenericMapReduceTest {
    @Test
    public void testReadSomeConfigFromHdfs(){
        HashMap<String,String> expected = new HashMap<String, String>();
        expected.put("input-path","input");
        expected.put("output-path","output");
        expected.put("htable-name","test");

        String inStr = "    <input-path>input</input-path>\n" +
                "    <output-path>output</output-path>\n" +
                "    <htable-name>test</htable-name>";
        HashMap<String,String> actual = new HashMap<String, String>();
        Matcher inputPathMatcher = Pattern.compile("<input-path>(.+)</input-path>").matcher(inStr);
        if(inputPathMatcher.find())
            actual.put("input-path",inputPathMatcher.group(1));
        Matcher outputPathMatcher = Pattern.compile("<output-path>(.+)</output-path>").matcher(inStr);
        if(outputPathMatcher.find())
            actual.put("output-path",outputPathMatcher.group(1));
        Matcher htableNameMatcher = Pattern.compile("<htable-name>(.+)</htable-name>").matcher(inStr);
        if(htableNameMatcher.find())
            actual.put("htable-name",htableNameMatcher.group(1));
        assertEquals(expected,actual);
    }
}
