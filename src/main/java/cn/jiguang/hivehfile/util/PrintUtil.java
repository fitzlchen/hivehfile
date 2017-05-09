package cn.jiguang.hivehfile.util;

/**
 * Created by jiguang
 * Date: 2017/5/8
 * <p>
 * Description:
 */
public class PrintUtil {
    /**
     *  替换文本中的分隔符
     *  当前处理的分隔符：\n,\t
     * @param input
     * @return
     */
    public static String escapeConnotation(String input){
        String str = input.replaceAll("\n","\\\\n");
        str = str.replaceAll("\t","\\\\t");
        return str;
    }
}
