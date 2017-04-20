package cn.jiguang.hivehfile.util;

import java.util.List;

/**
 * Created by jiguang
 * Date: 2017/4/20
 */
public class ArrayUtil {

    /**
     * 将ArrayList的元素打印为一个字符串（不包含 [ 、 ] )
     * @param list
     * @return
     */
    public static String printArrayListElements(List list){
        if(list==null)
            return "";
        return list.toString().substring(1,list.toString().length()-1).replace(" ","");
    }
}
