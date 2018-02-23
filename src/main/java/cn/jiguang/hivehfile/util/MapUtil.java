package cn.jiguang.hivehfile.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Stack;

/**
 * Created by fitz
 * Date: 2017/5/16
 * <p>
 * Description:
 */
public class MapUtil {
    /**
     * 简易字符串解析器，将输入的字符串解析成Map对象
     * 传递参数的双引号
     * @param input
     * @return
     */
    public static HashMap<String,String> convertStringToMap(String input){
        Stack<Character> stack = new Stack<Character>();
        ArrayList<String> mapParams = new ArrayList<String>();
        StringBuffer sb = new StringBuffer();
        for(char $c:input.toCharArray()){
            switch ($c){
                case '{':
                case '\'':
                case '\\':
                case ':':
                case ',':
                case '}':
                    if($c=='\'' && stack.peek()=='\\'){
                        stack.pop();
                        sb.append('\'');
                        break;
                    }
                    if($c=='\'' && stack.peek()=='\''){
                        stack.push($c);
                        mapParams.add(sb.toString());
                        sb = new StringBuffer();    // 清空元素
                        break;
                    }
                    // 区分是字符串中的逗号还是KV的间隔逗号
                    if($c==',' && stack.peek()=='\'' && sb.length()!=0){
                        sb.append($c);
                        break;
                    }
                    // 区分是字符串中的冒号还是KV间隔冒号
                    if($c==':' && sb.length()!=0){
                        sb.append($c);
                        break;
                    }
                    stack.push($c);
                    break;
                default:
                    sb.append($c);
            }
        }
        // 如果元素列表长度为0或元素个数不为偶数，则返回null
        if(mapParams.size()%2 != 0 || mapParams.size()==0) return null;
        HashMap<String,String> result = new HashMap<String, String>();
        int counter = 1;  // 计数器
        String mapKey = null;
        for(String $s : mapParams){
            if(counter%2 != 0)
                mapKey = $s;
            else
                result.put(mapKey,$s);

            counter++;
        }
        return result;
    }
}
