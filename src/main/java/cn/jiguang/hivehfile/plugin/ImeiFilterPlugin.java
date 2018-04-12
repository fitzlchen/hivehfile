package cn.jiguang.hivehfile.plugin;

/**
 * Created by: fitz
 * <p>
 * Date: 2018/3/5
 * <p>
 * Description:
 */
public class ImeiFilterPlugin implements IPlugin{
    private String regex = "(^iPhone.*)|([a-zA-Z0-9]*[^a-zA-Z0-9]+[a-zA-Z0-9]*)";

    @Override
    public boolean filter(String str) {
        return str.matches(regex);
    }
}
