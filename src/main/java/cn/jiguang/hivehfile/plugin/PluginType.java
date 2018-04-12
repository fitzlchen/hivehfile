package cn.jiguang.hivehfile.plugin;

/**
 * Created by: fitz
 * <p>
 * Date: 2018/3/8
 * <p>
 * Description:
 */
public enum PluginType {
    IMEIFILTER(new ImeiFilterPlugin());

    private IPlugin plugin;

    public IPlugin getPlugin(){
        return plugin;
    }

    PluginType(IPlugin plugin){
        this.plugin = plugin;
    }
}
