package cn.jiguang.hivehfile.plugin;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Created by: fitz
 * <p>
 * Date: 2018/3/8
 * <p>
 * Description:
 */
public class ImeiFilterPluginTest {
    ImeiFilterPlugin plugin = new ImeiFilterPlugin();

    @Test
    public void testFilter(){
        String str = "jbnjkzxc81_923";
        assertEquals(true, plugin.filter(str));
    }
}
