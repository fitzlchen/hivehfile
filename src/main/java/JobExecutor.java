import cn.jiguang.hivehfile.mapreduce.GenericMapReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


/**
 * Created by jiguang
 * Date: 2017/4/19
 */
public class JobExecutor {
    static Logger logger  = LogManager.getLogger(JobExecutor.class);

    public static void main(String[] args){
        Configuration conf = new Configuration();
        int code = 0;   // MapReduce 返回码
        try{
           code = ToolRunner.run(conf,new GenericMapReduce(),args);
        }catch (Exception e){
            logger.error(e.getMessage());
            System.exit(-1);
        }
        if (code != 0){
           System.exit(-1);
        }
    }
}
