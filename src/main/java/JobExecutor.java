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
        try{
           ToolRunner.run(conf,new GenericMapReduce(),args);
        }catch (Exception e){
            logger.error(e.getMessage());
        }
    }
}
