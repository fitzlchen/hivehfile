import cn.jiguang.hivehfile.mapreduce.FonovaActiveMapReduce;
import cn.jiguang.hivehfile.mapreduce.FonovaMapReduce;
import cn.jiguang.hivehfile.mapreduce.FraudFeatureNorMapReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
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
//            ToolRunner.run(conf, new FonovaMapReduce(),args);
            ToolRunner.run(conf, new FraudFeatureNorMapReduce(),args);
//            ToolRunner.run(conf,new FonovaActiveMapReduce(),args);
        }catch (Exception e){
            logger.error(e.getMessage());
        }
    }
}
