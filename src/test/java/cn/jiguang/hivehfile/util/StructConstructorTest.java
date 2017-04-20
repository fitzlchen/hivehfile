package cn.jiguang.hivehfile.util;

import cn.jiguang.hivehfile.exception.ColumnNumMismatchException;
import cn.jiguang.hivehfile.struct.FonovaStruct;
import org.junit.Test;
import static org.junit.Assert.*;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jiguang
 * Date: 2017/4/19
 */
public class StructConstructorTest {
    @Test
    public void testParse() throws ClassNotFoundException, ColumnNumMismatchException, InstantiationException, IllegalAccessException, InvocationTargetException {
        String input = "28e46edd676870dd\u0001重庆市\u0001中国\u00010\u00010\u00010\u0001重庆斯威特时尚酒店\u0002赛格尔酒店\u0002重庆希曼宾馆\u0002重庆洲际酒店\u0002家心怡酒店解放碑店\u0002扬子岛酒店\u00010\u00010\u00010\u00010\u00013\u0001229227\u00010\u00010\u00012\u0001152818\u00011\u000176409";
        // construct reference
        FonovaStruct reference = new FonovaStruct();
        ArrayList<String> city_list = new ArrayList<String>(),country_list = new ArrayList<String>(),sleep_loc = new ArrayList<String>();
        city_list.add("重庆市");
        country_list.add("中国");
        sleep_loc.add("重庆斯威特时尚酒店");
        sleep_loc.add("赛格尔酒店");
        sleep_loc.add("重庆希曼宾馆");
        sleep_loc.add("重庆洲际酒店");
        sleep_loc.add("家心怡酒店解放碑店");
        sleep_loc.add("扬子岛酒店");
        reference.setImei("28e46edd676870dd");
        reference.setCity_list(city_list);
        reference.setCountry_list(country_list);
        reference.setApp_count(0);
        reference.setApp_tour_count(0);
        reference.setApp_hotel_count(0);
        reference.setSleep_loc(sleep_loc);
        reference.setApp_fly_count(0);
        reference.setApp_train_count(0);
        reference.setVisit_times_car(0);
        reference.setDur_all_car(0);
        reference.setVisit_times_medical_instit(3);
        reference.setDur_all_medical_instit(229227);
        reference.setVisit_times_hospital_compre(0);
        reference.setDur_all_hospital_compre(0);
        reference.setVisit_times_hospital_profess(2);
        reference.setDur_all_hospital_profess(152818);
        reference.setVisit_times_clinic(1);
        reference.setDur_all_clinic(76409);

        // compare
        String columnTypeString = "imei:string,city_list:array,country_list:array,app_count:bigint,app_tour_count:bigint,app_hotel_count:bigint,sleep_loc:array,app_fly_count:bigint,app_train_count:bigint,visit_times_car:bigint,dur_all_car:bigint,visit_times_medical_instit:bigint,dur_all_medical_instit:bigint,visit_times_hospital_compre:bigint,dur_all_hospital_compre:bigint,visit_times_hospital_profess:bigint,dur_all_hospital_profess:bigint,visit_times_clinic:bigint,dur_all_clinic:bigint";
        List list = StructConstructor.assemblyColumnList(columnTypeString);
        FonovaStruct generated = (FonovaStruct) StructConstructor.parse(input,"cn.jiguang.hivehfile.struct.FonovaStruct",list);
        System.out.println(generated.equals(reference));
        System.out.println((String) StructConstructor.invokeGet(generated,"imei"));
    }
}
