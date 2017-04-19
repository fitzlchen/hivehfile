package cn.jiguang.hivehfile.struct;

import java.util.List;

/**
 * Created by jiguang
 * Date: 2017/4/19
 */
// 本对象对应Hive fosunapp.fosun_fonova表结构
public class FonovaStruct {
    private String imei;
    private long app_count;
    private long app_tour_count;
    private long app_hotel_count;
    private long app_fly_count;
    private long app_train_count;
    private long visit_times_car;
    private long dur_all_car;
    private long visit_times_medical_instit;
    private long dur_all_medical_instit;
    private long visit_times_hospital_compre;
    private long dur_all_hospital_compre;
    private long visit_times_hospital_profess;
    private long dur_all_hospital_profess;
    private long visit_times_clinic;
    private long dur_all_clinic;
    private List<String> city_list,country_list,sleep_loc;

    public String getImei() {
        return imei;
    }

    public void setImei(String imei) {
        this.imei = imei;
    }

    public long getApp_count() {
        return app_count;
    }

    public void setApp_count(long app_count) {
        this.app_count = app_count;
    }

    public long getApp_tour_count() {
        return app_tour_count;
    }

    public void setApp_tour_count(long app_tour_count) {
        this.app_tour_count = app_tour_count;
    }

    public long getApp_hotel_count() {
        return app_hotel_count;
    }

    public void setApp_hotel_count(long app_hotel_count) {
        this.app_hotel_count = app_hotel_count;
    }

    public long getApp_fly_count() {
        return app_fly_count;
    }

    public void setApp_fly_count(long app_fly_count) {
        this.app_fly_count = app_fly_count;
    }

    public long getApp_train_count() {
        return app_train_count;
    }

    public void setApp_train_count(long app_train_count) {
        this.app_train_count = app_train_count;
    }

    public long getVisit_times_car() {
        return visit_times_car;
    }

    public void setVisit_times_car(long visit_times_car) {
        this.visit_times_car = visit_times_car;
    }

    public long getDur_all_car() {
        return dur_all_car;
    }

    public void setDur_all_car(long dur_all_car) {
        this.dur_all_car = dur_all_car;
    }

    public long getVisit_times_medical_instit() {
        return visit_times_medical_instit;
    }

    public void setVisit_times_medical_instit(long visit_times_medical_instit) {
        this.visit_times_medical_instit = visit_times_medical_instit;
    }

    public long getDur_all_medical_instit() {
        return dur_all_medical_instit;
    }

    public void setDur_all_medical_instit(long dur_all_medical_instit) {
        this.dur_all_medical_instit = dur_all_medical_instit;
    }

    public long getVisit_times_hospital_compre() {
        return visit_times_hospital_compre;
    }

    public void setVisit_times_hospital_compre(long visit_times_hospital_compre) {
        this.visit_times_hospital_compre = visit_times_hospital_compre;
    }

    public long getDur_all_hospital_compre() {
        return dur_all_hospital_compre;
    }

    public void setDur_all_hospital_compre(long dur_all_hospital_compre) {
        this.dur_all_hospital_compre = dur_all_hospital_compre;
    }

    public long getVisit_times_hospital_profess() {
        return visit_times_hospital_profess;
    }

    public void setVisit_times_hospital_profess(long visit_times_hospital_profess) {
        this.visit_times_hospital_profess = visit_times_hospital_profess;
    }

    public long getDur_all_hospital_profess() {
        return dur_all_hospital_profess;
    }

    public void setDur_all_hospital_profess(long dur_all_hospital_profess) {
        this.dur_all_hospital_profess = dur_all_hospital_profess;
    }

    public long getVisit_times_clinic() {
        return visit_times_clinic;
    }

    public void setVisit_times_clinic(long visit_times_clinic) {
        this.visit_times_clinic = visit_times_clinic;
    }

    public long getDur_all_clinic() {
        return dur_all_clinic;
    }

    public void setDur_all_clinic(long dur_all_clinic) {
        this.dur_all_clinic = dur_all_clinic;
    }

    public List<String> getCity_list() {
        return city_list;
    }

    public void setCity_list(List<String> city_list) {
        this.city_list = city_list;
    }

    public List<String> getCountry_list() {
        return country_list;
    }

    public void setCountry_list(List<String> country_list) {
        this.country_list = country_list;
    }

    public List<String> getSleep_loc() {
        return sleep_loc;
    }

    public void setSleep_loc(List<String> sleep_loc) {
        this.sleep_loc = sleep_loc;
    }

    @Override
    public boolean equals(Object obj){
        if(this==obj)
            return true;
        if(obj instanceof FonovaStruct){
            FonovaStruct objInstance = (FonovaStruct) obj;
            return imei.equals(objInstance.getImei()) && city_list.equals(objInstance.getCity_list())
                    && country_list.equals(objInstance.getCountry_list()) && app_count==objInstance.getApp_count()
                    && app_tour_count==objInstance.getApp_tour_count() && app_hotel_count==objInstance.getApp_hotel_count()
                    && sleep_loc.equals(objInstance.getSleep_loc()) && app_fly_count == objInstance.getApp_fly_count()
                    && app_train_count == objInstance.getApp_train_count() && visit_times_car == objInstance.getVisit_times_car()
                    && dur_all_car == objInstance.getDur_all_car() && visit_times_medical_instit == objInstance.getVisit_times_medical_instit()
                    && dur_all_medical_instit == objInstance.getDur_all_medical_instit()
                    && visit_times_hospital_compre == objInstance.getVisit_times_hospital_compre()
                    && dur_all_hospital_compre == objInstance.getDur_all_hospital_compre()
                    && visit_times_hospital_profess == objInstance.getVisit_times_hospital_profess()
                    && dur_all_hospital_profess == objInstance.getDur_all_hospital_profess()
                    && visit_times_clinic == objInstance.getVisit_times_clinic() && dur_all_clinic == objInstance.getDur_all_clinic();
        }else{
            return false;
        }
    }
}
