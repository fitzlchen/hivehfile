package cn.jiguang.hivehfile.struct;

/**
 * Created by jiguang
 * Date: 2017/4/26
 */
public class FraudFeatureNorStruct {
    public String getImei() {
        return imei;
    }

    public void setImei(String imei) {
        this.imei = imei;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object obj){
        boolean isEquals = false;
        if(obj instanceof FraudFeatureNorStruct){
            FraudFeatureNorStruct reference = (FraudFeatureNorStruct) obj;
            isEquals = imei.equals(reference.getImei()) && value.equals(reference.getValue());
        }
        return isEquals;
    }

    private String imei;
    private String value;
}
