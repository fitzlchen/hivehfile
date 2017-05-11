package cn.jiguang.hivehfile.model;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by jiguang
 * Date: 2017/5/9
 * <p>
 * Description:
 */
public class MappingInfo {
    private String partition;
    private ArrayList<HashMap<String,String>> columnMappingList = new ArrayList<HashMap<String, String>>();

    public String getPartition() {
        return partition;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }

    public ArrayList<HashMap<String, String>> getColumnMappingList() {
        return columnMappingList;
    }

    public void setColumnMappingList(ArrayList<HashMap<String, String>> columnMappingList) {
        this.columnMappingList = columnMappingList;
    }

    public boolean equals(Object obj){
        return EqualsBuilder.reflectionEquals(this,obj);
    }

    public int hashCode(){
        return HashCodeBuilder.reflectionHashCode(this);
    }

}
