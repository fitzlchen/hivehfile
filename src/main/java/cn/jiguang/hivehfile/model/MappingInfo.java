package cn.jiguang.hivehfile.model;

import avro.shaded.com.google.common.collect.Maps;
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
    private ArrayList<HashMap<String, String>> columnMappingList = new ArrayList<HashMap<String, String>>();

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

    /**
     * 检验数据文件的字段数是否与配置文件中 MappingInfo 所含的 ColumnMapping 数目一致
     *
     * @param num 数据文件的字段数
     * @return
     */
    public boolean isColumnMatch(int num) {
        return columnMappingList.size() == num;
    }

    /**
     * 检验是否采用字段动态填充
     *
     * @return
     */
    public boolean isDynamicFill() {
        for (HashMap<String, String> columnMapping : columnMappingList) {
            if (columnMapping.containsKey("hbase-column-family") && columnMapping.get("hbase-column-family").indexOf("#") != -1) {
                return true;
            }
            if (columnMapping.containsKey("hbase-column-qualifier") && columnMapping.get("hbase-column-qualifier").indexOf("#") != -1) {
                return true;
            }
        }
        return false;
    }

    /**
     * 获取动态填充字段的名称和对应的索引，索引从0 开始
     *
     * @return
     */
    public HashMap<String, Integer> getDynamicFillColumnRela() {
        HashMap<String, Integer> result = Maps.newHashMap();
        for (HashMap<String, String> columnMapping : columnMappingList) {
            if (columnMapping.containsKey("hbase-column-family") && columnMapping.get("hbase-column-family").indexOf("#") != -1) {
                String columnName = columnMapping.get("hbase-column-family").replace("#","");
                result.put(columnName, findIndexByName(columnName));
            }
            if (columnMapping.containsKey("hbase-column-qualifier") && columnMapping.get("hbase-column-qualifier").indexOf("#") != -1) {
                String columnName = columnMapping.get("hbase-column-qualifier").replace("#","");
                result.put(columnName, findIndexByName(columnName));
            }
        }
        return result;
    }

    /**
     * 找出指定 hive 字段的索引
     *
     * @param name 字段名称
     * @return
     */
    private Integer findIndexByName(String name) {
        for (int i = 0; i < columnMappingList.size(); i++) {
            HashMap<String, String> columnMapping = columnMappingList.get(i);
            if (name.equals(columnMapping.get("hive-column-name"))) {
                return i;
            }
        }
        return -1;
    }

    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }

    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

}
