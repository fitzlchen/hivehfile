README

名词解释：
mappingInfo		： Hive字段和Hbase列的映射组关系。每一个映射组由一个或多个column-mapping构成，其表达了一套Hive字段与HBase字段的对应关系。
column-mapping  :  单个Hive字段与HBase字段的对应关系。
-------------------------------
配置文件组成部分：
1.全局设置
	1.1 输入的数据的HDFS路径<input-path> 	        				[常修改]
	1.2 输出HFile的HDFS路径 <output-path>							[常修改]
	1.3 HBase表名 <htable-name>										[常修改]
	1.4 Hive表的字段分隔符 <field-delimiter>						[不常修改]
	1.5 Hive表Array元素的分隔符 <collection-item-delimiter>			[不常修改]
	1.6 HBase集群配置（用于读取HBase表的Region信息）				[不修改]
		1.6.1 <hbase.zookeeper.quorum>
		1.6.2 <hbase.zookeeper.property.clientPort>
		1.6.3 <hbase.zookeeper.property.maxClientCnxns>
		1.6.4 <hbase.znode.parent>
2.局部设置
	2.1 Hive表字段与HBase列的映射关系 <MappingInfo>，关于 <MappingInfo>的具体填写方式见后文   [常修改]
-------------------------------
MappingInfo结构：
|- MappingInfo
		|- partition
		|- column-mapping
				|- hive-column-name
				|- hive-column-type
				|- rowkey
				|- hbase-column-family
				|- hbase-column-qualifier
-------------------------------
MappingInfo参考样例：
<mapping-info>
	<partition>
		feature=black_wifi_cnt/data_date=20170420,
		feature=black_wifi_cnt/data_date=20170421,
		feature=black_wifi_cnt/data_date=20170425
	</partition>
    <column-mapping>
        <hive-column-name>jid</hive-column-name>
        <hive-column-type>string</hive-column-type>
    </column-mapping>
    <column-mapping>
        <hive-column-name>imei</hive-column-name>
        <hive-column-type>string</hive-column-type>
        <rowkey>true</rowkey>
    </column-mapping>
    <column-mapping>
        <hive-column-name>value</hive-column-name>
        <hive-column-type>string</hive-column-type>
        <hbase-column-family>A</hbase-column-family>
        <hbase-column-qualifier>relationship_risk</hbase-column-qualifier>
    </column-mapping>
</mapping-info>

如果表是分区表，则填写<partition>标签。在上面例子中，feature和data_date均为分区字段。
<column-mapping>表示一个Hive表字段和一个HBase字段的对应关系。
填写<column-mapping>时，【必须】按照Hive表字段的顺序填写，这关系到Hive数据文件的解析结果。如果未按照Hive字段顺序填写，有可能出现字段和值错位的情况。
如果一个Hive字段需要作为HBase的rowkey,则<rowkey>标记为true，如上例的imei字段；
如果一个字段不需要写入HBase，则只填写该字段的<hive-column-name>和<hive-column-type>，如上例子的jid字段；
如果一个字段需要写入HBase，则需要填写HBase的ColumnFamily和ColumnQualifier信息，即填写<hbase-column-family>和<hbase-column-qualifier>标签，如上例的value字段。

对于非分区表，则没有<partition>标签，且一个配置文件只允许含有一个MappingInfo。（当非分区表存在多个MappingInfo时，无法判断一条Hive记录需要通过哪一个MappingInfo进行处理）
-------------------------------
配置文件的参考样例：
<?xml version="1.0" encoding="UTF-8" ?>
<config>
	<!-- Global Settings -->
	<input-path>hdfs://nameservice1/tmp/test-hfile-input</input-path>
    <output-path>hdfs://nameservice1/tmp/test-hfile-output</output-path>
    <htable-name>fraud_feature_nor</htable-name>
    <field-delimiter>\u0001</field-delimiter>
    <collection-item-delimiter>,</collection-item-delimiter>
    <hbase.zookeeper.quorum>192.168.254.71,192.168.254.72,192.168.254.73</hbase.zookeeper.quorum>
    <hbase.zookeeper.property.clientPort>2181</hbase.zookeeper.property.clientPort>
    <hbase.zookeeper.property.maxClientCnxns>400</hbase.zookeeper.property.maxClientCnxns>
    <hbase.znode.parent>/hbase</hbase.znode.parent>

	<!-- Local Settings -->
    <mapping-info>
		<partition>
			feature=black_wifi_cnt/data_date=20170420,
			feature=black_wifi_cnt/data_date=20170421,
			feature=black_wifi_cnt/data_date=20170425
		</partition>
        <column-mapping>
            <hive-column-name>imei</hive-column-name>
            <hive-column-type>string</hive-column-type>
            <rowkey>true</rowkey>
        </column-mapping>
        <column-mapping>
            <hive-column-name>value</hive-column-name>
            <hive-column-type>string</hive-column-type>
            <hbase-column-family>A</hbase-column-family>
            <hbase-column-qualifier>relationship_risk</hbase-column-qualifier>
        </column-mapping>
    </mapping-info>

	<mapping-info>
		<partition>
			feature=install_pkg_cnt/data_date=20170423,
			feature=install_pkg_cnt/data_date=20170424,
			feature=install_pkg_cnt/data_date=20170425
		</partition>
        <column-mapping>
            <hive-column-name>imei</hive-column-name>
            <hive-column-type>string</hive-column-type>
            <rowkey>true</rowkey>
        </column-mapping>
        <column-mapping>
            <hive-column-name>value</hive-column-name>
            <hive-column-type>string</hive-column-type>
            <hbase-column-family>A</hbase-column-family>
            <hbase-column-qualifier>total_app_cnt</hbase-column-qualifier>
        </column-mapping>
    </mapping-info>
</config>







