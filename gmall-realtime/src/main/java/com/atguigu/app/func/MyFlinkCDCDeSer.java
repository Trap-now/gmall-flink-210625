package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public class MyFlinkCDCDeSer implements DebeziumDeserializationSchema<String> {
    /**
     *{
     *   "database":"gmall_flink_0625",
     *   "tableName":"aaa",
     *   "after":{"id":"123","name":"zs"....},
     *   "before":{"id":"123","name":"zs"....},
     *   "type":"insert",
     *}
     */
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {

        //1.创建一个JSONObject用来存放最终封装的数据
        JSONObject jsonObject = new JSONObject();

        //TODO 2.提取数据库名
        String topic = sourceRecord.topic();
        String[] split = topic.split("\\.");
        String database = split[1];

        //TODO 3.提取表名
        String tableName = split[2];


        Struct value = (Struct) sourceRecord.value();
        //TODO 4.获取after数据
        Struct valueStruct = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
        //判断是否有after
        if (valueStruct!=null){
            List<Field> fields = valueStruct.schema().fields();
            for (Field field : fields) {
                afterJson.put(field.name(), valueStruct.get(field));
            }
        }

        //TODO 5.获取before数据
        Struct beforeStruct = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        //判断是否有after
        if (beforeStruct!=null){
            List<Field> fields = beforeStruct.schema().fields();
            for (Field field : fields) {
                beforeJson.put(field.name(), beforeStruct.get(field));
            }
        }

        //TODO 6.获取操作类型DELETE UPDATE CREATE
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);

        String type = operation.toString().toLowerCase();
        if ("create".equals(type)){
            type = "insert";
        }

        //TODO 7.封住数据
        jsonObject.put("database", database);
        jsonObject.put("tableName", tableName);
        jsonObject.put("after", afterJson);
        jsonObject.put("before", beforeJson);
        jsonObject.put("type", type);


        collector.collect(jsonObject.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
