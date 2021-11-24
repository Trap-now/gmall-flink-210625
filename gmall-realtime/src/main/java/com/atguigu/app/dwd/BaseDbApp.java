package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.app.func.MyFlinkCDCDeSer;
import com.atguigu.app.func.TableProcessFunction;
import com.atguigu.bean.TableProcess;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class BaseDbApp {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境设置跟Kafka的分区数保持一致

        //设置状态后端
        //env.setStateBackend(new FsStateBackend(""));
        //开启CK
        //env.enableCheckpointing(5000); //生产环境设置分钟级
        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //env.getCheckpointConfig().setCheckpointTimeout(10000);
        //env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        //TODO 2.读取Kafka ods_base_db 主题创建主流
        DataStreamSource<String> dataStream = env.addSource(MyKafkaUtil.getKafkaSource("ods_base_db", "base_db_app_210625"));

        //TODO 3.将每行数据转换为JSON对象  过滤脏数据
        SingleOutputStreamOperator<JSONObject> jsonObjDS = dataStream.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println(value);
                }
            }
        });

        //TODO 4.过滤删除数据
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return !"delete".equals(value.getString("type"));
            }
        });

        //TODO 5.使用FlinkCDC读取MySQL配置表并创建广播流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("gmall-210625-realtime")
                .tableList("gmall-210625-realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyFlinkCDCDeSer())
                .build();
        DataStreamSource<String> cdcDS = env.addSource(sourceFunction);
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = cdcDS.broadcast(mapStateDescriptor);

        //TODO 6.连接主流与广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = filterDS.connect(broadcastStream);

        //TODO 7.根据配置流信息处理主流数据（分流）
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbase") {
        };
        SingleOutputStreamOperator<JSONObject> kafkaDS = connectedStream.process(new TableProcessFunction(mapStateDescriptor, hbaseTag));

        //TODO 8.将HBase数据写出
        DataStream<JSONObject> hbaseDS = kafkaDS.getSideOutput(hbaseTag);
        hbaseDS.print("HBase>>>>>");

        //TODO 9.将Kafka数据写出
        kafkaDS.print("Kafka>>>>>");

        //TODO 10.启动任务
        env.execute("BaseDbApp");

    }

}
