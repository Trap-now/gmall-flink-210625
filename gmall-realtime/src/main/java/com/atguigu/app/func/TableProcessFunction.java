package com.atguigu.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import com.atguigu.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private Connection connection;

    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    private OutputTag<JSONObject> objectOutputTag;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor, OutputTag<JSONObject> outputTag) {
        this.mapStateDescriptor = mapStateDescriptor;
        this.objectOutputTag = outputTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    //value:{"db":"","tn":"table_process","before":{},"after":{"source_table":""....},"type":""}
    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {

        //1.解析数据为JavaBean
        JSONObject jsonObject = JSON.parseObject(value);
        TableProcess tableProcess = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);

        //2.校验HBase表是否存在,如果不存在则建表
        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
            checkTable(tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend());
        }

        //3.将数据写入状态,广播出去
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();
        broadcastState.put(key, tableProcess);
    }

    //create table if not exists db.tn(id varchar primary key,tm_name varchar) sinkExtend
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {

        PreparedStatement preparedStatement = null;

        try {
            if (sinkExtend == null) {
                sinkExtend = "";
            }

            if (sinkPk == null || sinkPk.equals("")) {
                sinkPk = "id";
            }

            StringBuilder createTableSql = new StringBuilder("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");

            String[] columns = sinkColumns.split(",");
            for (int i = 0; i < columns.length; i++) {

                //取出列名
                String column = columns[i];
                createTableSql.append(column).append(" varchar");

                //判断是否为主键
                if (column.equals(sinkPk)) {
                    createTableSql.append(" primary key");
                }

                //判断当前如果不是最后一个字段,则添加","
                if (i < columns.length - 1) {
                    createTableSql.append(",");
                }
            }

            createTableSql.append(")").append(sinkExtend);

            //打印建表语句
            System.out.println(createTableSql);

            //执行建表操作
            preparedStatement = connection.prepareStatement(createTableSql.toString());
            preparedStatement.execute();

        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("建表:" + sinkTable + "失败");
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //value:{"db":"","tableName":"base_trademark","before":{},"after":{"id":"","tm_name":"","logo_url":""},"type":""}
    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {

        //1.根据主键读取广播状态对应的数据
        String key = value.getString("tableName") + "-" + value.getString("type");
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        TableProcess tableProcess = broadcastState.get(key);

        if (tableProcess != null) {

            //2.根据广播状态数据  过滤字段
            filterColumns(value.getJSONObject("after"), tableProcess.getSinkColumns());

            //3.根据广播状态数据  分流   主流：Kafka   侧输出流：HBase
            //value.put("sinkTable", tableProcess.getSinkTable());

            if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
                ctx.output(objectOutputTag, value);
            } else if (TableProcess.SINK_TYPE_KAFKA.equals(tableProcess.getSinkType())) {
                out.collect(value);
            }

        } else {
            System.out.println(key + "不存在！");
        }
    }

    //after:{"id":"","tm_name":"","logo_url":"","name":""}  sinkColumns:id,tm_name
    //{"id":"","tm_name":""}
    private void filterColumns(JSONObject after, String sinkColumns) {

        String[] columns = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(columns);

//        Set<Map.Entry<String, Object>> entries = after.entrySet();
//        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
//        while (iterator.hasNext()) {
//            Map.Entry<String, Object> next = iterator.next();
//            if (!columnList.contains(next.getKey())) {
//                iterator.remove();
//            }
//        }

        Set<Map.Entry<String, Object>> entries = after.entrySet();
        entries.removeIf(next -> !columnList.contains(next.getKey()));

    }
}