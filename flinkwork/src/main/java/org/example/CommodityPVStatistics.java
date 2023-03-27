package org.example;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.entity.TempBehaviorData;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 热门商品PV统计
 *
 * @author ly
 * @date 2023/03/26,0026,星期日,下午 08:14
 */
public class CommodityPVStatistics {
    public static void main(String[] args) {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment);

        WatermarkStrategy<TempBehaviorData> behaviorDataWatermarkStrategy = WatermarkStrategy.<TempBehaviorData>forBoundedOutOfOrderness(Duration.ofMinutes(5)).withTimestampAssigner(new SerializableTimestampAssigner<TempBehaviorData>() {
            private static final long serialVersionUID = -4249051929292267728L;

            @Override
            public long extractTimestamp(TempBehaviorData element, long recordTimestamp) {
                return element.getTimestamp() * 1000;
            }

        });

        DataStream<TempBehaviorData> dataStream = executionEnvironment.socketTextStream("localhost", 9999).map(event -> {

            String[] strings = event.split(" ");
            return TempBehaviorData.builder().userId(strings[0]).itemId(strings[1]).categoryId(strings[2]).behavior(strings[3]).timestamp(Long.valueOf(strings[4])).build();
        }).assignTimestampsAndWatermarks(behaviorDataWatermarkStrategy);
        Table table = tableEnvironment.fromDataStream(dataStream, $("userId"), $("itemId"), $("categoryId"), $("behavior"), $("timestamp"), $("swEnvTime").rowtime());
        Table tableQuery = tableEnvironment.sqlQuery("select * from ( select *, ROW_NUMBER() OVER ( PARTITION BY winstart, winend ORDER BY itemId desc ) as rownum from ( select count(url), winstart, winsend, count(itemId), itemId , HOP_START(swEnvTime,INTERVAL '5' minute,INTERVAL '1' hour) as winstart from " + table + " where behavior='pv' GROUP BY itemId,HOP(swEnvTime,INTERVAL '5' minute,INTERVAL '1' hour) ) ) where rownum <= 3");
        tableQuery.execute().print();
    }
}