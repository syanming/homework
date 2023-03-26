package org.example;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.entity.TempLogData;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 页面浏览数据统计
 *
 * @Author ly
 * @Date 2023/03/26,0026,周日 下午 08:22
 */
public class PageViewStatistics {
    public static void main(String[] args) {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(executionEnvironment);
        WatermarkStrategy<TempLogData> logDataWatermarkStrategy = WatermarkStrategy.<TempLogData>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(new SerializableTimestampAssigner<TempLogData>() {

            private static final long serialVersionUID = -4201102779458806547L;

            @Override
            public long extractTimestamp(TempLogData element, long recordTimestamp) {
                return element.getEventTime() * 1000;
            }
        });
        DataStream<TempLogData> dataStream = executionEnvironment.socketTextStream("localhost", 8888).map(event -> {
            String[] split = event.split(" ");
            return TempLogData.builder().ip(split[0]).userId(split[1]).eventTime(Long.valueOf(split[2])).method(split[3]).url(split[4]).build();
        }).assignTimestampsAndWatermarks(logDataWatermarkStrategy);
        Table table = streamTableEnvironment.fromDataStream(dataStream, $("ip"), $("userIid"), $("eventTime"), $("method"), $("url"), $("swEnvTime").rowtime());
        Table tableQuery = streamTableEnvironment.sqlQuery("select count(url), url , HOP_START(swEnvTime,INTERVAL '5' second,INTERVAL '10' minute) as winstart " +
                "from " + table + " GROUP BY url,HOP(swEnvTime,INTERVAL '5' second,INTERVAL '10' minute) ");

        tableQuery.execute().print();

    }
}
