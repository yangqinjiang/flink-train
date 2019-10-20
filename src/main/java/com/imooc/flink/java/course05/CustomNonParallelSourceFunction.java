package com.imooc.flink.java.course05;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
/**
 * 自定义SourceFunction,不能并行
 * 每隔1s,产生一个自增的数字
 */
public class CustomNonParallelSourceFunction implements SourceFunction<Long> {
    Long count = 1L;
    boolean isRunning = true;
    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while (true){
            ctx.collect(count);
            count ++;
            Thread.sleep(1000); // 睡眠1s,
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
