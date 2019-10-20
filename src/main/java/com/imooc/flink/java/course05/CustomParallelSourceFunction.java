package com.imooc.flink.java.course05;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/**
 * 自定义SourceFunction,支持并行
 * 每隔1s,产生一个自增的数字
 */
public class CustomParallelSourceFunction implements ParallelSourceFunction<Long> {
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
