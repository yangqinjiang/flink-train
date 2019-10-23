package com.imooc.flink.java.project;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

/**
 * 自定义SourceFunction,支持并行
 * 每隔1s,产生一个一条日志
 */
public class LogSourceFunction implements ParallelSourceFunction<String> {

    boolean isRunning = true;
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (isRunning){
            StringBuilder builder = new StringBuilder();
            builder.append("imooc").append("\t");
            builder.append("CN").append("\t");
            builder.append(getLevels()).append("\t");
            builder.append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())).append("\t");
            builder.append(getIps()).append("\t");
            builder.append(getDomain()).append("\t");
            builder.append(getTraffic()).append("\t");
            //System.out.println(builder.toString());
            ctx.collect(builder.toString());
            Thread.sleep(1000); // 睡眠1s,
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }


    //流量
    public  int getTraffic(){
        return new Random().nextInt(10000);
    }
    //域名
    public  String getDomain(){
        String[]  domain = new String[]{
                "v1.go2yd.com",
                "v2.go2yd.com",
                "v3.go2yd.com",
                "v4.go2yd.com",
                "vmi.go2yd.com",
        };
        return domain[new Random().nextInt(domain.length)];

    }
    //生产ip数据
    public  String getIps(){
        String[]  ips = new String[]{
                "223.104.18.110",
                "113.101.75.194",
                "27.17.127.135",
                "183.225.139.16",
                "112.1.66.34",
                "175.148.211.190",
                "183.227.58.21",
                "59.83.198.84",
                "117.28.38.28",
                "117.59.39.169"
        };
        return ips[new Random().nextInt(ips.length)];
    }
    //生产level数据
    public  String getLevels(){
        String[]  levels = new String[]{"M","E"};
        return levels[new Random().nextInt(levels.length)];
    }
}
