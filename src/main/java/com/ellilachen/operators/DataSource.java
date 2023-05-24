package com.ellilachen.operators;

import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.concurrent.TimeUnit;

/**
 * Project name: CquirrelDemo
 * Class name：CustomSource
 * Description：TODO
 * Create time：2023/2/10 19:04
 * Creator：ellilachen
 */
public class DataSource implements SourceFunction<String>  {
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        long startTime = System.currentTimeMillis();
        try (BufferedReader reader = new BufferedReader(new FileReader("data/input_sf_1.txt"))) {
            String line = reader.readLine();
            while (line != null) {
                if (line.equals("Bye") || line.equals("End")) {
                    long endTime = System.currentTimeMillis();
                    System.out.println("===== Period Duration: " + (endTime - startTime) + "ms ======");
                    if (line.equals("Bye")) {
                        TimeUnit.SECONDS.sleep(5);
                        startTime = System.currentTimeMillis();
                    }
                } else {
                    ctx.collect(line);
                }
                line = reader.readLine();
            }
        }
    }

    @Override
    public void cancel() {

    }
}
