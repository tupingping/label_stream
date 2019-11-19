package com.sinaif.stream.common.utils;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.sinaif.stream.common.utils.ConfigRespostory.CommonConfig;

/**
 * 
 * create env
 * 
 * @author simonzhang
 *
 */
public class ExecutionEnvUtil {
    public static ParameterTool createParameterTool() throws Exception {
        return ParameterTool
                .fromPropertiesFile(ConfigRespostory.CONFIG_INPUT_STREAM)
                .mergeWith(ParameterTool.fromSystemProperties())
                .mergeWith(ParameterTool.fromMap(getenv()));
    }


    public static StreamExecutionEnvironment prepare(ParameterTool parameterTool) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parameterTool.getInt(CommonConfig.STREAM_PARALLELISM, 5));
        env.getConfig().disableSysoutLogging();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        if (parameterTool.getBoolean(CommonConfig.STREAM_CHECKPOINT_ENABLE, true)) {
            env.enableCheckpointing(parameterTool.getInt(CommonConfig.STREAM_CHECKPOINT_INTERVAL, 1000)); // create a checkpoint every 5 seconds
        }
        env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        return env;
    }

    private static Map<String, String> getenv() {
        Map<String, String> map = new HashMap<>();
        for (Map.Entry<String, String> entry : System.getenv().entrySet()) {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }
    
    
}
