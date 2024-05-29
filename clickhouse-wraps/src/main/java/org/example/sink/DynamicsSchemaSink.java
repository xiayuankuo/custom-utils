package org.example.sink;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.example.connector.DynamicsSchemaOutputFormat;

/**
 * @author yuankuo.xia
 * @Date 2024/5/27
 */
public class DynamicsSchemaSink extends RichSinkFunction<JSONObject> implements CheckpointedFunction {

    private final DynamicsSchemaOutputFormat outputFormat;

    public DynamicsSchemaSink(DynamicsSchemaOutputFormat outputFormat){
        this.outputFormat = outputFormat;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.outputFormat.open(getRuntimeContext().getMaxNumberOfParallelSubtasks(), getIterationRuntimeContext().getIndexOfThisSubtask());
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        outputFormat.writeRecord(value);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        outputFormat.checkpointFlushAll();
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        // AT-LEAST-ONCE sink端不保存状态
    }
}
