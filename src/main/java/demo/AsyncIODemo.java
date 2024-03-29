package demo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * <p>
 *
 * @author Guan Peixiang (guanpeixiang@juzishuke.com)
 * @date 2021/10/26
 */
public class AsyncIODemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final int maxCount = 6;
        final int taskNum = 1;
        final long timeout = 4000;

        DataStream<Integer> inputStream = env.addSource(new SimpleSource(maxCount));
        AsyncFunction<Integer, String> function = new SampleAsyncFunction();

        DataStream<String> result = AsyncDataStream
                        .unorderedWait(inputStream, function, timeout, TimeUnit.MILLISECONDS, 10)
                        .setParallelism(taskNum);

        result.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value + "," + System.currentTimeMillis();
            }
        }).print();

        env.execute("Async IO Demo");
    }

    private static class SimpleSource implements SourceFunction<Integer> {
        private volatile boolean isRunning = true;
        private int counter = 0;
        private int start = 0;

        public SimpleSource(int maxNum) {
            this.counter = maxNum;
        }

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while ((start < counter || counter == -1) && isRunning) {
                synchronized (ctx.getCheckpointLock()) {
                    System.out.println("send data:" + start);
                    ctx.collect(start);
                    ++start;
                }
                Thread.sleep(10L);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}

class SampleAsyncFunction extends RichAsyncFunction<Integer, String> {
    private long[] sleep = { 100L, 1000L, 5000L, 2000L, 6000L, 100L };

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void asyncInvoke(final Integer input, final ResultFuture<String> resultFuture) {
        System.out.println(System.currentTimeMillis() + "-input:" + input + " will sleep " + sleep[input] + " ms");

        //        query(input, resultFuture);
        asyncQuery(input, resultFuture);
    }

    private void query(final Integer input, final ResultFuture<String> resultFuture) {
        try {
            Thread.sleep(sleep[input]);
            resultFuture.complete(Collections.singletonList(String.valueOf(input)));
        } catch (InterruptedException e) {
            resultFuture.complete(new ArrayList<>(0));
        }
    }

    private void asyncQuery(final Integer input, final ResultFuture<String> resultFuture) {
        CompletableFuture.supplyAsync(new Supplier<Integer>() {

            @Override
            public Integer get() {
                try {
                    Thread.sleep(sleep[input]);
                    return input;
                } catch (Exception e) {
                    return null;
                }
            }
        }).thenAccept((Integer dbResult) -> {
            resultFuture.complete(Collections.singleton(String.valueOf(dbResult)));
        });
    }
}