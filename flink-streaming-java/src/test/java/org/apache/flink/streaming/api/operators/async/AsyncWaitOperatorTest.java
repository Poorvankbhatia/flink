/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators.async;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.ExpectedTestException;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.AsyncRetryStrategy;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.async.queue.StreamElementQueue;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTaskTestHarness;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarness;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarnessBuilder;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.streaming.util.retryable.AsyncRetryStrategies;
import org.apache.flink.streaming.util.retryable.RetryPredicates;
import org.apache.flink.testutils.junit.SharedObjectsExtension;
import org.apache.flink.testutils.junit.SharedReference;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava33.com.google.common.collect.Lists;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.util.retryable.AsyncRetryStrategies.NO_RETRY_STRATEGY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link AsyncWaitOperator}. These test that:
 *
 * <ul>
 *   <li>Process StreamRecords and Watermarks in ORDERED mode
 *   <li>Process StreamRecords and Watermarks in UNORDERED mode
 *   <li>Process StreamRecords with retry
 *   <li>AsyncWaitOperator in operator chain
 *   <li>Snapshot state and restore state
 * </ul>
 */
@Timeout(value = 100, unit = TimeUnit.SECONDS)
public class AsyncWaitOperatorTest {
    private static final long TIMEOUT = 1000L;

    @RegisterExtension
    private final SharedObjectsExtension sharedObjects = SharedObjectsExtension.create();

    private static final AsyncRetryStrategy emptyResultFixedDelayRetryStrategy =
            new AsyncRetryStrategies.FixedDelayRetryStrategyBuilder(2, 10L)
                    .ifResult(RetryPredicates.EMPTY_RESULT_PREDICATE)
                    .build();

    private static final AsyncRetryStrategy exceptionRetryStrategy =
            new AsyncRetryStrategies.FixedDelayRetryStrategyBuilder(2, 10L)
                    .ifException(RetryPredicates.HAS_EXCEPTION_PREDICATE)
                    .build();

    private abstract static class MyAbstractAsyncFunction<IN>
            extends RichAsyncFunction<IN, Integer> {
        private static final long serialVersionUID = 8522411971886428444L;

        private static final long TERMINATION_TIMEOUT = 5000L;
        private static final int THREAD_POOL_SIZE = 10;

        static ExecutorService executorService;
        static int counter = 0;

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);

            synchronized (MyAbstractAsyncFunction.class) {
                if (counter == 0) {
                    executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
                }

                ++counter;
            }
        }

        @Override
        public void close() throws Exception {
            super.close();

            freeExecutor();
        }

        private void freeExecutor() {
            synchronized (MyAbstractAsyncFunction.class) {
                --counter;

                if (counter == 0) {
                    executorService.shutdown();

                    try {
                        if (!executorService.awaitTermination(
                                TERMINATION_TIMEOUT, TimeUnit.MILLISECONDS)) {
                            executorService.shutdownNow();
                        }
                    } catch (InterruptedException interrupted) {
                        executorService.shutdownNow();

                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
    }

    public static class MyAsyncFunction extends MyAbstractAsyncFunction<Integer> {
        private static final long serialVersionUID = -1504699677704123889L;

        @Override
        public void asyncInvoke(final Integer input, final ResultFuture<Integer> resultFuture)
                throws Exception {
            executorService.submit(
                    new Runnable() {
                        @Override
                        public void run() {
                            resultFuture.complete(Collections.singletonList(input * 2));
                        }
                    });
        }
    }

    /**
     * A special {@link AsyncFunction} without issuing {@link ResultFuture#complete} until the latch
     * counts to zero. {@link ResultFuture#complete} until the latch counts to zero. This function
     * is used in the testStateSnapshotAndRestore, ensuring that {@link StreamElement} can stay in
     * the {@link StreamElementQueue} to be snapshotted while checkpointing.
     */
    public static class LazyAsyncFunction extends MyAsyncFunction {
        private static final long serialVersionUID = 3537791752703154670L;

        private final transient CountDownLatch latch;

        public LazyAsyncFunction() {
            latch = new CountDownLatch(1);
        }

        @Override
        public void asyncInvoke(final Integer input, final ResultFuture<Integer> resultFuture)
                throws Exception {
            executorService.submit(
                    () -> {
                        try {
                            waitLatch();
                        } catch (InterruptedException e) {
                            // do nothing
                        }

                        resultFuture.complete(Collections.singletonList(input));
                    });
        }

        protected void waitLatch() throws InterruptedException {
            latch.await();
        }

        public void countDown() {
            latch.countDown();
        }
    }

    private static class InputReusedAsyncFunction extends MyAbstractAsyncFunction<Tuple1<Integer>> {

        private static final long serialVersionUID = 8627909616410487720L;

        @Override
        public void asyncInvoke(Tuple1<Integer> input, ResultFuture<Integer> resultFuture)
                throws Exception {
            executorService.submit(
                    new Runnable() {
                        @Override
                        public void run() {
                            resultFuture.complete(Collections.singletonList(input.f0 * 2));
                        }
                    });
        }
    }

    /**
     * A special {@link LazyAsyncFunction} for timeout handling. Complete the result future with 3
     * times the input when the timeout occurred.
     */
    public static class IgnoreTimeoutLazyAsyncFunction extends LazyAsyncFunction {
        private static final long serialVersionUID = 1428714561365346128L;

        @Override
        public void timeout(Integer input, ResultFuture<Integer> resultFuture) throws Exception {
            resultFuture.complete(Collections.singletonList(input * 3));
        }
    }

    /** Completes input at half the TIMEOUT and registers timeouts. */
    private static class TimeoutAfterCompletionTestFunction
            implements AsyncFunction<Integer, Integer> {
        static final AtomicBoolean TIMED_OUT = new AtomicBoolean(false);
        static final CountDownLatch COMPLETION_TRIGGER = new CountDownLatch(1);

        @Override
        public void asyncInvoke(Integer input, ResultFuture<Integer> resultFuture) {
            ForkJoinPool.commonPool()
                    .submit(
                            () -> {
                                COMPLETION_TRIGGER.await();
                                resultFuture.complete(Collections.singletonList(input));
                                return null;
                            });
        }

        @Override
        public void timeout(Integer input, ResultFuture<Integer> resultFuture) {
            TIMED_OUT.set(true);
        }
    }

    private static class OddInputEmptyResultAsyncFunction extends MyAbstractAsyncFunction<Integer> {
        private static final long serialVersionUID = 1L;

        @Override
        public void asyncInvoke(final Integer input, final ResultFuture<Integer> resultFuture)
                throws Exception {
            executorService.submit(
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                Thread.sleep(3);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            if (input % 2 == 1) {
                                resultFuture.complete(Collections.EMPTY_LIST);
                            } else {
                                resultFuture.complete(Collections.singletonList(input * 2));
                            }
                        }
                    });
        }
    }

    private static class IllWrittenOddInputEmptyResultAsyncFunction
            extends MyAbstractAsyncFunction<Integer> {
        private static final long serialVersionUID = 1L;

        @Override
        public void asyncInvoke(final Integer input, final ResultFuture<Integer> resultFuture)
                throws Exception {
            executorService.submit(
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                Thread.sleep(3);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            if (input % 2 == 1) {
                                // repeated calling complete
                                for (int i = 0; i < 10; i++) {
                                    resultFuture.complete(Collections.EMPTY_LIST);
                                }
                            } else {
                                resultFuture.complete(Collections.singletonList(input * 2));
                            }
                        }
                    });
        }
    }

    /** A {@link Comparator} to compare {@link StreamRecord} while sorting them. */
    private class StreamRecordComparator implements Comparator<Object> {
        @Override
        public int compare(Object o1, Object o2) {
            if (o1 instanceof Watermark || o2 instanceof Watermark) {
                return 0;
            } else {
                StreamRecord<Integer> sr0 = (StreamRecord<Integer>) o1;
                StreamRecord<Integer> sr1 = (StreamRecord<Integer>) o2;

                if (sr0.getTimestamp() != sr1.getTimestamp()) {
                    return (int) (sr0.getTimestamp() - sr1.getTimestamp());
                }

                int comparison = sr0.getValue().compareTo(sr1.getValue());
                if (comparison != 0) {
                    return comparison;
                } else {
                    return sr0.getValue() - sr1.getValue();
                }
            }
        }
    }

    /** Test the AsyncWaitOperator with ordered mode and event time. */
    @Test
    void testEventTimeOrdered() throws Exception {
        testEventTime(AsyncDataStream.OutputMode.ORDERED);
    }

    /** Test the AsyncWaitOperator with unordered mode and event time. */
    @Test
    void testWaterMarkUnordered() throws Exception {
        testEventTime(AsyncDataStream.OutputMode.UNORDERED);
    }

    private void testEventTime(AsyncDataStream.OutputMode mode) throws Exception {
        final OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                createTestHarness(new MyAsyncFunction(), TIMEOUT, 2, mode);

        final long initialTime = 0L;
        final ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        synchronized (testHarness.getCheckpointLock()) {
            testHarness.processElement(new StreamRecord<>(1, initialTime + 1));
            testHarness.processElement(new StreamRecord<>(2, initialTime + 2));
            testHarness.processWatermark(new Watermark(initialTime + 2));
            testHarness.processElement(new StreamRecord<>(3, initialTime + 3));
        }

        // wait until all async collectors in the buffer have been emitted out.
        synchronized (testHarness.getCheckpointLock()) {
            testHarness.endInput();
            testHarness.close();
        }

        expectedOutput.add(new StreamRecord<>(2, initialTime + 1));
        expectedOutput.add(new StreamRecord<>(4, initialTime + 2));
        expectedOutput.add(new Watermark(initialTime + 2));
        expectedOutput.add(new StreamRecord<>(6, initialTime + 3));

        if (AsyncDataStream.OutputMode.ORDERED == mode) {
            TestHarnessUtil.assertOutputEquals(
                    "Output with watermark was not correct.",
                    expectedOutput,
                    testHarness.getOutput());
        } else {
            Object[] jobOutputQueue = testHarness.getOutput().toArray();

            assertThat(jobOutputQueue[2])
                    .as("Watermark should be at index 2")
                    .isEqualTo(new Watermark(initialTime + 2));
            assertThat(jobOutputQueue[3])
                    .as("StreamRecord 3 should be at the end")
                    .isEqualTo(new StreamRecord<>(6, initialTime + 3));

            TestHarnessUtil.assertOutputEqualsSorted(
                    "Output for StreamRecords does not match",
                    expectedOutput,
                    testHarness.getOutput(),
                    new StreamRecordComparator());
        }
    }

    /** Test the AsyncWaitOperator with ordered mode and processing time. */
    @Test
    void testProcessingTimeOrdered() throws Exception {
        testProcessingTime(AsyncDataStream.OutputMode.ORDERED);
    }

    /** Test the AsyncWaitOperator with unordered mode and processing time. */
    @Test
    void testProcessingUnordered() throws Exception {
        testProcessingTime(AsyncDataStream.OutputMode.UNORDERED);
    }

    private void testProcessingTime(AsyncDataStream.OutputMode mode) throws Exception {
        final OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                createTestHarness(new MyAsyncFunction(), TIMEOUT, 6, mode);

        final long initialTime = 0L;
        final Queue<Object> expectedOutput = new ArrayDeque<>();

        testHarness.open();

        synchronized (testHarness.getCheckpointLock()) {
            testHarness.processElement(new StreamRecord<>(1, initialTime + 1));
            testHarness.processElement(new StreamRecord<>(2, initialTime + 2));
            testHarness.processElement(new StreamRecord<>(3, initialTime + 3));
            testHarness.processElement(new StreamRecord<>(4, initialTime + 4));
            testHarness.processElement(new StreamRecord<>(5, initialTime + 5));
            testHarness.processElement(new StreamRecord<>(6, initialTime + 6));
            testHarness.processElement(new StreamRecord<>(7, initialTime + 7));
            testHarness.processElement(new StreamRecord<>(8, initialTime + 8));
        }

        expectedOutput.add(new StreamRecord<>(2, initialTime + 1));
        expectedOutput.add(new StreamRecord<>(4, initialTime + 2));
        expectedOutput.add(new StreamRecord<>(6, initialTime + 3));
        expectedOutput.add(new StreamRecord<>(8, initialTime + 4));
        expectedOutput.add(new StreamRecord<>(10, initialTime + 5));
        expectedOutput.add(new StreamRecord<>(12, initialTime + 6));
        expectedOutput.add(new StreamRecord<>(14, initialTime + 7));
        expectedOutput.add(new StreamRecord<>(16, initialTime + 8));

        synchronized (testHarness.getCheckpointLock()) {
            testHarness.endInput();
            testHarness.close();
        }

        if (mode == AsyncDataStream.OutputMode.ORDERED) {
            TestHarnessUtil.assertOutputEquals(
                    "ORDERED Output was not correct.", expectedOutput, testHarness.getOutput());
        } else {
            TestHarnessUtil.assertOutputEqualsSorted(
                    "UNORDERED Output was not correct.",
                    expectedOutput,
                    testHarness.getOutput(),
                    new StreamRecordComparator());
        }
    }

    /** Tests that the AsyncWaitOperator works together with chaining. */
    @Test
    void testOperatorChainWithProcessingTime() throws Exception {

        JobVertex chainedVertex = createChainedVertex(new MyAsyncFunction(), new MyAsyncFunction());

        final OneInputStreamTaskTestHarness<Integer, Integer> testHarness =
                new OneInputStreamTaskTestHarness<>(
                        OneInputStreamTask::new,
                        1,
                        1,
                        BasicTypeInfo.INT_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO);
        testHarness.setupOutputForSingletonOperatorChain();

        testHarness.taskConfig = chainedVertex.getConfiguration();

        final StreamConfig streamConfig = testHarness.getStreamConfig();
        final StreamConfig operatorChainStreamConfig =
                new StreamConfig(chainedVertex.getConfiguration());
        streamConfig.setStreamOperatorFactory(
                operatorChainStreamConfig.getStreamOperatorFactory(
                        AsyncWaitOperatorTest.class.getClassLoader()));

        testHarness.invoke();
        testHarness.waitForTaskRunning();

        long initialTimestamp = 0L;

        testHarness.processElement(new StreamRecord<>(5, initialTimestamp));
        testHarness.processElement(new StreamRecord<>(6, initialTimestamp + 1L));
        testHarness.processElement(new StreamRecord<>(7, initialTimestamp + 2L));
        testHarness.processElement(new StreamRecord<>(8, initialTimestamp + 3L));
        testHarness.processElement(new StreamRecord<>(9, initialTimestamp + 4L));

        testHarness.endInput();
        testHarness.waitForTaskCompletion();

        List<Object> expectedOutput = new LinkedList<>();
        expectedOutput.add(new StreamRecord<>(22, initialTimestamp));
        expectedOutput.add(new StreamRecord<>(26, initialTimestamp + 1L));
        expectedOutput.add(new StreamRecord<>(30, initialTimestamp + 2L));
        expectedOutput.add(new StreamRecord<>(34, initialTimestamp + 3L));
        expectedOutput.add(new StreamRecord<>(38, initialTimestamp + 4L));

        TestHarnessUtil.assertOutputEqualsSorted(
                "Test for chained operator with AsyncWaitOperator failed",
                expectedOutput,
                testHarness.getOutput(),
                new StreamRecordComparator());
    }

    private JobVertex createChainedVertex(
            AsyncFunction<Integer, Integer> firstFunction,
            AsyncFunction<Integer, Integer> secondFunction) {

        StreamExecutionEnvironment chainEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        // set parallelism to 2 to avoid chaining with source in case when available processors is
        // 1.
        chainEnv.setParallelism(2);

        // the input is only used to construct a chained operator, and they will not be used in the
        // real tests.
        DataStream<Integer> input = chainEnv.fromData(1, 2, 3);

        input =
                AsyncDataStream.orderedWait(
                        input, firstFunction, TIMEOUT, TimeUnit.MILLISECONDS, 6);

        // the map function is designed to chain after async function. we place an Integer object in
        // it and
        // it is initialized in the open() method.
        // it is used to verify that operators in the operator chain should be opened from the tail
        // to the head,
        // so the result from AsyncWaitOperator can pass down successfully and correctly.
        // if not, the test can not be passed.
        input =
                input.map(
                        new RichMapFunction<Integer, Integer>() {
                            private static final long serialVersionUID = 1L;

                            private Integer initialValue = null;

                            @Override
                            public void open(OpenContext openContext) throws Exception {
                                initialValue = 1;
                            }

                            @Override
                            public Integer map(Integer value) throws Exception {
                                return initialValue + value;
                            }
                        });

        input =
                AsyncDataStream.unorderedWait(
                        input, secondFunction, TIMEOUT, TimeUnit.MILLISECONDS, 3);

        input.map(
                        new MapFunction<Integer, Integer>() {
                            private static final long serialVersionUID = 5162085254238405527L;

                            @Override
                            public Integer map(Integer value) throws Exception {
                                return value;
                            }
                        })
                .startNewChain()
                .sinkTo(new DiscardingSink<>());

        // be build our own OperatorChain
        final JobGraph jobGraph = chainEnv.getStreamGraph().getJobGraph();

        assertThat(jobGraph.getVerticesSortedTopologicallyFromSources()).hasSize(3);

        return jobGraph.getVerticesSortedTopologicallyFromSources().get(1);
    }

    @Test
    void testStateSnapshotAndRestore() throws Exception {
        final OneInputStreamTaskTestHarness<Integer, Integer> testHarness =
                new OneInputStreamTaskTestHarness<>(
                        OneInputStreamTask::new,
                        1,
                        1,
                        BasicTypeInfo.INT_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO);

        testHarness.setupOutputForSingletonOperatorChain();

        LazyAsyncFunction lazyAsyncFunction = new LazyAsyncFunction();

        AsyncWaitOperatorFactory<Integer, Integer> factory =
                new AsyncWaitOperatorFactory<>(
                        lazyAsyncFunction, TIMEOUT, 4, AsyncDataStream.OutputMode.ORDERED);

        final StreamConfig streamConfig = testHarness.getStreamConfig();
        OperatorID operatorID = new OperatorID(42L, 4711L);
        streamConfig.setStreamOperatorFactory(factory);
        streamConfig.setOperatorID(operatorID);

        final TestTaskStateManager taskStateManagerMock = testHarness.getTaskStateManager();

        testHarness.invoke();
        testHarness.waitForTaskRunning();

        final OneInputStreamTask<Integer, Integer> task = testHarness.getTask();

        final long initialTime = 0L;

        testHarness.processElement(new StreamRecord<>(1, initialTime + 1));
        testHarness.processElement(new StreamRecord<>(2, initialTime + 2));
        testHarness.processElement(new StreamRecord<>(3, initialTime + 3));
        testHarness.processElement(new StreamRecord<>(4, initialTime + 4));

        testHarness.waitForInputProcessing();

        final long checkpointId = 1L;
        final long checkpointTimestamp = 1L;

        final CheckpointMetaData checkpointMetaData =
                new CheckpointMetaData(checkpointId, checkpointTimestamp);

        task.triggerCheckpointAsync(
                checkpointMetaData, CheckpointOptions.forCheckpointWithDefaultLocation());

        taskStateManagerMock.getWaitForReportLatch().await();

        assertThat(taskStateManagerMock.getReportedCheckpointId()).isEqualTo(checkpointId);

        lazyAsyncFunction.countDown();

        testHarness.endInput();
        testHarness.waitForTaskCompletion();

        // set the operator state from previous attempt into the restored one
        TaskStateSnapshot subtaskStates = taskStateManagerMock.getLastJobManagerTaskStateSnapshot();

        final OneInputStreamTaskTestHarness<Integer, Integer> restoredTaskHarness =
                new OneInputStreamTaskTestHarness<>(
                        OneInputStreamTask::new,
                        BasicTypeInfo.INT_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO);

        restoredTaskHarness.setTaskStateSnapshot(checkpointId, subtaskStates);
        restoredTaskHarness.setupOutputForSingletonOperatorChain();

        AsyncWaitOperatorFactory<Integer, Integer> restoredOperator =
                new AsyncWaitOperatorFactory<>(
                        new MyAsyncFunction(), TIMEOUT, 6, AsyncDataStream.OutputMode.ORDERED);

        restoredTaskHarness.getStreamConfig().setStreamOperatorFactory(restoredOperator);
        restoredTaskHarness.getStreamConfig().setOperatorID(operatorID);

        restoredTaskHarness.invoke();
        restoredTaskHarness.waitForTaskRunning();

        final OneInputStreamTask<Integer, Integer> restoredTask = restoredTaskHarness.getTask();

        restoredTaskHarness.processElement(new StreamRecord<>(5, initialTime + 5));
        restoredTaskHarness.processElement(new StreamRecord<>(6, initialTime + 6));
        restoredTaskHarness.processElement(new StreamRecord<>(7, initialTime + 7));

        // trigger the checkpoint while processing stream elements
        restoredTask
                .triggerCheckpointAsync(
                        new CheckpointMetaData(checkpointId, checkpointTimestamp),
                        CheckpointOptions.forCheckpointWithDefaultLocation())
                .get();

        restoredTaskHarness.processElement(new StreamRecord<>(8, initialTime + 8));

        restoredTaskHarness.endInput();
        restoredTaskHarness.waitForTaskCompletion();

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        expectedOutput.add(new StreamRecord<>(2, initialTime + 1));
        expectedOutput.add(new StreamRecord<>(4, initialTime + 2));
        expectedOutput.add(new StreamRecord<>(6, initialTime + 3));
        expectedOutput.add(new StreamRecord<>(8, initialTime + 4));
        expectedOutput.add(new StreamRecord<>(10, initialTime + 5));
        expectedOutput.add(new StreamRecord<>(12, initialTime + 6));
        expectedOutput.add(new StreamRecord<>(14, initialTime + 7));
        expectedOutput.add(new StreamRecord<>(16, initialTime + 8));

        // remove CheckpointBarrier which is not expected
        restoredTaskHarness.getOutput().removeIf(record -> record instanceof CheckpointBarrier);

        TestHarnessUtil.assertOutputEquals(
                "StateAndRestored Test Output was not correct.",
                expectedOutput,
                restoredTaskHarness.getOutput());
    }

    @SuppressWarnings("rawtypes")
    @Test
    void testObjectReused() throws Exception {
        TypeSerializer[] fieldSerializers = new TypeSerializer[] {IntSerializer.INSTANCE};
        TupleSerializer<Tuple1> inputSerializer =
                new TupleSerializer<>(Tuple1.class, fieldSerializers);
        AsyncWaitOperatorFactory<Tuple1<Integer>, Integer> factory =
                new AsyncWaitOperatorFactory<>(
                        new InputReusedAsyncFunction(),
                        TIMEOUT,
                        4,
                        AsyncDataStream.OutputMode.ORDERED);

        //noinspection unchecked
        final OneInputStreamOperatorTestHarness<Tuple1<Integer>, Integer> testHarness =
                new OneInputStreamOperatorTestHarness(factory, inputSerializer);
        // enable object reuse
        testHarness.getExecutionConfig().enableObjectReuse();

        final long initialTime = 0L;
        Tuple1<Integer> reusedTuple = new Tuple1<>();
        StreamRecord<Tuple1<Integer>> reusedRecord = new StreamRecord<>(reusedTuple, -1L);

        testHarness.setup();
        testHarness.open();

        synchronized (testHarness.getCheckpointLock()) {
            reusedTuple.setFields(1);
            reusedRecord.setTimestamp(initialTime + 1);
            testHarness.processElement(reusedRecord);

            reusedTuple.setFields(2);
            reusedRecord.setTimestamp(initialTime + 2);
            testHarness.processElement(reusedRecord);

            reusedTuple.setFields(3);
            reusedRecord.setTimestamp(initialTime + 3);
            testHarness.processElement(reusedRecord);

            reusedTuple.setFields(4);
            reusedRecord.setTimestamp(initialTime + 4);
            testHarness.processElement(reusedRecord);
        }

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        expectedOutput.add(new StreamRecord<>(2, initialTime + 1));
        expectedOutput.add(new StreamRecord<>(4, initialTime + 2));
        expectedOutput.add(new StreamRecord<>(6, initialTime + 3));
        expectedOutput.add(new StreamRecord<>(8, initialTime + 4));

        synchronized (testHarness.getCheckpointLock()) {
            testHarness.endInput();
            testHarness.close();
        }

        TestHarnessUtil.assertOutputEquals(
                "StateAndRestoredWithObjectReuse Test Output was not correct.",
                expectedOutput,
                testHarness.getOutput());
    }

    @Test
    void testAsyncTimeoutFailure() throws Exception {
        testAsyncTimeout(
                new LazyAsyncFunction(),
                Optional.of(TimeoutException.class),
                new StreamRecord<>(2, 5L));
    }

    @Test
    void testAsyncTimeoutIgnore() throws Exception {
        testAsyncTimeout(
                new IgnoreTimeoutLazyAsyncFunction(),
                Optional.empty(),
                new StreamRecord<>(3, 0L),
                new StreamRecord<>(2, 5L));
    }

    private void testAsyncTimeout(
            LazyAsyncFunction lazyAsyncFunction,
            Optional<Class<? extends Throwable>> expectedException,
            StreamRecord<Integer>... expectedRecords)
            throws Exception {
        final long timeout = 10L;

        final OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                createTestHarness(
                        lazyAsyncFunction, timeout, 2, AsyncDataStream.OutputMode.ORDERED);

        final MockEnvironment mockEnvironment = testHarness.getEnvironment();
        mockEnvironment.setExpectedExternalFailureCause(Throwable.class);

        final long initialTime = 0L;
        final ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        testHarness.setProcessingTime(initialTime);

        synchronized (testHarness.getCheckpointLock()) {
            testHarness.processElement(new StreamRecord<>(1, initialTime));
            testHarness.setProcessingTime(initialTime + 5L);
            testHarness.processElement(new StreamRecord<>(2, initialTime + 5L));
        }

        // trigger the timeout of the first stream record
        testHarness.setProcessingTime(initialTime + timeout + 1L);

        // allow the second async stream record to be processed
        lazyAsyncFunction.countDown();

        // wait until all async collectors in the buffer have been emitted out.
        synchronized (testHarness.getCheckpointLock()) {
            testHarness.endInput();
            testHarness.close();
        }

        expectedOutput.addAll(Arrays.asList(expectedRecords));

        TestHarnessUtil.assertOutputEquals(
                "Output with watermark was not correct.", expectedOutput, testHarness.getOutput());

        if (expectedException.isPresent()) {
            assertThat(mockEnvironment.getActualExternalFailureCause()).isPresent();
            assertThat(
                            ExceptionUtils.findThrowable(
                                    mockEnvironment.getActualExternalFailureCause().get(),
                                    expectedException.get()))
                    .isPresent();
        }
    }

    /**
     * FLINK-5652 Tests that registered timers are properly canceled upon completion of a {@link
     * StreamElement} in order to avoid resource leaks because TriggerTasks hold a reference on the
     * StreamRecordQueueEntry.
     */
    @Test
    void testTimeoutCleanup() throws Exception {
        OneInputStreamOperatorTestHarness<Integer, Integer> harness =
                createTestHarness(
                        new MyAsyncFunction(), TIMEOUT, 1, AsyncDataStream.OutputMode.UNORDERED);

        harness.open();

        synchronized (harness.getCheckpointLock()) {
            harness.processElement(42, 1L);
        }

        synchronized (harness.getCheckpointLock()) {
            harness.endInput();
            harness.close();
        }

        // check that we actually outputted the result of the single input
        assertThat(harness.getOutput()).containsOnly(new StreamRecord<>(42 * 2, 1L));

        // check that we have cancelled our registered timeout
        assertThat(harness.getProcessingTimeService().getNumActiveTimers()).isZero();
    }

    /**
     * Checks if timeout has been called after the element has been completed within the timeout.
     *
     * @see <a href="https://issues.apache.org/jira/browse/FLINK-22573">FLINK-22573</a>
     */
    @Test
    void testTimeoutAfterComplete() throws Exception {
        StreamTaskMailboxTestHarnessBuilder<Integer> builder =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                OneInputStreamTask::new, BasicTypeInfo.INT_TYPE_INFO)
                        .addInput(BasicTypeInfo.INT_TYPE_INFO);
        try (StreamTaskMailboxTestHarness<Integer> harness =
                builder.setupOutputForSingletonOperatorChain(
                                new AsyncWaitOperatorFactory<>(
                                        new TimeoutAfterCompletionTestFunction(),
                                        TIMEOUT,
                                        1,
                                        AsyncDataStream.OutputMode.UNORDERED))
                        .build()) {
            harness.processElement(new StreamRecord<>(1));
            // add a timer after AsyncIO added its timer to verify that AsyncIO timer is processed
            ScheduledFuture<?> testTimer =
                    harness.getTimerService()
                            .registerTimer(
                                    harness.getTimerService().getCurrentProcessingTime() + TIMEOUT,
                                    ts -> {});
            // trigger the regular completion in AsyncIO
            TimeoutAfterCompletionTestFunction.COMPLETION_TRIGGER.countDown();
            // wait until all timers have been processed
            testTimer.get();
            // handle normal completion call outputting the element in mailbox thread
            harness.processAll();
            assertThat(harness.getOutput()).containsOnly(new StreamRecord<>(1));
            assertThat(TimeoutAfterCompletionTestFunction.TIMED_OUT).isFalse();
            harness.waitForTaskCompletion();
        }
    }

    /**
     * FLINK-6435
     *
     * <p>Tests that a user exception triggers the completion of a StreamElementQueueEntry and does
     * not wait to until another StreamElementQueueEntry is properly completed before it is
     * collected.
     */
    @Test
    void testOrderedWaitUserExceptionHandling() throws Exception {
        testUserExceptionHandling(
                AsyncDataStream.OutputMode.ORDERED, AsyncRetryStrategies.NO_RETRY_STRATEGY);
    }

    @Test
    void testOrderedWaitUserExceptionHandlingWithRetry() throws Exception {
        testUserExceptionHandling(AsyncDataStream.OutputMode.ORDERED, exceptionRetryStrategy);
    }

    /**
     * FLINK-6435
     *
     * <p>Tests that a user exception triggers the completion of a StreamElementQueueEntry and does
     * not wait to until another StreamElementQueueEntry is properly completed before it is
     * collected.
     */
    @Test
    void testUnorderedWaitUserExceptionHandling() throws Exception {
        testUserExceptionHandling(
                AsyncDataStream.OutputMode.UNORDERED, AsyncRetryStrategies.NO_RETRY_STRATEGY);
    }

    @Test
    void testUnorderedWaitUserExceptionHandlingWithRetry() throws Exception {
        testUserExceptionHandling(AsyncDataStream.OutputMode.UNORDERED, exceptionRetryStrategy);
    }

    private void testUserExceptionHandling(
            AsyncDataStream.OutputMode outputMode, AsyncRetryStrategy asyncRetryStrategy)
            throws Exception {
        OneInputStreamOperatorTestHarness<Integer, Integer> harness =
                createTestHarnessWithRetry(
                        new UserExceptionAsyncFunction(),
                        TIMEOUT,
                        2,
                        outputMode,
                        asyncRetryStrategy);

        harness.getEnvironment().setExpectedExternalFailureCause(Throwable.class);
        harness.open();

        synchronized (harness.getCheckpointLock()) {
            harness.processElement(1, 1L);
        }

        synchronized (harness.getCheckpointLock()) {
            harness.endInput();
            harness.close();
        }

        assertThat(harness.getEnvironment().getActualExternalFailureCause()).isPresent();
    }

    /** AsyncFunction which completes the result with an {@link Exception}. */
    private static class UserExceptionAsyncFunction implements AsyncFunction<Integer, Integer> {

        private static final long serialVersionUID = 6326568632967110990L;

        @Override
        public void asyncInvoke(Integer input, ResultFuture<Integer> resultFuture)
                throws Exception {
            resultFuture.completeExceptionally(new Exception("Test exception"));
        }
    }

    /**
     * FLINK-6435
     *
     * <p>Tests that timeout exceptions are properly handled in ordered output mode. The proper
     * handling means that a StreamElementQueueEntry is completed in case of a timeout exception.
     */
    @Test
    void testOrderedWaitTimeoutHandling() throws Exception {
        testTimeoutExceptionHandling(
                AsyncDataStream.OutputMode.ORDERED, AsyncRetryStrategies.NO_RETRY_STRATEGY);
    }

    @Test
    void testOrderedWaitTimeoutHandlingWithRetry() throws Exception {
        testTimeoutExceptionHandling(
                AsyncDataStream.OutputMode.ORDERED, emptyResultFixedDelayRetryStrategy);
    }

    /**
     * FLINK-6435
     *
     * <p>Tests that timeout exceptions are properly handled in ordered output mode. The proper
     * handling means that a StreamElementQueueEntry is completed in case of a timeout exception.
     */
    @Test
    void testUnorderedWaitTimeoutHandling() throws Exception {
        testTimeoutExceptionHandling(
                AsyncDataStream.OutputMode.UNORDERED, AsyncRetryStrategies.NO_RETRY_STRATEGY);
    }

    @Test
    void testUnorderedWaitTimeoutHandlingWithRetry() throws Exception {
        testTimeoutExceptionHandling(
                AsyncDataStream.OutputMode.UNORDERED, emptyResultFixedDelayRetryStrategy);
    }

    private void testTimeoutExceptionHandling(
            AsyncDataStream.OutputMode outputMode, AsyncRetryStrategy asyncRetryStrategy)
            throws Exception {
        OneInputStreamOperatorTestHarness<Integer, Integer> harness =
                createTestHarnessWithRetry(
                        new NoOpAsyncFunction<>(), 10L, 2, outputMode, asyncRetryStrategy);

        harness.getEnvironment().setExpectedExternalFailureCause(Throwable.class);
        harness.open();

        synchronized (harness.getCheckpointLock()) {
            harness.processElement(1, 1L);
        }

        harness.setProcessingTime(10L);

        synchronized (harness.getCheckpointLock()) {
            harness.close();
        }
    }

    /**
     * Tests that the AsyncWaitOperator can restart if checkpointed queue was full.
     *
     * <p>See FLINK-7949
     */
    @Test
    @Timeout(value = 10000, unit = TimeUnit.MILLISECONDS)
    void testRestartWithFullQueue() throws Exception {
        final int capacity = 10;

        // 1. create the snapshot which contains capacity + 1 elements
        final CompletableFuture<Void> trigger = new CompletableFuture<>();

        final OneInputStreamOperatorTestHarness<Integer, Integer> snapshotHarness =
                createTestHarness(
                        new ControllableAsyncFunction<>(
                                trigger), // the NoOpAsyncFunction is like a blocking function
                        1000L,
                        capacity,
                        AsyncDataStream.OutputMode.ORDERED);

        snapshotHarness.open();

        final OperatorSubtaskState snapshot;

        final ArrayList<Integer> expectedOutput = new ArrayList<>(capacity);

        try {
            synchronized (snapshotHarness.getCheckpointLock()) {
                for (int i = 0; i < capacity; i++) {
                    snapshotHarness.processElement(i, 0L);
                    expectedOutput.add(i);
                }
            }

            synchronized (snapshotHarness.getCheckpointLock()) {
                // execute the snapshot within the checkpoint lock, because then it is guaranteed
                // that the lastElementWriter has written the exceeding element
                snapshot = snapshotHarness.snapshot(0L, 0L);
            }

            // trigger the computation to make the close call finish
            trigger.complete(null);
        } finally {
            synchronized (snapshotHarness.getCheckpointLock()) {
                snapshotHarness.close();
            }
        }

        // 2. restore the snapshot and check that we complete
        final OneInputStreamOperatorTestHarness<Integer, Integer> recoverHarness =
                createTestHarness(
                        new ControllableAsyncFunction<>(CompletableFuture.completedFuture(null)),
                        1000L,
                        capacity,
                        AsyncDataStream.OutputMode.ORDERED);

        recoverHarness.initializeState(snapshot);

        synchronized (recoverHarness.getCheckpointLock()) {
            recoverHarness.open();
        }

        synchronized (recoverHarness.getCheckpointLock()) {
            recoverHarness.endInput();
            recoverHarness.close();
        }

        final ConcurrentLinkedQueue<Object> output = recoverHarness.getOutput();

        final List<Integer> outputElements =
                output.stream()
                        .map(r -> ((StreamRecord<Integer>) r).getValue())
                        .collect(Collectors.toList());

        assertThat(outputElements).isEqualTo(expectedOutput);
    }

    @Test
    void testIgnoreAsyncOperatorRecordsOnDrain() throws Exception {
        testIgnoreAsyncOperatorRecordsOnDrain(AsyncRetryStrategies.NO_RETRY_STRATEGY);
    }

    @Test
    void testIgnoreAsyncOperatorRecordsOnDrainWithRetry() throws Exception {
        testIgnoreAsyncOperatorRecordsOnDrain(emptyResultFixedDelayRetryStrategy);
    }

    private void testIgnoreAsyncOperatorRecordsOnDrain(AsyncRetryStrategy asyncRetryStrategy)
            throws Exception {
        // given: Async wait operator which are able to collect result futures.
        StreamTaskMailboxTestHarnessBuilder<Integer> builder =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                OneInputStreamTask::new, BasicTypeInfo.INT_TYPE_INFO)
                        .addInput(BasicTypeInfo.INT_TYPE_INFO);
        SharedReference<List<ResultFuture<?>>> resultFutures = sharedObjects.add(new ArrayList<>());
        try (StreamTaskMailboxTestHarness<Integer> harness =
                builder.setupOutputForSingletonOperatorChain(
                                new AsyncWaitOperatorFactory<>(
                                        new CollectableFuturesAsyncFunction<>(resultFutures),
                                        TIMEOUT,
                                        5,
                                        AsyncDataStream.OutputMode.ORDERED,
                                        asyncRetryStrategy))
                        .build()) {
            // when: Processing at least two elements in reverse order to keep completed queue not
            // empty.
            harness.processElement(new StreamRecord<>(1));
            harness.processElement(new StreamRecord<>(2));

            for (ResultFuture<?> resultFuture : Lists.reverse(resultFutures.get())) {
                resultFuture.complete(Collections.emptyList());
            }

            // then: All records from async operator should be ignored during drain since they will
            // be processed on recovery.
            harness.finishProcessing();
            assertThat(harness.getOutput()).isEmpty();
        }
    }

    /** Test the AsyncWaitOperator with ordered mode and processing time. */
    @Test
    void testProcessingTimeOrderedWithRetry() throws Exception {
        testProcessingTimeWithRetry(
                AsyncDataStream.OutputMode.ORDERED, new OddInputEmptyResultAsyncFunction());
    }

    /** Test the AsyncWaitOperator with unordered mode and processing time. */
    @Test
    void testProcessingTimeUnorderedWithRetry() throws Exception {
        testProcessingTimeWithRetry(
                AsyncDataStream.OutputMode.UNORDERED, new OddInputEmptyResultAsyncFunction());
    }

    /**
     * Test the AsyncWaitOperator with an ill-written async function under unordered mode and
     * processing time.
     */
    @Test
    void testProcessingTimeRepeatedCompleteUnorderedWithRetry() throws Exception {
        testProcessingTimeWithRetry(
                AsyncDataStream.OutputMode.UNORDERED,
                new IllWrittenOddInputEmptyResultAsyncFunction());
    }

    /**
     * Test the AsyncWaitOperator with an ill-written async function under ordered mode and
     * processing time.
     */
    @Test
    void testProcessingTimeRepeatedCompleteOrderedWithRetry() throws Exception {
        testProcessingTimeWithRetry(
                AsyncDataStream.OutputMode.ORDERED,
                new IllWrittenOddInputEmptyResultAsyncFunction());
    }

    private void testProcessingTimeWithRetry(
            AsyncDataStream.OutputMode mode, RichAsyncFunction asyncFunction) throws Exception {

        StreamTaskMailboxTestHarnessBuilder<Integer> builder =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                OneInputStreamTask::new, BasicTypeInfo.INT_TYPE_INFO)
                        .addInput(BasicTypeInfo.INT_TYPE_INFO);

        try (StreamTaskMailboxTestHarness<Integer> testHarness =
                builder.setupOutputForSingletonOperatorChain(
                                new AsyncWaitOperatorFactory<>(
                                        asyncFunction,
                                        TIMEOUT,
                                        6,
                                        mode,
                                        emptyResultFixedDelayRetryStrategy))
                        .build()) {

            final long initialTime = 0L;
            final Queue<Object> expectedOutput = new ArrayDeque<>();

            testHarness.processElement(new StreamRecord<>(1, initialTime + 1));
            testHarness.processElement(new StreamRecord<>(2, initialTime + 2));
            testHarness.processElement(new StreamRecord<>(3, initialTime + 3));
            testHarness.processElement(new StreamRecord<>(4, initialTime + 4));
            testHarness.processElement(new StreamRecord<>(5, initialTime + 5));
            testHarness.processElement(new StreamRecord<>(6, initialTime + 6));

            expectedOutput.add(new StreamRecord<>(4, initialTime + 2));
            expectedOutput.add(new StreamRecord<>(8, initialTime + 4));
            expectedOutput.add(new StreamRecord<>(12, initialTime + 6));

            while (testHarness.getOutput().size() < expectedOutput.size()) {
                testHarness.processAll();
                //noinspection BusyWait
                Thread.sleep(100);
            }

            if (mode == AsyncDataStream.OutputMode.ORDERED) {
                TestHarnessUtil.assertOutputEquals(
                        "ORDERED Output was not correct.", expectedOutput, testHarness.getOutput());
            } else {
                TestHarnessUtil.assertOutputEqualsSorted(
                        "UNORDERED Output was not correct.",
                        expectedOutput,
                        testHarness.getOutput(),
                        new StreamRecordComparator());
            }
            testHarness.waitForTaskCompletion();
        }
    }

    /**
     * Test the AsyncWaitOperator with an always-timeout async function under unordered mode and
     * processing time.
     */
    @Test
    void testProcessingTimeWithTimeoutFunctionUnorderedWithRetry() throws Exception {
        testProcessingTimeAlwaysTimeoutFunctionWithRetry(AsyncDataStream.OutputMode.UNORDERED);
    }

    /**
     * Test the AsyncWaitOperator with an always-timeout async function under ordered mode and
     * processing time.
     */
    @Test
    void testProcessingTimeWithTimeoutFunctionOrderedWithRetry() throws Exception {
        testProcessingTimeAlwaysTimeoutFunctionWithRetry(AsyncDataStream.OutputMode.ORDERED);
    }

    private void testProcessingTimeAlwaysTimeoutFunctionWithRetry(AsyncDataStream.OutputMode mode)
            throws Exception {

        StreamTaskMailboxTestHarnessBuilder<Integer> builder =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                OneInputStreamTask::new, BasicTypeInfo.INT_TYPE_INFO)
                        .addInput(BasicTypeInfo.INT_TYPE_INFO);

        AsyncRetryStrategy exceptionRetryStrategy =
                new AsyncRetryStrategies.FixedDelayRetryStrategyBuilder(5, 100L)
                        .ifException(RetryPredicates.HAS_EXCEPTION_PREDICATE)
                        .build();
        AlwaysTimeoutWithDefaultValueAsyncFunction asyncFunction =
                new AlwaysTimeoutWithDefaultValueAsyncFunction();

        try (StreamTaskMailboxTestHarness<Integer> testHarness =
                builder.setupOutputForSingletonOperatorChain(
                                new AsyncWaitOperatorFactory<>(
                                        asyncFunction, TIMEOUT, 10, mode, exceptionRetryStrategy))
                        .build()) {

            final long initialTime = 0L;
            final Queue<Object> expectedOutput = new ArrayDeque<>();

            testHarness.processElement(new StreamRecord<>(1, initialTime + 1));
            testHarness.processElement(new StreamRecord<>(2, initialTime + 2));

            expectedOutput.add(new StreamRecord<>(-1, initialTime + 1));
            expectedOutput.add(new StreamRecord<>(-1, initialTime + 2));

            while (testHarness.getOutput().size() < expectedOutput.size()) {
                testHarness.processAll();
                //noinspection BusyWait
                Thread.sleep(100);
            }

            if (mode == AsyncDataStream.OutputMode.ORDERED) {
                TestHarnessUtil.assertOutputEquals(
                        "ORDERED Output was not correct.", expectedOutput, testHarness.getOutput());
            } else {
                TestHarnessUtil.assertOutputEqualsSorted(
                        "UNORDERED Output was not correct.",
                        expectedOutput,
                        testHarness.getOutput(),
                        new StreamRecordComparator());
            }

            // verify the elements' try count never beyond 2 (use <= instead of == to avoid unstable
            // case when test machine under high load)
            assertThat(asyncFunction.getTryCount(1)).isLessThanOrEqualTo(2);
            assertThat(asyncFunction.getTryCount(2)).isLessThanOrEqualTo(2);
            testHarness.waitForTaskCompletion();
        }
    }

    @Test
    void testProcessingTimeWithAlwaysTimeoutFunctionUnorderedWithRetry() throws Exception {
        testProcessingTimeAlwaysTimeoutFunction(AsyncDataStream.OutputMode.UNORDERED);
    }

    @Test
    void testProcessingTimeWithAlwaysTimeoutFunctionOrderedWithRetry() throws Exception {
        testProcessingTimeAlwaysTimeoutFunction(AsyncDataStream.OutputMode.ORDERED);
    }

    private void testProcessingTimeAlwaysTimeoutFunction(AsyncDataStream.OutputMode mode)
            throws Exception {
        StreamTaskMailboxTestHarnessBuilder<Integer> builder =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                OneInputStreamTask::new, BasicTypeInfo.INT_TYPE_INFO)
                        .addInput(BasicTypeInfo.INT_TYPE_INFO);

        AlwaysTimeoutAsyncFunction asyncFunction = new AlwaysTimeoutAsyncFunction();
        try (StreamTaskMailboxTestHarness<Integer> testHarness =
                builder.setupOutputForSingletonOperatorChain(
                                new AsyncWaitOperatorFactory<Integer, Integer>(
                                        asyncFunction, TIMEOUT, 10, mode, exceptionRetryStrategy))
                        .build()) {

            final long initialTime = 0L;
            AtomicReference<Throwable> error = new AtomicReference<>();
            testHarness.getStreamMockEnvironment().setExternalExceptionHandler(error::set);

            try {
                testHarness.processElement(new StreamRecord<>(1, initialTime + 1));
                testHarness.processElement(new StreamRecord<>(2, initialTime + 2));
                while (error.get() == null) {
                    testHarness.processAll();
                }
            } catch (Exception e) {
                error.set(e);
            }
            ExceptionUtils.assertThrowableWithMessage(error.get(), "Dummy timeout error");
            // verify the 1st element's try count is exactly 1
            assertThat(asyncFunction.getTryCount(1)).isEqualTo(1);
        }
    }

    @Test
    public void testProcessingTimeWithMailboxThreadOrdered() throws Exception {
        testProcessingTimeWithCollectFromMailboxThread(
                AsyncDataStream.OutputMode.ORDERED, NO_RETRY_STRATEGY);
    }

    @Test
    public void testProcessingTimeWithMailboxThreadUnordered() throws Exception {
        testProcessingTimeWithCollectFromMailboxThread(
                AsyncDataStream.OutputMode.UNORDERED, NO_RETRY_STRATEGY);
    }

    @Test
    public void testProcessingTimeWithMailboxThreadOrderedWithRetry() throws Exception {
        testProcessingTimeWithCollectFromMailboxThread(
                AsyncDataStream.OutputMode.ORDERED, exceptionRetryStrategy);
    }

    @Test
    public void testProcessingTimeWithMailboxThreadUnorderedWithRetry() throws Exception {
        testProcessingTimeWithCollectFromMailboxThread(
                AsyncDataStream.OutputMode.UNORDERED, exceptionRetryStrategy);
    }

    @Test
    public void testProcessingTimeWithErrorFromMailboxThread() throws Exception {
        testProcessingTimeWithErrorFromMailboxThread(NO_RETRY_STRATEGY);
    }

    @Test
    public void testProcessingTimeWithMailboxThreadErrorWithRetry() throws Exception {
        testProcessingTimeWithErrorFromMailboxThread(exceptionRetryStrategy);
    }

    private void testProcessingTimeWithErrorFromMailboxThread(
            @Nullable AsyncRetryStrategy<Integer> asyncRetryStrategy) throws Exception {
        StreamTaskMailboxTestHarnessBuilder<Integer> builder =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                OneInputStreamTask::new, BasicTypeInfo.INT_TYPE_INFO)
                        .addInput(BasicTypeInfo.INT_TYPE_INFO);
        try (StreamTaskMailboxTestHarness<Integer> testHarness =
                builder.setupOutputForSingletonOperatorChain(
                                new AsyncWaitOperatorFactory<>(
                                        new CallThreadAsyncFunctionError(),
                                        TIMEOUT,
                                        1,
                                        AsyncDataStream.OutputMode.UNORDERED,
                                        asyncRetryStrategy))
                        .build()) {
            final long initialTime = 0L;
            AtomicReference<Throwable> error = new AtomicReference<>();
            testHarness.getStreamMockEnvironment().setExternalExceptionHandler(error::set);

            // Sometimes, processElement invoke the async function immediately, so we should catch
            // any exception.
            try {
                testHarness.processElement(new StreamRecord<>(1, initialTime + 1));
                while (error.get() == null) {
                    testHarness.processAll();
                }
            } catch (Exception e) {
                // This simulates a mailbox failure failing the job
                error.set(e);
            }

            ExceptionUtils.assertThrowable(error.get(), ExpectedTestException.class);

            testHarness.endInput();
        }
    }

    private void testProcessingTimeWithCollectFromMailboxThread(
            AsyncDataStream.OutputMode mode,
            @Nullable AsyncRetryStrategy<Integer> asyncRetryStrategy)
            throws Exception {
        StreamTaskMailboxTestHarnessBuilder<Integer> builder =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                OneInputStreamTask::new, BasicTypeInfo.INT_TYPE_INFO)
                        .addInput(BasicTypeInfo.INT_TYPE_INFO);
        try (StreamTaskMailboxTestHarness<Integer> testHarness =
                builder.setupOutputForSingletonOperatorChain(
                                new AsyncWaitOperatorFactory<>(
                                        new CallThreadAsyncFunction(),
                                        TIMEOUT,
                                        3,
                                        mode,
                                        asyncRetryStrategy))
                        .build()) {

            final long initialTime = 0L;
            final Queue<Object> expectedOutput = new ArrayDeque<>();

            testHarness.processElement(new StreamRecord<>(1, initialTime + 1));
            testHarness.processElement(new StreamRecord<>(2, initialTime + 2));
            testHarness.processElement(new StreamRecord<>(3, initialTime + 3));

            expectedOutput.add(new StreamRecord<>(2, initialTime + 1));
            expectedOutput.add(new StreamRecord<>(4, initialTime + 2));
            expectedOutput.add(new StreamRecord<>(6, initialTime + 3));

            while (testHarness.getOutput().size() < expectedOutput.size()) {
                testHarness.processAll();
            }

            if (mode == AsyncDataStream.OutputMode.ORDERED) {
                TestHarnessUtil.assertOutputEquals(
                        "ORDERED Output was not correct.", expectedOutput, testHarness.getOutput());
            } else {
                TestHarnessUtil.assertOutputEqualsSorted(
                        "UNORDERED Output was not correct.",
                        expectedOutput,
                        testHarness.getOutput(),
                        new StreamRecordComparator());
            }

            testHarness.endInput();
        }
    }

    private static class CollectableFuturesAsyncFunction<IN> implements AsyncFunction<IN, IN> {

        private static final long serialVersionUID = -4214078239227288637L;

        private final SharedReference<List<ResultFuture<?>>> resultFutures;

        private CollectableFuturesAsyncFunction(
                SharedReference<List<ResultFuture<?>>> resultFutures) {
            this.resultFutures = resultFutures;
        }

        @Override
        public void asyncInvoke(IN input, ResultFuture<IN> resultFuture) throws Exception {
            resultFutures.get().add(resultFuture);
        }
    }

    private static class ControllableAsyncFunction<IN> implements AsyncFunction<IN, IN> {

        private static final long serialVersionUID = -4214078239267288636L;

        private transient CompletableFuture<Void> trigger;

        private ControllableAsyncFunction(CompletableFuture<Void> trigger) {
            this.trigger = Preconditions.checkNotNull(trigger);
        }

        @Override
        public void asyncInvoke(IN input, ResultFuture<IN> resultFuture) throws Exception {
            trigger.thenAccept(v -> resultFuture.complete(Collections.singleton(input)));
        }
    }

    private static class NoOpAsyncFunction<IN, OUT> implements AsyncFunction<IN, OUT> {
        private static final long serialVersionUID = -3060481953330480694L;

        @Override
        public void asyncInvoke(IN input, ResultFuture<OUT> resultFuture) throws Exception {
            // no op
        }
    }

    private static <OUT> OneInputStreamOperatorTestHarness<Integer, OUT> createTestHarness(
            AsyncFunction<Integer, OUT> function,
            long timeout,
            int capacity,
            AsyncDataStream.OutputMode outputMode)
            throws Exception {

        return new OneInputStreamOperatorTestHarness<>(
                new AsyncWaitOperatorFactory<>(function, timeout, capacity, outputMode),
                IntSerializer.INSTANCE);
    }

    private static <OUT> OneInputStreamOperatorTestHarness<Integer, OUT> createTestHarnessWithRetry(
            AsyncFunction<Integer, OUT> function,
            long timeout,
            int capacity,
            AsyncDataStream.OutputMode outputMode,
            AsyncRetryStrategy<OUT> asyncRetryStrategy)
            throws Exception {

        return new OneInputStreamOperatorTestHarness<>(
                new AsyncWaitOperatorFactory<>(
                        function, timeout, capacity, outputMode, asyncRetryStrategy),
                IntSerializer.INSTANCE);
    }

    private static class AlwaysTimeoutWithDefaultValueAsyncFunction
            extends RichAsyncFunction<Integer, Integer> {

        private static final long serialVersionUID = 1L;

        protected static Map<Integer, Integer> tryCounts = new HashMap<>();

        @VisibleForTesting
        public int getTryCount(Integer item) {
            return tryCounts.getOrDefault(item, 0);
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);
            tryCounts = new HashMap<>();
        }

        @Override
        public void asyncInvoke(Integer input, ResultFuture<Integer> resultFuture) {
            tryCounts.merge(input, 1, Integer::sum);

            CompletableFuture.runAsync(
                    () -> {
                        try {
                            Thread.sleep(501L);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        resultFuture.completeExceptionally(new Exception("Dummy error"));
                    });
        }

        @Override
        public void timeout(Integer input, ResultFuture<Integer> resultFuture) {
            // collect a default value -1 when timeout
            resultFuture.complete(Collections.singletonList(-1));
        }
    }

    private static class AlwaysTimeoutAsyncFunction
            extends AlwaysTimeoutWithDefaultValueAsyncFunction {

        private final transient CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void asyncInvoke(Integer input, ResultFuture<Integer> resultFuture) {
            tryCounts.merge(input, 1, Integer::sum);
            CompletableFuture.runAsync(
                    () -> {
                        try {
                            latch.await();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }

        @Override
        public void timeout(Integer input, ResultFuture<Integer> resultFuture) {
            // simulate the case reported in https://issues.apache.org/jira/browse/FLINK-38082
            resultFuture.completeExceptionally(new TimeoutException("Dummy timeout error"));
        }
    }

    private static class CallThreadAsyncFunction extends MyAbstractAsyncFunction<Integer> {
        private static final long serialVersionUID = -1504699677704123889L;

        @Override
        public void asyncInvoke(final Integer input, final ResultFuture<Integer> resultFuture)
                throws Exception {
            final Thread callThread = Thread.currentThread();
            executorService.submit(
                    () ->
                            resultFuture.complete(
                                    () -> {
                                        assertEquals(callThread, Thread.currentThread());
                                        return Collections.singletonList(input * 2);
                                    }));
        }
    }

    private static class CallThreadAsyncFunctionError extends MyAbstractAsyncFunction<Integer> {
        private static final long serialVersionUID = -1504699677704123889L;

        @Override
        public void asyncInvoke(final Integer input, final ResultFuture<Integer> resultFuture)
                throws Exception {
            Thread callThread = Thread.currentThread();
            executorService.submit(
                    () ->
                            resultFuture.complete(
                                    () -> {
                                        assertEquals(callThread, Thread.currentThread());
                                        throw new ExpectedTestException();
                                    }));
        }
    }
}
