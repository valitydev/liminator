package dev.vality.liminator.transaction;

import dev.vality.liminator.LimitChange;
import dev.vality.liminator.LimitRequest;
import dev.vality.liminator.OperationAlreadyInFinalState;
import dev.vality.liminator.OperationNotFound;
import dev.vality.liminator.dao.LimitContextDao;
import dev.vality.liminator.dao.LimitDataDao;
import dev.vality.liminator.dao.OperationStateHistoryDao;
import dev.vality.liminator.domain.enums.OperationState;
import dev.vality.liminator.domain.tables.pojos.LimitContext;
import dev.vality.liminator.domain.tables.pojos.LimitData;
import dev.vality.liminator.domain.tables.pojos.OperationStateHistory;
import dev.vality.liminator.service.LimitDataService;
import dev.vality.liminator.service.LiminatorService;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static dev.vality.liminator.domain.Tables.LIMIT_DATA;
import static dev.vality.liminator.domain.Tables.OPERATION_STATE_HISTORY;
import static dev.vality.liminator.domain.enums.OperationState.COMMIT;
import static dev.vality.liminator.domain.enums.OperationState.HOLD;
import static dev.vality.liminator.domain.enums.OperationState.ROLLBACK;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

// No surrounding test transaction: each worker must commit through the real Spring proxy.
@Timeout(30)
class OperationConcurrencyTest extends AbstractIntegrationTestWithEmbeddedPostgres {

    @Autowired
    private LiminatorService service;
    @Autowired
    private LimitDataService limitDataService;
    @Autowired
    private LimitContextDao contextDao;
    @Autowired
    private DSLContext dslContext;
    @Autowired
    private PlatformTransactionManager transactionManager;
    @MockitoSpyBean
    private OperationStateHistoryDao historyDao;
    @MockitoSpyBean
    private LimitDataDao limitDataDao;

    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        executor = Executors.newFixedThreadPool(3);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(10, SECONDS));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void concurrentDuplicateHoldPreservesFirst(boolean existingLimit) throws Exception {
        String operationId = id();
        String name = id();
        LimitChange first = new LimitChange(name, 100).setLimitId("first").setContext(Map.of("owner", "first"));
        if (existingLimit) {
            limitDataService.save(first);
        }
        LimitChange second = new LimitChange(name, 900).setLimitId("second").setContext(Map.of("owner", "second"));

        try (PausedWrite pause = pauseFirstWrite(operationId)) {
            final Future<?> firstResult = hold(request(operationId, first));
            pause.awaitEntered();
            final Future<?> secondResult = hold(request(operationId, second));
            awaitBlockedLock();
            pause.close();
            firstResult.get(10, SECONDS);
            secondResult.get(10, SECONDS);
        }

        LimitData limit = limitDataDao.get(name);
        assertEquals("first", limit.getLimitId());
        assertEquals("{\"owner\":\"first\"}", contextDao.getLimitContext(limit.getId()).getContext());
        List<OperationStateHistory> history = history(operationId);
        assertEquals(1, history.size());
        assertEquals(100, history.get(0).getOperationValue());
        assertEquals(100, service.getLastLimitsValues(List.of(name)).get(0).getTotalValue());

        service.hold(request(operationId, second));
        assertEquals(history, history(operationId));
        assertEquals(limit, limitDataDao.get(name));
    }

    @Test
    void duplicateContextReturnsOriginalIdWithoutUpdatingFields() {
        long limitId = limitDataService.save(new LimitChange(id(), 100));
        LocalDateTime time = LocalDateTime.now().minusDays(1).withNano(0);
        LimitContext first = new LimitContext(null, limitId, "first", time);
        Long contextId = contextDao.save(first);
        LimitContext original = contextDao.getLimitContext(limitId);

        assertEquals(contextId, contextDao.save(new LimitContext(null, limitId, "second", LocalDateTime.now())));
        assertEquals(original, contextDao.getLimitContext(limitId));
    }

    @Test
    void differentOperationsCreateSharedLimitWithOneContext() throws Exception {
        String name = id();
        synchronizeInitialLimitReads(name);
        String firstId = id();
        String secondId = id();
        final Future<?> first = hold(request(firstId, new LimitChange(name, 100).setContext(Map.of("owner", "first"))));
        final Future<?> second = hold(request(secondId,
                new LimitChange(name, 200).setContext(Map.of("owner", "second"))));
        first.get(10, SECONDS);
        second.get(10, SECONDS);

        assertEquals(1, dslContext.fetchCount(LIMIT_DATA, LIMIT_DATA.NAME.eq(name)));
        assertNotNull(contextDao.getLimitContext(limitDataDao.get(name).getId()));
        assertEquals(1, history(firstId).size());
        assertEquals(1, history(secondId).size());
        assertEquals(300, service.getLastLimitsValues(List.of(name)).get(0).getTotalValue());
    }

    @Test
    void oppositeLimitOrderDoesNotDeadlock() throws Exception {
        String nameA = "a-" + id();
        String nameB = "b-" + id();
        String firstId = id();
        String secondId = id();
        try (PausedWrite pause = new PausedWrite()) {
            AtomicBoolean firstInsert = new AtomicBoolean(true);
            doAnswer(invocation -> {
                Object result = invocation.callRealMethod();
                LimitData limit = invocation.getArgument(0);
                if (limit.getName().equals(nameA) && firstInsert.compareAndSet(true, false)) {
                    pause.entered.countDown();
                    awaitLatch(pause.release);
                }
                return result;
            }).when(limitDataDao).save(any(LimitData.class));
            final Future<?> first = hold(request(firstId, new LimitChange(nameA, 100), new LimitChange(nameB, 200)));
            pause.awaitEntered();
            final Future<?> second = hold(request(secondId, new LimitChange(nameB, 400), new LimitChange(nameA, 300)));
            // The second transaction waits on A. In request order it would own B,
            // causing a deadlock as soon as the first transaction continues.
            await().atMost(Duration.ofSeconds(2)).until(() -> dslContext.fetchOne(
                    "select count(*) from pg_locks where locktype = 'transactionid' and not granted")
                    .get(0, Integer.class) > 0);
            pause.close();
            first.get(10, SECONDS);
            second.get(10, SECONDS);
        }

        assertEquals(2, history(firstId).size());
        assertEquals(2, history(secondId).size());
        var values = service.getLastLimitsValues(List.of(nameA, nameB));
        assertEquals(400, values.stream().filter(value -> value.getLimitName().equals(nameA))
                .findFirst().orElseThrow().getTotalValue());
        assertEquals(600, values.stream().filter(value -> value.getLimitName().equals(nameB))
                .findFirst().orElseThrow().getTotalValue());
    }

    @ParameterizedTest
    @EnumSource(value = OperationState.class, names = {"COMMIT", "ROLLBACK"})
    void concurrentRepeatedFinalizationWritesOnce(OperationState state) throws Exception {
        LimitRequest request = request(id(), new LimitChange(id(), 100));
        service.hold(request);
        try (PausedWrite pause = pauseFirstWrite(request.getOperationId())) {
            final Future<?> first = finalizeOperation(request, state);
            pause.awaitEntered();
            final Future<?> second = finalizeOperation(request, state);
            awaitBlockedLock();
            pause.close();
            first.get(10, SECONDS);
            second.get(10, SECONDS);
        }
        assertStates(request.getOperationId(), HOLD, state);
        assertEquals(state == COMMIT ? 100 : 0, service.getLastLimitsValues(
                List.of(request.getLimitChanges().get(0).getLimitName())).get(0).getTotalValue());
    }

    @ParameterizedTest
    @EnumSource(value = OperationState.class, names = {"COMMIT", "ROLLBACK"})
    void oppositeFinalizationPreservesFirstState(OperationState firstState) throws Exception {
        LimitRequest request = request(id(), new LimitChange(id(), 100));
        service.hold(request);
        try (PausedWrite pause = pauseFirstWrite(request.getOperationId())) {
            final Future<?> first = finalizeOperation(request, firstState);
            pause.awaitEntered();
            final Future<?> second = finalizeOperation(request, firstState == COMMIT ? ROLLBACK : COMMIT);
            awaitBlockedLock();
            pause.close();
            first.get(10, SECONDS);
            assertInstanceOf(OperationNotFound.class, failure(second));
        }
        assertStates(request.getOperationId(), HOLD, firstState);
    }

    @Test
    void concurrentCommitWithDifferentValueIsRejected() throws Exception {
        LimitRequest request = request(id(), new LimitChange(id(), 100));
        service.hold(request);
        try (PausedWrite pause = pauseFirstWrite(request.getOperationId())) {
            final Future<?> first = finalizeOperation(request, COMMIT);
            pause.awaitEntered();
            final Future<?> second = finalizeOperation(request(request.getOperationId(),
                    new LimitChange(request.getLimitChanges().get(0).getLimitName(), 50)), COMMIT);
            awaitBlockedLock();
            pause.close();
            first.get(10, SECONDS);
            assertInstanceOf(OperationNotFound.class, failure(second));
        }
        assertStates(request.getOperationId(), HOLD, COMMIT);
    }

    @Test
    void commitWaitsForConcurrentHold() throws Exception {
        LimitRequest request = request(id(), new LimitChange(id(), 100));
        try (PausedWrite pause = pauseFirstWrite(request.getOperationId())) {
            final Future<?> first = hold(request);
            pause.awaitEntered();
            final Future<?> second = finalizeOperation(request, COMMIT);
            awaitBlockedLock();
            pause.close();
            first.get(10, SECONDS);
            second.get(10, SECONDS);
        }
        assertStates(request.getOperationId(), HOLD, COMMIT);
    }

    @Test
    void holdWaitsForConcurrentFinalizationAndIsRejected() throws Exception {
        LimitRequest request = request(id(), new LimitChange(id(), 100));
        service.hold(request);
        try (PausedWrite pause = pauseFirstWrite(request.getOperationId())) {
            final Future<?> first = finalizeOperation(request, COMMIT);
            pause.awaitEntered();
            final Future<?> second = hold(request);
            awaitBlockedLock();
            pause.close();
            first.get(10, SECONDS);
            assertInstanceOf(OperationAlreadyInFinalState.class, failure(second));
        }
        assertStates(request.getOperationId(), HOLD, COMMIT);
    }

    @Test
    void failedHoldRollsBackAndWaitingRequestCanSave() throws Exception {
        String operationId = id();
        String name = id();
        try (PausedWrite pause = pauseFirstWrite(operationId, invocation -> {
            invocation.callRealMethod();
            throw new IllegalStateException("Failure after writing history");
        })) {
            final Future<?> first = hold(request(operationId,
                    new LimitChange(name, 100).setContext(Map.of("owner", "failed"))));
            pause.awaitEntered();
            final Future<?> second = hold(request(operationId,
                    new LimitChange(name, 200).setContext(Map.of("owner", "saved"))));
            awaitBlockedLock();
            pause.close();
            assertInstanceOf(IllegalStateException.class, failure(first));
            second.get(10, SECONDS);
        }
        assertStates(operationId, HOLD);
        assertEquals(200, history(operationId).get(0).getOperationValue());
        assertEquals("{\"owner\":\"saved\"}", contextDao.getLimitContext(limitDataDao.get(name).getId()).getContext());
    }

    @Test
    void checkedExceptionRollsBackFinalizationAndReleasesLock() throws Exception {
        LimitRequest request = request(id(), new LimitChange(id(), 100));
        service.hold(request);
        try (PausedWrite pause = pauseFirstWrite(request.getOperationId(), invocation -> {
            invocation.callRealMethod();
            return new int[0]; // Trigger the handler's checked consistency exception after the insert.
        })) {
            final Future<?> first = finalizeOperation(request, COMMIT);
            pause.awaitEntered();
            final Future<?> second = finalizeOperation(request, ROLLBACK);
            awaitBlockedLock();
            pause.close();
            assertInstanceOf(OperationNotFound.class, failure(first));
            second.get(10, SECONDS);
        }
        assertStates(request.getOperationId(), HOLD, ROLLBACK);
    }

    @Test
    void lockTimeoutDoesNotWriteAndRetrySucceeds() throws Exception {
        LimitRequest request = request(id(), new LimitChange(id(), 100));
        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        final Future<?> owner = executor.submit(() -> new TransactionTemplate(transactionManager)
                .executeWithoutResult(status -> {
                    historyDao.lockOperation(request.getOperationId());
                    locked.countDown();
                    awaitLatch(release);
                }));
        try {
            assertTrue(locked.await(5, SECONDS));
            final Future<?> blocked = hold(request);
            awaitBlockedLock();
            Throwable cause = failure(blocked);
            while (cause != null && !(cause instanceof SQLException)) {
                cause = cause.getCause();
            }
            assertNotNull(cause);
            assertEquals("57014", ((SQLException) cause).getSQLState());
            assertNull(limitDataDao.get(request.getLimitChanges().get(0).getLimitName()));
            assertTrue(history(request.getOperationId()).isEmpty());
        } finally {
            release.countDown();
            owner.get(10, SECONDS);
        }
        service.hold(request);
        assertStates(request.getOperationId(), HOLD);
    }

    @Test
    void differentOperationIdsDoNotBlockEachOther() throws Exception {
        String name = id();
        limitDataService.save(new LimitChange(name, 0));
        String firstId = id();
        try (PausedWrite pause = pauseFirstWrite(firstId)) {
            final Future<?> first = hold(request(firstId, new LimitChange(name, 100)));
            pause.awaitEntered();
            hold(request(id(), new LimitChange(name, 200))).get(2, SECONDS);
            pause.close();
            first.get(10, SECONDS);
        }
        assertEquals(300, service.getLastLimitsValues(List.of(name)).get(0).getTotalValue());
    }

    @Test
    void overlappingDuplicateDoesNotCreateExtraLimits() throws Exception {
        String operationId = id();
        String name = id();
        String extra = id();
        service.hold(request(operationId, new LimitChange(name, 100)));
        service.hold(request(operationId, new LimitChange(name, 900), new LimitChange(extra, 200)));
        assertNull(limitDataDao.get(extra));
        assertStates(operationId, HOLD);
        assertEquals(100, history(operationId).get(0).getOperationValue());
    }

    @Test
    void disjointLimitsCanReuseOperationId() throws Exception {
        String operationId = id();
        service.hold(request(operationId, new LimitChange(id(), 100)));
        service.hold(request(operationId, new LimitChange(id(), 200)));
        assertStates(operationId, HOLD, HOLD);
    }

    @Test
    void holdAfterFinalizationDoesNotCreateExtraLimits() throws Exception {
        String operationId = id();
        String name = id();
        String extra = id();
        LimitRequest request = request(operationId, new LimitChange(name, 100));
        service.hold(request);
        service.commit(request);
        assertThrows(OperationAlreadyInFinalState.class,
                () -> service.hold(request(operationId, new LimitChange(name, 100), new LimitChange(extra, 200))));
        assertNull(limitDataDao.get(extra));
        assertStates(operationId, HOLD, COMMIT);
    }

    private void synchronizeInitialLimitReads(String name) {
        CyclicBarrier barrier = new CyclicBarrier(2);
        doAnswer(invocation -> {
            Object result = invocation.callRealMethod();
            Collection<String> names = invocation.getArgument(0);
            if (names.contains(name)) {
                barrier.await(5, SECONDS);
            }
            return result;
        }).when(limitDataDao).get(anyCollection());
    }

    private PausedWrite pauseFirstWrite(String operationId) {
        return pauseFirstWrite(operationId, InvocationOnMock::callRealMethod);
    }

    private PausedWrite pauseFirstWrite(String operationId, Answer<?> action) {
        PausedWrite pause = new PausedWrite();
        AtomicBoolean first = new AtomicBoolean(true);
        doAnswer(invocation -> {
            List<OperationStateHistory> records = invocation.getArgument(0);
            if (records.get(0).getOperationId().equals(operationId) && first.compareAndSet(true, false)) {
                pause.entered.countDown();
                awaitLatch(pause.release);
                return action.answer(invocation);
            }
            return invocation.callRealMethod();
        }).when(historyDao).saveBatch(anyList());
        return pause;
    }

    private void awaitBlockedLock() {
        await().atMost(Duration.ofSeconds(2)).pollInterval(Duration.ofMillis(20)).until(() -> dslContext.fetchOne(
                "select count(*) from pg_locks where locktype = 'advisory' " +
                        "and classid = cast(? as oid) and not granted",
                "liminator:operation".hashCode()).get(0, Integer.class) > 0);
    }

    private Future<?> hold(LimitRequest request) {
        return executor.submit(() -> service.hold(request));
    }

    private Future<?> finalizeOperation(LimitRequest request, OperationState state) {
        return executor.submit(() -> {
            if (state == COMMIT) {
                service.commit(request);
            } else {
                service.rollback(request);
            }
            return null;
        });
    }

    private Throwable failure(Future<?> result) {
        return assertThrows(ExecutionException.class, () -> result.get(10, SECONDS)).getCause();
    }

    private List<OperationStateHistory> history(String operationId) {
        return dslContext.selectFrom(OPERATION_STATE_HISTORY)
                .where(OPERATION_STATE_HISTORY.OPERATION_ID.eq(operationId))
                .orderBy(OPERATION_STATE_HISTORY.ID)
                .fetchInto(OperationStateHistory.class);
    }

    private void assertStates(String operationId, OperationState... states) {
        assertEquals(List.of(states), history(operationId).stream().map(OperationStateHistory::getState).toList());
    }

    private static LimitRequest request(String operationId, LimitChange... changes) {
        return new LimitRequest().setOperationId(operationId).setLimitChanges(List.of(changes));
    }

    private static String id() {
        return UUID.randomUUID().toString();
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            if (!latch.await(15, SECONDS)) {
                throw new IllegalStateException("Timed out waiting for the test transaction");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static class PausedWrite implements AutoCloseable {
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        private void awaitEntered() throws InterruptedException {
            assertTrue(entered.await(5, SECONDS));
        }

        @Override
        public void close() {
            release.countDown();
        }
    }
}
