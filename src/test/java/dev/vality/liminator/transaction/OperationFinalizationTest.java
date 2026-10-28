package dev.vality.liminator.transaction;

import dev.vality.liminator.DuplicateOperation;
import dev.vality.liminator.InvalidRequest;
import dev.vality.liminator.LimitChange;
import dev.vality.liminator.LimitRequest;
import dev.vality.liminator.OperationNotFound;
import dev.vality.liminator.dao.LimitContextDao;
import dev.vality.liminator.dao.LimitDataDao;
import dev.vality.liminator.dao.OperationStateHistoryDao;
import dev.vality.liminator.domain.enums.OperationState;
import dev.vality.liminator.domain.tables.pojos.OperationStateHistory;
import dev.vality.liminator.service.LimitDataService;
import dev.vality.liminator.service.LiminatorService;
import org.apache.thrift.TException;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static dev.vality.liminator.domain.Tables.OPERATION_STATE_HISTORY;
import static dev.vality.liminator.domain.enums.OperationState.COMMIT;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class OperationFinalizationTest extends AbstractIntegrationTestWithEmbeddedPostgres {

    @Autowired
    private LiminatorService service;
    @Autowired
    private LimitDataService limitDataService;
    @Autowired
    private LimitDataDao limitDataDao;
    @Autowired
    private LimitContextDao contextDao;
    @Autowired
    private DSLContext dslContext;
    @MockitoSpyBean
    private OperationStateHistoryDao historyDao;

    @ParameterizedTest
    @EnumSource(value = OperationState.class, names = {"COMMIT", "ROLLBACK"})
    void partiallyFinalizedSetFinishesRemainingLimitsAndCanBeRepeated(OperationState state) throws Exception {
        String operationId = id();
        LimitChange first = new LimitChange(id(), 100);
        LimitChange second = new LimitChange(id(), 200);
        LimitRequest full = request(operationId, first, second);
        final LimitRequest originalRequest = full.deepCopy();
        service.hold(full);
        apply(request(operationId, first), state);
        List<OperationStateHistory> originalHistory = history(operationId);

        apply(full, state);

        List<OperationStateHistory> completedHistory = history(operationId);
        assertEquals(4, completedHistory.size());
        assertEquals(originalHistory, completedHistory.subList(0, originalHistory.size()));
        assertEquals(2, completedHistory.stream().filter(row -> row.getState() == state).count());
        assertEquals(originalRequest, full);
        apply(full, state);
        assertEquals(completedHistory, history(operationId));
    }

    @Test
    void mixedCommitPreservesZeroAndAbsoluteValueComparison() throws Exception {
        String operationId = id();
        String first = id();
        String second = id();
        service.hold(request(operationId, new LimitChange(first, 100), new LimitChange(second, -200)));
        service.commit(request(operationId, new LimitChange(first, 0)));
        service.commit(request(operationId, new LimitChange(first, 0), new LimitChange(second, -150)));
        List<OperationStateHistory> completedHistory = history(operationId);

        service.commit(request(operationId, new LimitChange(first, 0), new LimitChange(second, 150)));

        assertEquals(completedHistory, history(operationId));
        assertEquals(List.of(0L, -150L), completedHistory.stream()
                .filter(row -> row.getState() == COMMIT).map(OperationStateHistory::getOperationValue).toList());
    }

    @ParameterizedTest
    @EnumSource(value = OperationState.class, names = {"COMMIT", "ROLLBACK"})
    void conflictingFinalStatesRejectEntireSet(OperationState state) throws Exception {
        String operationId = id();
        LimitChange first = new LimitChange(id(), 100);
        LimitChange second = new LimitChange(id(), 200);
        LimitChange pending = new LimitChange(id(), 300);
        service.hold(request(operationId, first, second, pending));
        service.commit(request(operationId, first));
        service.rollback(request(operationId, second));
        List<OperationStateHistory> originalHistory = history(operationId);

        assertThrows(OperationNotFound.class, () -> apply(request(operationId, first, second, pending), state));
        assertThrows(OperationNotFound.class, () -> apply(request(operationId, pending, second, first), state));

        assertEquals(originalHistory, history(operationId));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void invalidCommitValueDoesNotFinalizePendingLimit(boolean invalidRepeatedValue) throws Exception {
        String operationId = id();
        String first = id();
        String second = id();
        service.hold(request(operationId, new LimitChange(first, 100), new LimitChange(second, 200)));
        service.commit(request(operationId, new LimitChange(first, 50)));
        List<OperationStateHistory> originalHistory = history(operationId);

        LimitRequest invalid = request(operationId,
                new LimitChange(first, invalidRepeatedValue ? 51 : 50),
                new LimitChange(second, invalidRepeatedValue ? 100 : 201));
        assertThrows(OperationNotFound.class, () -> service.commit(invalid));

        assertEquals(originalHistory, history(operationId));
    }

    @ParameterizedTest
    @EnumSource(value = OperationState.class, names = {"COMMIT", "ROLLBACK"})
    void missingHoldRejectsRemainingLimitsWithoutChanges(OperationState state) throws Exception {
        String operationId = id();
        LimitChange first = new LimitChange(id(), 100);
        LimitChange pending = new LimitChange(id(), 200);
        LimitChange withoutHold = new LimitChange(id(), 300);
        service.hold(request(operationId, first, pending));
        limitDataService.save(withoutHold);
        apply(request(operationId, first), state);
        List<OperationStateHistory> originalHistory = history(operationId);

        assertThrows(OperationNotFound.class,
                () -> apply(request(operationId, first, pending, withoutHold), state));

        assertEquals(originalHistory, history(operationId));
    }

    @ParameterizedTest
    @EnumSource(OperationState.class)
    void duplicateLimitNamesAreRejectedBeforeLockAndPreserveExistingData(OperationState state) throws Exception {
        String operationId = id();
        String name = id();
        String extra = id();
        service.hold(request(operationId,
                new LimitChange(name, 100).setContext(Map.of("owner", "original"))));
        var originalLimit = limitDataDao.get(name);
        final var originalContext = contextDao.getLimitContext(originalLimit.getId());
        final var originalHistory = history(operationId);
        clearInvocations(historyDao);
        LimitRequest duplicate = request(operationId,
                new LimitChange(name, 100).setContext(Map.of("owner", "changed")),
                new LimitChange(name, 200),
                new LimitChange(extra, 300).setContext(Map.of("owner", "extra")));
        Class<? extends TException> expected = switch (state) {
            case HOLD -> DuplicateOperation.class;
            case COMMIT, ROLLBACK -> InvalidRequest.class;
        };

        assertThrows(expected, () -> apply(duplicate, state));

        verify(historyDao, never()).lockOperation(anyString());
        assertEquals(originalLimit, limitDataDao.get(name));
        assertEquals(originalContext, contextDao.getLimitContext(originalLimit.getId()));
        assertEquals(originalHistory, history(operationId));
        assertNull(limitDataDao.get(extra));
    }

    @Test
    void duplicateHoldDoesNotCreateLimitsOrHistory() {
        String operationId = id();
        String name = id();
        String extra = id();

        assertThrows(DuplicateOperation.class, () -> service.hold(request(operationId,
                new LimitChange(name, 100).setContext(Map.of("owner", "first")),
                new LimitChange(name, 200), new LimitChange(extra, 300))));

        verify(historyDao, never()).lockOperation(anyString());
        assertNull(limitDataDao.get(name));
        assertNull(limitDataDao.get(extra));
        assertTrue(history(operationId).isEmpty());
    }

    @ParameterizedTest
    @EnumSource(value = OperationState.class, names = {"COMMIT", "ROLLBACK"})
    void duplicateFinalHistoryReturnsDomainError(OperationState state) throws Exception {
        String operationId = id();
        LimitRequest request = request(operationId, new LimitChange(id(), 100));
        service.hold(request);
        apply(request, state);
        var duplicate = new OperationStateHistory(history(operationId).get(1));
        duplicate.setId(null);
        historyDao.save(duplicate);
        var originalHistory = history(operationId);

        assertThrows(OperationNotFound.class, () -> apply(request, state));

        assertEquals(originalHistory, history(operationId));
    }

    @ParameterizedTest
    @EnumSource(value = OperationState.class, names = {"COMMIT", "ROLLBACK"})
    void duplicateHoldCannotMaskMissingHoldForAnotherLimit(OperationState state) throws Exception {
        String operationId = id();
        LimitChange first = new LimitChange(id(), 100);
        LimitChange missing = new LimitChange(id(), 200);
        service.hold(request(operationId, first));
        limitDataService.save(missing);
        var duplicate = new OperationStateHistory(history(operationId).get(0));
        duplicate.setId(null);
        historyDao.save(duplicate);
        var originalHistory = history(operationId);

        assertThrows(OperationNotFound.class, () -> apply(request(operationId, first, missing), state));

        assertEquals(originalHistory, history(operationId));
    }

    private void apply(LimitRequest request, OperationState state) throws TException {
        switch (state) {
            case HOLD -> service.hold(request);
            case COMMIT -> service.commit(request);
            case ROLLBACK -> service.rollback(request);
            default -> throw new IllegalArgumentException("Unsupported state: " + state);
        }
    }

    private List<OperationStateHistory> history(String operationId) {
        return dslContext.selectFrom(OPERATION_STATE_HISTORY)
                .where(OPERATION_STATE_HISTORY.OPERATION_ID.eq(operationId))
                .orderBy(OPERATION_STATE_HISTORY.ID)
                .fetchInto(OperationStateHistory.class);
    }

    private static LimitRequest request(String operationId, LimitChange... changes) {
        return new LimitRequest().setOperationId(operationId).setLimitChanges(List.of(changes));
    }

    private static String id() {
        return UUID.randomUUID().toString();
    }
}
