package dev.vality.liminator.service;

import dev.vality.liminator.LimitChange;
import dev.vality.liminator.OperationNotFound;
import dev.vality.liminator.converter.OperationStateHistoryConverter;
import dev.vality.liminator.dao.OperationStateHistoryDao;
import dev.vality.liminator.domain.enums.OperationState;
import dev.vality.liminator.domain.tables.pojos.OperationStateHistory;
import dev.vality.liminator.model.CurrentLimitValue;
import dev.vality.liminator.model.LimitValue;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Service;

import java.util.*;

import static dev.vality.liminator.domain.enums.OperationState.COMMIT;
import static dev.vality.liminator.domain.enums.OperationState.ROLLBACK;
import static java.util.stream.Collectors.toMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class LimitOperationsHistoryService {

    private final OperationStateHistoryDao operationStateHistoryDao;
    private final OperationStateHistoryConverter operationStateHistoryConverter;

    public void lockOperation(String operationId) {
        operationStateHistoryDao.lockOperation(operationId);
    }

    public int[] writeOperations(String operationId,
                                 List<LimitChange> limitChanges,
                                 OperationState state,
                                 Map<String, Long> limitNamesMap) {
        var operationsHistory = operationStateHistoryConverter.convert(operationId, limitChanges, state, limitNamesMap);
        return operationStateHistoryDao.saveBatch(operationsHistory);
    }

    public List<CurrentLimitValue> getCurrentLimitValue(List<String> limitNames) {
        return operationStateHistoryDao.getCurrentValues(limitNames);
    }

    public List<CurrentLimitValue> getCurrentLimitValue(List<String> limitNames, String operationId) {
        return operationStateHistoryDao.getCurrentValues(limitNames, operationId);
    }

    public List<OperationStateHistory> get(String operationId,
                                           Collection<Long> limitIds,
                                           List<OperationState> states) {
        return operationStateHistoryDao.get(operationId, limitIds, states);
    }

    public List<OperationStateHistory> getExistedFinalOperations(String operationId, Collection<Long> limitIds) {
        return operationStateHistoryDao.get(
                operationId,
                limitIds,
                List.of(COMMIT, ROLLBACK)
        );
    }

    public void checkCorrectnessFinalizingOperation(String operationId,
                                                    List<LimitChange> limitChanges,
                                                    OperationState state,
                                                    Map<String, Long> limitNamesMap) throws TException {
        var existedHoldOperations = checkExistedHoldOperations(operationId, limitNamesMap, state);
        if (state == COMMIT) {
            checkCommitValueCorrectness(operationId, limitChanges, existedHoldOperations);
        }
    }

    // For each finalizing operation, there must be a hold operation
    private List<LimitValue> checkExistedHoldOperations(String operationId,
                                                        Map<String, Long> limitNamesMap,
                                                        OperationState state) throws TException {
        Set<String> limitNames = limitNamesMap.keySet();
        var existedHoldOperations =
                operationStateHistoryDao.getHoldLimitValues(limitNames, operationId);
        long distinctHoldLimits = existedHoldOperations.stream()
                .map(LimitValue::getLimitName)
                .distinct()
                .count();
        if (limitNames.size() != existedHoldOperations.size() || distinctHoldLimits != limitNames.size()) {
            log.error("[{}] Count of existed hold operations for limits is not equal to expected (existed size: {}, " +
                            "expected size: {}, existedHoldOperations: {}, operationId: {})",
                    state.getLiteral(),
                    existedHoldOperations.size(),
                    limitNames.size(),
                    existedHoldOperations,
                    operationId);
            throw new OperationNotFound();
        }
        return existedHoldOperations;
    }

    // The absolute commit value must not exceed the hold.
    private void checkCommitValueCorrectness(String operationId,
                                             List<LimitChange> limitChanges,
                                             List<LimitValue> existedHoldOperations) throws TException {
        Map<String, Long> valuesMap = existedHoldOperations.stream()
                .collect(toMap(LimitValue::getLimitName, LimitValue::getOperationValue));
        Optional<LimitChange> incorrectCommitValue = limitChanges.stream()
                .filter(change -> isIncorrectCommitValue(valuesMap, change))
                .findAny();
        if (incorrectCommitValue.isPresent()) {
            log.error("[COMMIT] Received incorrect commit value - hold is less than commit " +
                            "(operationId: {}, change: {})", operationId, incorrectCommitValue.get());
            throw new OperationNotFound();
        }
    }

    private boolean isIncorrectCommitValue(Map<String, Long> valuesMap, LimitChange change) {
        Long holdValue = valuesMap.get(change.getLimitName());
        long commitValue = change.getValue();
        return Math.abs(holdValue) < Math.abs(commitValue);
    }

    // A repeated commit must preserve the absolute value of the existing commit.
    public void checkNewCommitValueCorrectness(String operationId,
                                               LimitChange change,
                                               Long existingCommitValue) throws OperationNotFound {
        if (existingCommitValue == null || Math.abs(existingCommitValue) != Math.abs(change.getValue())) {
            log.error("[COMMIT] Repeated commit value differs from the existing value " +
                            "(operationId: {}, change: {}, existing value: {})",
                    operationId, change, existingCommitValue);
            throw new OperationNotFound();
        }
    }
}
