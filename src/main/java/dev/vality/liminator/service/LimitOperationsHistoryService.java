package dev.vality.liminator.service;

import dev.vality.liminator.LimitChange;
import dev.vality.liminator.LimitRequest;
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
import java.util.stream.Collectors;

import static dev.vality.liminator.domain.enums.OperationState.COMMIT;
import static dev.vality.liminator.domain.enums.OperationState.ROLLBACK;
import static java.util.stream.Collectors.toMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class LimitOperationsHistoryService {

    private final OperationStateHistoryDao operationStateHistoryDao;
    private final OperationStateHistoryConverter operationStateHistoryConverter;

    public int[] writeOperations(LimitRequest request,
                                 OperationState state,
                                 Map<String, Long> limitNamesMap) {
        var operationsHistory = operationStateHistoryConverter.convert(request, state, limitNamesMap);
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

    public List<OperationStateHistory> getExistedFinalOperations(LimitRequest request,
                                                                 Map<String, Long> limitNamesMap) {
        String operationId = request.getOperationId();
        var limitIds = limitNamesMap.values();
        return operationStateHistoryDao.get(operationId, limitIds, List.of(COMMIT, ROLLBACK));
    }

    public void checkCorrectnessFinalizingOperation(LimitRequest request,
                                                    Map<String, Long> limitNamesMap,
                                                    OperationState state) throws TException {
        var existedHoldOperations = checkExistedHoldOperations(request, limitNamesMap, state);
        if (state == COMMIT) {
            checkCommitValueCorrectness(request, existedHoldOperations, limitNamesMap, state);
        }
    }

    // For each finalizing operation, there must be a hold operation
    private List<LimitValue> checkExistedHoldOperations(LimitRequest request,
                                                        Map<String, Long> limitNamesMap,
                                                        OperationState state) throws TException {
        Set<String> limitNames = limitNamesMap.keySet();
        var existedHoldOperations =
                operationStateHistoryDao.getHoldLimitValues(limitNames, request.getOperationId());
        if (limitNames.size() != existedHoldOperations.size()) {
            log.error("[{}] Count of existed hold operations for limits is not equal to expected (existed size: {}, " +
                            "expected size: {}, request: {})", state.getLiteral(), existedHoldOperations,
                    limitNames.size(), request);
            throw new OperationNotFound();
        }
        return existedHoldOperations;
    }

    // The commit value must be less than the hold
    private void checkCommitValueCorrectness(LimitRequest request,
                                             List<LimitValue> existedHoldOperations,
                                             Map<String, Long> limitNamesMap,
                                             OperationState state) throws TException {
        Map<String, Long> valuesMap = existedHoldOperations.stream()
                .collect(toMap(LimitValue::getLimitName, LimitValue::getOperationValue));
        Optional<LimitChange> incorrectCommitValue = request.getLimitChanges().stream()
                .filter(change -> isIncorrectCommitValue(valuesMap, change))
                .findAny();
        if (incorrectCommitValue.isPresent()) {
            log.error("[{}] Received incorrect commit value - hold is less than commit (existed size: {}, " +
                            "expected size: {}, request: {})", state.getLiteral(), existedHoldOperations,
                    limitNamesMap.keySet().size(), request);
            throw new OperationNotFound();
        }
    }

    private boolean isIncorrectCommitValue(Map<String, Long> valuesMap, LimitChange change) {
        Long holdValue = valuesMap.get(change.getLimitName());
        long commitValue = change.getValue();
        return Math.abs(holdValue) < Math.abs(commitValue);
    }

    // The new commit value must be equal existed commit value
    public void checkNewCommitValuesCorrectness(LimitRequest request,
                                                List<OperationStateHistory> existedCommitOperations,
                                                Map<String, Long> limitNamesMap,
                                                OperationState state) throws TException {
        Map<Long, String> limitIdsMap =
                limitNamesMap.entrySet()
                        .stream()
                        .collect(toMap(Map.Entry::getValue, Map.Entry::getKey));
        Map<String, Long> existedCommitValuesMap = existedCommitOperations.stream()
                .collect(toMap(op -> limitIdsMap.get(op.getLimitDataId()), OperationStateHistory::getOperationValue));
        Optional<LimitChange> incorrectCommitValue = request.getLimitChanges().stream()
                .filter(change -> isIncorrectNewCommitValue(existedCommitValuesMap, change))
                .findAny();
        if (incorrectCommitValue.isPresent()) {
            log.error("[{}] Received incorrect commit value - existed commit value is not equal to new commit value " +
                            "( request: {})",
                    state.getLiteral(), request);
            throw new OperationNotFound();
        }
    }

    private boolean isIncorrectNewCommitValue(Map<String, Long> existedCommitValuesMap, LimitChange change) {
        Long existedCommitValue = existedCommitValuesMap.get(change.getLimitName());
        long commitValue = change.getValue();
        return Math.abs(existedCommitValue) != Math.abs(commitValue);
    }
}
