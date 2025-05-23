package dev.vality.liminator.dao;

import dev.vality.liminator.config.PostgresqlSpringBootITest;
import dev.vality.liminator.domain.enums.OperationState;
import dev.vality.liminator.domain.tables.pojos.LimitContext;
import dev.vality.liminator.domain.tables.pojos.LimitData;
import dev.vality.liminator.domain.tables.pojos.OperationStateHistory;
import dev.vality.liminator.model.CurrentLimitValue;
import dev.vality.liminator.model.LimitValue;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Slf4j
@PostgresqlSpringBootITest
class DaoTests {

    @Autowired
    private LimitDataDao limitDataDao;

    @Autowired
    private LimitContextDao limitContextDao;

    @Autowired
    private OperationStateHistoryDao operationStateHistoryDao;

    @Test
    void limitDataDaoTest() {
        List<String> limitNames = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            String limitName = "Limit-" + i;
            String limitId = "limit-id-" + i;
            limitDataDao.save(new LimitData(null, limitName, LocalDate.now(), LocalDateTime.now(), limitId));
            limitNames.add(limitName);
        }

        List<LimitData> limitDataList = limitDataDao.get(limitNames);
        assertEquals(limitNames.size(), limitDataList.size());
    }

    @Test
    void limitContextDaoTest() {
        LimitContext limitContext = new LimitContext();
        long limitId = 123L;
        limitContext.setLimitDataId(limitId);
        limitContext.setContext("{\"provider\":\"test\"}");
        limitContextDao.save(limitContext);
        LimitContext result = limitContextDao.getLimitContext(limitId);
        assertNotNull(result);
    }

    @Test
    void operationDaoHistoryTest() {
        var limitsList = new ArrayList<Pair<String, Long>>();
        for (int i = 0; i < 10; i++) {
            String limitName = "Limit-odc-1-" + i;
            String limitId = "Limit-id-odc-1-" + i;
            Long id = limitDataDao.save(new LimitData(null, limitName, LocalDate.now(), LocalDateTime.now(), limitId));
            limitsList.add(Pair.of(limitName, id));
        }
        List<OperationStateHistory> operations = new ArrayList<>();
        String operationNameTemplate = "Operation-odc-1-%s";
        for (Pair<String, Long> limit : limitsList) {
            for (int i = 0; i < 5; i++) {
                operations.add(
                        createOperationHistory(
                                limit.getKey(),
                                limit.getValue(),
                                operationNameTemplate.formatted(i))
                );
            }
        }
        operationStateHistoryDao.saveBatch(operations);


        List<String> limitNames = limitsList.stream().map(Pair::getKey).toList();
        List<LimitValue> currentLimitValue = operationStateHistoryDao.getLimitHistory(limitNames);
        assertEquals(operations.size(), currentLimitValue.size());
        currentLimitValue.forEach(value -> assertEquals(100, value.getOperationValue()));

        operations.clear();
        var commitLimits = limitsList.subList(0, 3);
        String finalizeOperationName = operationNameTemplate.formatted(1);
        for (Pair<String, Long> commitLimit : commitLimits) {
            var operationHistory = createOperationHistory(
                    commitLimit.getKey(),
                    commitLimit.getValue(),
                    finalizeOperationName,
                    LocalDateTime.now(),
                    OperationState.COMMIT);
            operations.add(operationHistory);
        }
        operationStateHistoryDao.saveBatch(operations);
        operations.clear();

        var rollbackLimits = limitsList.subList(4, 9);
        for (Pair<String, Long> rollbackLimit : rollbackLimits) {
            var operationHistory = createOperationHistory(
                    rollbackLimit.getKey(),
                    rollbackLimit.getValue(),
                    finalizeOperationName,
                    LocalDateTime.now(),
                    OperationState.ROLLBACK);
            operations.add(operationHistory);
        }
        operationStateHistoryDao.saveBatch(operations);
        operations.clear();

        List<LimitValue> limitValuesAfterChanges = operationStateHistoryDao.getLimitHistory(limitNames);

        List<LimitValue> operationsWithCommitData = limitValuesAfterChanges.stream()
                .filter(value -> value.getState() == OperationState.COMMIT)
                .toList();
        assertEquals(commitLimits.size(), operationsWithCommitData.size());

        List<LimitValue> operationsWithRollback = limitValuesAfterChanges.stream()
                .filter(value -> value.getState() == OperationState.ROLLBACK)
                .toList();
        assertEquals(rollbackLimits.size(), operationsWithRollback.size());

        List<LimitValue> operationsWithHold = limitValuesAfterChanges.stream()
                .filter(value -> value.getState() == OperationState.HOLD)
                .toList();
        assertEquals(currentLimitValue.size(), operationsWithHold.size());
    }

    // total = hold1 + hold2 + hold3 + commit4 + commit5
    // -> received commit for hold2
    // total = hold1 + hold3 + commit4 + commit5 + commit2
    @Test
    void getCurrentValuesTest() {
        var limitsMap = new HashMap<String, Long>();
        for (int i = 0; i < 1; i++) {
            String limitName = "Limit-CV-" + i;
            String limitId = "Limit-CV-ID-" + i;
            Long id = limitDataDao.save(new LimitData(null, limitName, LocalDate.now(), LocalDateTime.now(), limitId));
            limitsMap.put(limitName, id);
        }
        Map<String, List<OperationStateHistory>> holdOperationsMap = new HashMap<>();
        String operationNameTemplate = "%s-operation-%s";
        for (String limitName : limitsMap.keySet()) {
            List<OperationStateHistory> holdOperations = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                OperationStateHistory operationHistory = createOperationHistory(
                        limitName,
                        limitsMap.get(limitName),
                        operationNameTemplate.formatted(limitName, i));
                holdOperations.add(operationHistory);
                operationStateHistoryDao.save(operationHistory);
            }
            holdOperationsMap.put(limitName, holdOperations);
        }

        OperationStateHistory firstCommitOp = null;
        for (String limitName : holdOperationsMap.keySet()) {
            List<OperationStateHistory> holdOperations = holdOperationsMap.get(limitName);
            for (OperationStateHistory holdOperation : holdOperations.subList(0, 2)) {
                var commitOperation = createOperationHistory(
                        limitName,
                        limitsMap.get(limitName),
                        holdOperation.getOperationId(),
                        LocalDateTime.now(),
                        OperationState.COMMIT);
                if (firstCommitOp == null) {
                    firstCommitOp = commitOperation;
                }
                operationStateHistoryDao.save(commitOperation);
            }
        }

        List<String> limitNames = holdOperationsMap.keySet().stream().toList();
        List<CurrentLimitValue> currentValues = operationStateHistoryDao.getCurrentValues(limitNames);
        CurrentLimitValue currentLimitValue = currentValues.get(0);
        assertEquals(200, currentLimitValue.getCommitValue());
        assertEquals(300, currentLimitValue.getHoldValue());

        String firstLimitName = limitNames.get(0);
        List<OperationStateHistory> holds = holdOperationsMap.get(firstLimitName);
        OperationStateHistory holdOperation = holds.get(holds.size() - 2);
        List<CurrentLimitValue> middleCurrentValues =
                operationStateHistoryDao.getCurrentValues(List.of(firstLimitName), holdOperation.getOperationId());
        CurrentLimitValue middleCurrentLimitValue = middleCurrentValues.get(0);
        assertEquals(0, middleCurrentLimitValue.getCommitValue());
        assertEquals(400, middleCurrentLimitValue.getHoldValue());

        List<CurrentLimitValue> firstCommitCurrentValues =
                operationStateHistoryDao.getCurrentValues(List.of(firstLimitName), firstCommitOp.getOperationId());
        CurrentLimitValue firstCurrentLimitValue = firstCommitCurrentValues.get(0);
        assertEquals(100, firstCurrentLimitValue.getCommitValue());
        assertEquals(400, firstCurrentLimitValue.getHoldValue());
    }

    private OperationStateHistory createOperationHistory(String limitName, Long id, String operationId) {
        return createOperationHistory(limitName, id, operationId, LocalDateTime.now(), OperationState.HOLD);
    }

    private OperationStateHistory createOperationHistory(String limitName,
                                                         Long id,
                                                         String operationId,
                                                         LocalDateTime createdAt,
                                                         OperationState state) {
        OperationStateHistory operation = new OperationStateHistory();
        operation.setLimitDataId(id);
        operation.setOperationId(operationId);
        operation.setState(state);
        operation.setOperationValue(100L);
        operation.setCreatedAt(createdAt);
        return operation;
    }
}
