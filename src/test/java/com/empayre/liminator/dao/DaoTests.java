package com.empayre.liminator.dao;

import com.empayre.liminator.config.PostgresqlSpringBootITest;
import com.empayre.liminator.domain.enums.OperationState;
import com.empayre.liminator.domain.tables.pojos.LimitContext;
import com.empayre.liminator.domain.tables.pojos.LimitData;
import com.empayre.liminator.domain.tables.pojos.OperationStateHistory;
import com.empayre.liminator.model.LimitValue;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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

    private OperationStateHistory createOperationHistory(String limitName, Long id, String operationId) {
        return createOperationHistory(limitName, id, operationId, LocalDateTime.now(), OperationState.HOLD);
    }

    private OperationStateHistory createOperationHistory(String limitName,
                                                         Long id,
                                                         String operationId,
                                                         LocalDateTime createdAt,
                                                         OperationState state) {
        OperationStateHistory operation = new OperationStateHistory();
        operation.setLimitName(limitName);
        operation.setLimitDataId(id);
        operation.setOperationId(operationId);
        operation.setState(state);
        operation.setOperationValue(100L);
        operation.setCreatedAt(createdAt);
        return operation;
    }
}
