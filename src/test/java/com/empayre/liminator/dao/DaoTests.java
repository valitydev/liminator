package com.empayre.liminator.dao;

import com.empayre.liminator.config.PostgresqlSpringBootITest;
import com.empayre.liminator.domain.enums.OperationState;
import com.empayre.liminator.domain.tables.pojos.LimitContext;
import com.empayre.liminator.domain.tables.pojos.LimitData;
import com.empayre.liminator.domain.tables.pojos.OperationStateHistory;
import com.empayre.liminator.model.LimitValue;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

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
        List<String> limitNamesList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            String limitName = "Limit-odc-1-" + i;
            String limitId = "Limit-id-odc-1-" + i;
            limitDataDao.save(new LimitData(null, limitName, LocalDate.now(), LocalDateTime.now(), limitId));
            limitNamesList.add(limitName);
        }
        List<OperationStateHistory> operations = new ArrayList<>();
        String operationNameTemplate = "Operation-odc-1-%s";
        for (String limitName : limitNamesList) {
            for (int i = 0; i < 5; i++) {
                operations.add(createOperationHistory(limitName, operationNameTemplate.formatted(i)));
            }
        }
        operationStateHistoryDao.saveBatch(operations);
        operations.clear();

        List<LimitValue> currentLimitValue = operationStateHistoryDao.getCurrentLimitValue(limitNamesList);
        assertEquals(limitNamesList.size(), currentLimitValue.size());
        currentLimitValue.forEach(value -> assertEquals(0, value.getCommitValue()));
        currentLimitValue.forEach(value -> assertEquals(0, value.getRollbackValue()));
        currentLimitValue.forEach(value -> assertNotEquals(0, value.getHoldValue()));
        currentLimitValue.forEach(value -> assertNotEquals(0, getTotal(value)));

        List<String> commitLimitNames = limitNamesList.subList(0, 3);
        String finalizeOperationName = operationNameTemplate.formatted(1);
        for (String limitName : commitLimitNames) {
            var operationHistory = createOperationHistory(
                    limitName,
                    finalizeOperationName,
                    LocalDateTime.now(),
                    OperationState.COMMIT);
            operations.add(operationHistory);
        }
        operationStateHistoryDao.saveBatch(operations);
        operations.clear();

        List<String> rollbackLimitNames = limitNamesList.subList(4, 9);
        for (String limitName : rollbackLimitNames) {
            var operationHistory = createOperationHistory(
                    limitName,
                    finalizeOperationName,
                    LocalDateTime.now(),
                    OperationState.ROLLBACK);
            operations.add(operationHistory);
        }
        operationStateHistoryDao.saveBatch(operations);
        operations.clear();

        List<LimitValue> limitValuesAfterChanges = operationStateHistoryDao.getCurrentLimitValue(limitNamesList);
        List<LimitValue> limitValuesWithCommitData = limitValuesAfterChanges.stream()
                .filter(value -> value.getCommitValue() == 100
                        && value.getHoldValue() == 500
                        && value.getRollbackValue() == 0)
                .toList();
        assertEquals(3, limitValuesWithCommitData.size());

        List<LimitValue> limitValuesAfterRollback = limitValuesAfterChanges.stream()
                .filter(value -> value.getHoldValue() == 500
                        && value.getCommitValue() == 0
                        && value.getRollbackValue() == 100)
                .toList();
        assertEquals(5, limitValuesAfterRollback.size());

        List<LimitValue> limitValuesWithoutChanges = limitValuesAfterChanges.stream()
                .filter(value -> value.getHoldValue() == 500
                        && value.getCommitValue() == 0
                        && value.getRollbackValue() == 0)
                .toList();
        assertEquals(2, limitValuesWithoutChanges.size());
    }

    private long getTotal(LimitValue value) {
        return value.getHoldValue() - value.getCommitValue() - value.getRollbackValue();
    }

    @Test
    void operationDaoCurrentLimitWithOperationIdTest() {
        String limitName = "Limit-odc-2";
        String limitId = "Limit-id-odc-2";
        Long id = limitDataDao.save(new LimitData(null, limitName, LocalDate.now(), LocalDateTime.now(), limitId));
        List<OperationStateHistory> operations = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            var operation = createOperationHistory(
                    limitName,
                    "Operation-odc-2-%s-%s".formatted(id, i),
                    LocalDateTime.now().minusMinutes(11L - i),
                    OperationState.HOLD);
            operationStateHistoryDao.save(operation);
            operations.add(operation);
        }

        List<LimitValue> valuesForFifthOperation =
                operationStateHistoryDao.getCurrentLimitValue(List.of(limitName), operations.get(2).getOperationId());
        LimitValue limitValue = valuesForFifthOperation.get(0);
        assertEquals(300, getTotal(limitValue));

        valuesForFifthOperation =
                operationStateHistoryDao.getCurrentLimitValue(List.of(limitName), operations.get(5).getOperationId());
        assertEquals(600, getTotal(valuesForFifthOperation.get(0)));
    }

    private OperationStateHistory createOperationHistory(String limitName, String operationId) {
        return createOperationHistory(limitName, operationId, LocalDateTime.now(), OperationState.HOLD);
    }

    private OperationStateHistory createOperationHistory(String limitName,
                                                         String operationId,
                                                         LocalDateTime createdAt,
                                                         OperationState state) {
        OperationStateHistory operation = new OperationStateHistory();
        operation.setLimitName(limitName);
        operation.setOperationId(operationId);
        operation.setState(state);
        operation.setOperationValue(100L);
        operation.setCreatedAt(createdAt);
        return operation;
    }
}
