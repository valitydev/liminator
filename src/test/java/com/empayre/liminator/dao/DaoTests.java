package com.empayre.liminator.dao;

import com.empayre.liminator.config.PostgresqlSpringBootITest;
import com.empayre.liminator.domain.enums.OperationState;
import com.empayre.liminator.domain.tables.pojos.LimitContext;
import com.empayre.liminator.domain.tables.pojos.LimitData;
import com.empayre.liminator.domain.tables.pojos.Operation;
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
public class DaoTests {

    @Autowired
    private LimitDataDao limitDataDao;

    @Autowired
    private LimitContextDao limitContextDao;

    @Autowired
    private OperationDao operationDao;

    @Test
    public void limitDataDaoTest() {
        List<String> limitNames = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            String limitName = "Limit-" + i;
            limitDataDao.save(new LimitData(null, limitName, LocalDate.now(), LocalDateTime.now()));
            limitNames.add(limitName);
        }

        List<LimitData> limitDataList = limitDataDao.get(limitNames);
        assertEquals(limitNames.size(), limitDataList.size());
    }

    @Test
    public void limitContextDaoTest() {
        LimitContext limitContext = new LimitContext();
        long limitId = 123L;
        limitContext.setLimitId(limitId);
        limitContext.setContext("{\"provider\":\"test\"}");
        limitContextDao.save(limitContext);
        LimitContext result = limitContextDao.getLimitContext(limitId);
        assertNotNull(result);
    }

    @Test
    public void operationDaoTest() {
        List<Long> limitIdsList = new ArrayList<>();
        List<String> limitNamesList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            String limitName = "Limit-odc-1-" + i;
            Long limitId = limitDataDao.save(new LimitData(null, limitName, LocalDate.now(), LocalDateTime.now()));
            limitIdsList.add(limitId);
            limitNamesList.add(limitName);
        }
        List<Operation> operations = new ArrayList<>();
        String operationNameTemplate = "Operation-odc-1-%s";
        for (Long limitId : limitIdsList) {
            for (int i = 0; i < 5; i++) {
                operations.add(createOperation(limitId, operationNameTemplate.formatted(i)));
            }
        }
        operationDao.saveBatch(operations);

        List<LimitValue> currentLimitValue = operationDao.getCurrentLimitValue(limitNamesList);
        assertEquals(10, currentLimitValue.size());
        currentLimitValue.forEach(value -> assertEquals(0, value.getCommitValue()));
        currentLimitValue.forEach(value -> assertNotEquals(0, value.getHoldValue()));

        List<Long> commitLimitIds = limitIdsList.subList(0, 3);
        String finalizeOperationName = operationNameTemplate.formatted(1);
        operationDao.commit(finalizeOperationName, commitLimitIds);

        List<Long> rollbackLimitIds = limitIdsList.subList(4, 9);
        operationDao.rollback(finalizeOperationName, rollbackLimitIds);

        List<LimitValue> limitValuesAfterChanges = operationDao.getCurrentLimitValue(limitNamesList);
        List<LimitValue> limitValuesWithCommitData = limitValuesAfterChanges.stream()
                .filter(value -> value.getCommitValue() == 100 && value.getHoldValue() == 400)
                .toList();
        assertEquals(3, limitValuesWithCommitData.size());

        List<LimitValue> limitValuesAfterRollback = limitValuesAfterChanges.stream()
                .filter(value -> value.getHoldValue() == 400 && value.getCommitValue() == 0)
                .toList();
        assertEquals(5, limitValuesAfterRollback.size());

        List<LimitValue> limitValuesWithoutChanges = limitValuesAfterChanges.stream()
                .filter(value -> value.getCommitValue() == 0 && value.getHoldValue() == 500)
                .toList();
        assertEquals(2, limitValuesWithoutChanges.size());
    }

    @Test
    public void operationDaoCurrentLimitWithOperationIdTest() {
        String limitName = "Limit-odc-2";
        Long limitId = limitDataDao.save(new LimitData(null, limitName, LocalDate.now(), LocalDateTime.now()));
        List<Operation> operations = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Operation operation = createOperation(
                    limitId,
                    "Operation-odc-2-%s-%s".formatted(limitId, i),
                    LocalDateTime.now().minusMinutes(11L - i));
            operationDao.save(operation);
            operations.add(operation);
        }

        List<LimitValue> valuesForFifthOperation =
                operationDao.getCurrentLimitValue(List.of(limitName), operations.get(2).getOperationId());
        LimitValue limitValue = valuesForFifthOperation.get(0);
        assertEquals(300, limitValue.getHoldValue());

        valuesForFifthOperation =
                operationDao.getCurrentLimitValue(List.of(limitName), operations.get(5).getOperationId());
        assertEquals(600, valuesForFifthOperation.get(0).getHoldValue());
    }

    private Operation createOperation(Long limitId, String operationId) {
        return createOperation(limitId, operationId, LocalDateTime.now());
    }

    private Operation createOperation(Long limitId, String operationId, LocalDateTime createdAt) {
        Operation operation = new Operation();
        operation.setLimitId(limitId);
        operation.setOperationId(operationId);
        operation.setState(OperationState.HOLD);
        operation.setOperationValue(100L);
        operation.setCreatedAt(createdAt);
        return operation;
    }
}
