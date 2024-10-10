package com.empayre.liminator.service;

import com.empayre.liminator.config.PostgresqlSpringBootITest;
import dev.vality.liminator.*;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@PostgresqlSpringBootITest
class LiminatorServiceTest {

    @Autowired
    private LiminatorService liminatorService;

    @Test
    void createLimitTest() throws TException {
        String limitName = "TestLimitCreate";
        long holdValue = 123L;
        String limitId = "limitId";
        LimitRequest request = new LimitRequest()
                .setOperationId("operationId")
                .setLimitChanges(List.of(
                                new LimitChange()
                                        .setLimitName(limitName)
                                        .setLimitId(limitId)
                                        .setValue(holdValue)
                                        .setContext(Map.of("test", "test"))
                        )
                );

        List<LimitResponse> response = liminatorService.hold(request);
        assertEquals(limitName, response.get(0).getLimitName());
        assertEquals(limitId, response.get(0).getLimitId());
        assertEquals(holdValue, response.get(0).getTotalValue());
        assertEquals(0, response.get(0).getCommitValue());
    }

    @Test
    void operationAlreadyFinaleStateTest() throws TException {
        String limitName = "TestLimitCommit";
        String operationId = "OpComit";
        LimitRequest holdRequest = new LimitRequest()
                .setOperationId(operationId)
                .setLimitChanges(List.of(new LimitChange(limitName, 500L)));

        liminatorService.hold(holdRequest);

        liminatorService.commit(holdRequest);

        assertThrows(OperationAlreadyInFinalState.class, () -> liminatorService.hold(holdRequest));
    }

    @Test
    void limitNotFoundTest() {
        String limitName = "TestLimitCommit";
        String operationId = "OpComit";
        LimitRequest holdRequest = new LimitRequest()
                .setOperationId(operationId)
                .setLimitChanges(List.of(new LimitChange(limitName, 500L)));

        assertThrows(LimitNotFound.class, () -> liminatorService.rollback(holdRequest));
    }

    @Test
    void operationNotFoundTest() throws TException {
        String limitName = "TestLimitCommit";
        String operationId = "OpComit";
        LimitRequest holdRequest = new LimitRequest()
                .setOperationId(operationId)
                .setLimitChanges(List.of(new LimitChange(limitName, 500L)));

        liminatorService.hold(holdRequest);

        liminatorService.commit(holdRequest);

        assertThrows(OperationNotFound.class, () -> liminatorService.rollback(holdRequest));
    }

    @Test
    void holdValueTest() throws TException {
        String limitName = "TestLimitHold";
        String operationId = "OpHold";
        LimitRequest holdRequest = new LimitRequest()
                .setOperationId(operationId)
                .setLimitChanges(List.of(new LimitChange(limitName, 500L)));

        List<LimitResponse> holdResponse = liminatorService.hold(holdRequest);

        assertEquals(1, holdResponse.size());
        LimitResponse response = holdResponse.get(0);
        assertEquals(500, response.getTotalValue());
        assertEquals(0, response.getCommitValue());
        assertEquals(limitName, response.getLimitName());
    }

    @Test
    void holdFewValueTest() throws TException {
        String limitNameFirst = "TestLimitHold";
        String limitNameSecond = "TestLimitHold2";
        String operationId = "OpHold";
        LimitRequest holdRequest = new LimitRequest()
                .setOperationId(operationId)
                .setLimitChanges(List.of(
                        new LimitChange(limitNameFirst, 500L),
                        new LimitChange(limitNameSecond, 500L))
                );

        List<LimitResponse> holdResponse = liminatorService.hold(holdRequest);

        assertEquals(2, holdResponse.size());
        LimitResponse limitResponseFirst = holdResponse.stream()
                .filter(limitResponse -> limitResponse.getLimitName().equals(limitNameFirst))
                .findFirst()
                .get();
        assertEquals(500, limitResponseFirst.getTotalValue());
        assertEquals(0, limitResponseFirst.getCommitValue());
        assertEquals(limitNameFirst, limitResponseFirst.getLimitName());
        LimitResponse limitResponseSecond = holdResponse.stream()
                .filter(limitResponse -> limitResponse.getLimitName().equals(limitNameSecond))
                .findFirst()
                .get();
        assertEquals(500, limitResponseSecond.getTotalValue());
        assertEquals(0, limitResponseSecond.getCommitValue());
        assertEquals(limitNameSecond, limitResponseSecond.getLimitName());
    }

    @Test
    void commitValueTest() throws TException {
        String limitName = "TestLimitCommit";
        String operationId = "OpComit";
        LimitRequest holdRequest = new LimitRequest()
                .setOperationId(operationId)
                .setLimitChanges(List.of(new LimitChange(limitName, 500L)));
        liminatorService.hold(holdRequest);
        liminatorService.commit(holdRequest);

        List<LimitResponse> limitResponses = liminatorService.getLastLimitsValues(List.of(limitName));

        assertEquals(1, limitResponses.size());
        assertEquals(0, limitResponses.get(0).getTotalValue());
        assertEquals(500, limitResponses.get(0).getCommitValue());
        assertEquals(limitName, limitResponses.get(0).getLimitName());
    }

    @Test
    void rollbackValueTest() throws TException {
        String limitName = "TestLimitRollback";
        String operationId = "Op-112";
        LimitRequest holdRequest = new LimitRequest()
                .setOperationId(operationId)
                .setLimitChanges(List.of(new LimitChange(limitName, 500L)));
        liminatorService.hold(holdRequest);
        liminatorService.rollback(holdRequest);

        List<LimitResponse> limitResponses = liminatorService.getLastLimitsValues(List.of(limitName));

        assertEquals(1, limitResponses.size());
        assertEquals(0, limitResponses.get(0).getTotalValue());
        assertEquals(0, limitResponses.get(0).getCommitValue());
        assertEquals(limitName, limitResponses.get(0).getLimitName());
    }

    @Test
    void complexOperationsTest() throws TException {
        String limitName = "TestLimitRollback";
        String operationId = "Op-112-%s";
        LimitRequest firstHoldRequest = new LimitRequest()
                .setOperationId(operationId.formatted(1))
                .setLimitChanges(List.of(new LimitChange(limitName, 100L)));
        liminatorService.hold(firstHoldRequest);
        LimitRequest secondHoldRequest = new LimitRequest()
                .setOperationId(operationId.formatted(2))
                .setLimitChanges(List.of(new LimitChange(limitName, 100L)));
        liminatorService.hold(secondHoldRequest);

        liminatorService.commit(secondHoldRequest);

        LimitRequest thirdHoldRequest = new LimitRequest()
                .setOperationId(operationId.formatted(3))
                .setLimitChanges(List.of(new LimitChange(limitName, 100L)));
        liminatorService.hold(thirdHoldRequest);

        LimitRequest fourthHoldRequest = new LimitRequest()
                .setOperationId(operationId.formatted(4))
                .setLimitChanges(List.of(new LimitChange(limitName, 100L)));

        List<LimitResponse> limitResponseAfterFourthHold = liminatorService.hold(fourthHoldRequest);

        assertEquals(1, limitResponseAfterFourthHold.size());
        assertEquals(300, limitResponseAfterFourthHold.get(0).getTotalValue());
        assertEquals(100, limitResponseAfterFourthHold.get(0).getCommitValue());
        assertEquals(limitName, limitResponseAfterFourthHold.get(0).getLimitName());

        liminatorService.rollback(firstHoldRequest);

        LimitRequest fifthHoldRequest = new LimitRequest()
                .setOperationId(operationId.formatted(4))
                .setLimitChanges(List.of(new LimitChange(limitName, 100L)));
        liminatorService.hold(fifthHoldRequest);

        List<LimitResponse> limitResponses = liminatorService.hold(fifthHoldRequest);

        assertEquals(1, limitResponses.size());
        assertEquals(300, limitResponses.get(0).getTotalValue());
        assertEquals(100, limitResponses.get(0).getCommitValue());
        assertEquals(limitName, limitResponses.get(0).getLimitName());

        List<LimitResponse> limitResponseAfterAllForFourthHold = liminatorService.hold(fourthHoldRequest);

        assertEquals(1, limitResponseAfterAllForFourthHold.size());
        assertEquals(300, limitResponseAfterAllForFourthHold.get(0).getTotalValue());
        assertEquals(100, limitResponseAfterAllForFourthHold.get(0).getCommitValue());
        assertEquals(limitName, limitResponseAfterAllForFourthHold.get(0).getLimitName());
    }
}
