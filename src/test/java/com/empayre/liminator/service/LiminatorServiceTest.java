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
        String operationId = "Op-123";
        LimitRequest holdRequest = new LimitRequest()
                .setOperationId(operationId)
                .setLimitChanges(List.of(new LimitChange(limitName, 500L)));

        assertThrows(LimitNotFound.class, () -> liminatorService.rollback(holdRequest));
    }

    @Test
    void operationNotFoundWithNotExistHoldTest() throws TException {
        String limitName = "TestLimitCommit";
        String operationId = "Op-123";
        LimitRequest holdRequest = new LimitRequest()
                .setOperationId(operationId)
                .setLimitChanges(List.of(new LimitChange(limitName, 500L)));

        liminatorService.hold(holdRequest);

        liminatorService.commit(holdRequest);

        assertThrows(OperationNotFound.class, () -> liminatorService.rollback(holdRequest));
    }

    @Test
    void operationNotFoundWithNotExpectedHoldCountTest() throws TException {
        String firstLimitName = "TestLimit1";
        String secondLimitName = "TestLimit2";
        String operationId = "Op-123";
        LimitRequest holdRequest = new LimitRequest()
                .setOperationId(operationId)
                .setLimitChanges(List.of(
                        new LimitChange(firstLimitName, 500L),
                        new LimitChange(secondLimitName, 500L)
                ));

        liminatorService.hold(holdRequest);

        LimitRequest commitRequest = new LimitRequest()
                .setOperationId(operationId)
                .setLimitChanges(List.of(
                        new LimitChange(firstLimitName, 500L)
                ));

        liminatorService.commit(commitRequest);

        LimitRequest rollbackRequest = new LimitRequest()
                .setOperationId(operationId)
                .setLimitChanges(List.of(
                        new LimitChange(firstLimitName, 500L),
                        new LimitChange(secondLimitName, 500L)
                ));

        assertThrows(OperationNotFound.class, () -> liminatorService.rollback(rollbackRequest));
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
        String operationId = "Op-123";
        String limitId = "limit_day_id";
        LimitRequest holdRequest = new LimitRequest()
                .setOperationId(operationId)
                .setLimitChanges(List.of(new LimitChange(limitName, 500L).setLimitId(limitId)));

        List<LimitResponse> holdResponses = liminatorService.hold(holdRequest);

        assertEquals(1, holdResponses.size());
        assertEquals(500, holdResponses.get(0).getTotalValue());
        assertEquals(0, holdResponses.get(0).getCommitValue());
        assertEquals(limitName, holdResponses.get(0).getLimitName());

        liminatorService.commit(holdRequest);

        List<LimitResponse> commitResponses = liminatorService.getLastLimitsValues(List.of(limitName));

        assertEquals(1, commitResponses.size());
        assertEquals(0, commitResponses.get(0).getTotalValue());
        assertEquals(500, commitResponses.get(0).getCommitValue());
        assertEquals(limitName, commitResponses.get(0).getLimitName());
        assertEquals(limitId, commitResponses.get(0).getLimitId());
    }

    @Test
    void rollbackValueTest() throws TException {
        String limitName = "TestLimitRollback";
        String operationId = "Op-112";
        LimitRequest holdRequest = new LimitRequest()
                .setOperationId(operationId)
                .setLimitChanges(List.of(new LimitChange(limitName, 500L)));
        List<LimitResponse> holdResponses = liminatorService.hold(holdRequest);

        assertEquals(1, holdResponses.size());
        assertEquals(500, holdResponses.get(0).getTotalValue());
        assertEquals(0, holdResponses.get(0).getCommitValue());
        assertEquals(limitName, holdResponses.get(0).getLimitName());

        liminatorService.rollback(holdRequest);

        List<LimitResponse> rollbackResponses = liminatorService.getLastLimitsValues(List.of(limitName));

        assertEquals(1, rollbackResponses.size());
        assertEquals(0, rollbackResponses.get(0).getTotalValue());
        assertEquals(0, rollbackResponses.get(0).getCommitValue());
        assertEquals(limitName, rollbackResponses.get(0).getLimitName());
    }

    @Test
    void complexOperationsTest() throws TException {
        String limitName = "TestLimitComplex";
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
                .setOperationId(operationId.formatted(5))
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
