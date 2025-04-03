package dev.vality.liminator.service;

import dev.vality.liminator.*;
import dev.vality.liminator.config.PostgresqlSpringBootITest;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

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
        LimitRequest request = new LimitRequest()
                .setOperationId(operationId)
                .setLimitChanges(List.of(new LimitChange(limitName, 500L)));

        assertThrows(LimitNotFound.class, () -> liminatorService.commit(request));
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
    void commitLessThanHoldPart1Test() throws TException {
        String limitName = "TestLimitCommit";
        String operationId = "Op-123";
        LimitRequest holdRequest = new LimitRequest()
                .setOperationId(operationId)
                .setLimitChanges(List.of(new LimitChange(limitName, 500L)));
        liminatorService.hold(holdRequest);

        List<LimitResponse> limitsBeforeCommit = liminatorService.getLastLimitsValues(List.of(limitName));
        assertNotNull(limitsBeforeCommit);
        LimitResponse responseBeforeCommit = limitsBeforeCommit.stream().findFirst().get();
        assertEquals(500L, responseBeforeCommit.getTotalValue());
        assertEquals(0, responseBeforeCommit.getCommitValue());

        LimitRequest commitRequest = new LimitRequest()
                .setOperationId(operationId)
                .setLimitChanges(List.of(new LimitChange(limitName, 300L)));
        liminatorService.commit(commitRequest);

        List<LimitResponse> limitsAfterCommit = liminatorService.getLastLimitsValues(List.of(limitName));
        assertNotNull(limitsAfterCommit);
        LimitResponse response = limitsAfterCommit.stream().findFirst().get();
        assertEquals(300L, response.getCommitValue());
        assertEquals(300L, response.getTotalValue());
    }

    @Test
    void commitLessThanHoldPart2Test() throws TException {
        String limitName = "TestLimitCommit";
        String operationId = "Op-123";
        LimitRequest holdRequest = new LimitRequest()
                .setOperationId(operationId)
                .setLimitChanges(List.of(new LimitChange(limitName, -1000L)));
        liminatorService.hold(holdRequest);

        List<LimitResponse> limitsBeforeCommit = liminatorService.getLastLimitsValues(List.of(limitName));
        assertNotNull(limitsBeforeCommit);
        LimitResponse responseBeforeCommit = limitsBeforeCommit.stream().findFirst().get();
        assertEquals(-1000L, responseBeforeCommit.getTotalValue());
        assertEquals(0, responseBeforeCommit.getCommitValue());

        LimitRequest commitRequest = new LimitRequest()
                .setOperationId(operationId)
                .setLimitChanges(List.of(new LimitChange(limitName, -800L)));
        liminatorService.commit(commitRequest);

        List<LimitResponse> limitsAfterCommit = liminatorService.getLastLimitsValues(List.of(limitName));
        assertNotNull(limitsAfterCommit);
        LimitResponse response = limitsAfterCommit.stream().findFirst().get();
        assertEquals(-800L, response.getCommitValue());
        assertEquals(-800L, response.getTotalValue());
    }

    @Test
    void commitLessThanHoldPart3Test() throws TException {
        String limitName = "TestLimitCommit";
        String operationId = "Op-123";
        LimitRequest holdRequest = new LimitRequest()
                .setOperationId(operationId)
                .setLimitChanges(List.of(new LimitChange(limitName, -1000L)));
        liminatorService.hold(holdRequest);

        List<LimitResponse> limitsBeforeCommit = liminatorService.getLastLimitsValues(List.of(limitName));
        assertNotNull(limitsBeforeCommit);
        LimitResponse responseBeforeCommit = limitsBeforeCommit.stream().findFirst().get();
        assertEquals(-1000L, responseBeforeCommit.getTotalValue());
        assertEquals(0, responseBeforeCommit.getCommitValue());

        LimitRequest commitRequest = new LimitRequest()
                .setOperationId(operationId)
                .setLimitChanges(List.of(new LimitChange(limitName, -1000L)));
        liminatorService.commit(commitRequest);

        List<LimitResponse> limitsAfterCommit = liminatorService.getLastLimitsValues(List.of(limitName));
        assertNotNull(limitsAfterCommit);
        LimitResponse response = limitsAfterCommit.stream().findFirst().get();
        assertEquals(-1000L, response.getCommitValue());
        assertEquals(-1000L, response.getTotalValue());
    }

    @Test
    void commitGrossThanHoldTest() throws TException {
        String limitName = "TestLimitCommit";
        String operationId = "Op-123";
        LimitRequest holdRequest = new LimitRequest()
                .setOperationId(operationId)
                .setLimitChanges(List.of(new LimitChange(limitName, 500L)));

        liminatorService.hold(holdRequest);

        LimitRequest commitRequest = new LimitRequest()
                .setOperationId(operationId)
                .setLimitChanges(List.of(new LimitChange(limitName, 600L)));

        assertThrows(OperationNotFound.class, () -> liminatorService.commit(commitRequest));
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
        assertEquals(500, commitResponses.get(0).getTotalValue());
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

        LimitRequest firstHoldRequest = createRequest(limitName, operationId.formatted(1));
        liminatorService.hold(firstHoldRequest);
        liminatorService.hold(createRequest(limitName, operationId.formatted(2)));
        liminatorService.commit(createRequest(limitName, operationId.formatted(2)));
        liminatorService.hold(createRequest(limitName, operationId.formatted(3)));
        List<LimitResponse> limitResponseAfterFourthHold =
                liminatorService.hold(createRequest(limitName, operationId.formatted(4)));

        // total: 3 hold ops, 1 commit: total - 400, commit - 100
        assertEquals(1, limitResponseAfterFourthHold.size());
        assertEquals(400, limitResponseAfterFourthHold.get(0).getTotalValue());
        assertEquals(100, limitResponseAfterFourthHold.get(0).getCommitValue());
        assertEquals(limitName, limitResponseAfterFourthHold.get(0).getLimitName());

        liminatorService.rollback(firstHoldRequest);
        List<LimitResponse> limitResponses = liminatorService.getLastLimitsValues(List.of(limitName));

        // total: 2 hold ops, 1 commit; total - 300, commit - 100
        assertEquals(1, limitResponses.size());
        assertEquals(300, limitResponses.get(0).getTotalValue());
        assertEquals(100, limitResponses.get(0).getCommitValue());
        assertEquals(limitName, limitResponses.get(0).getLimitName());

        liminatorService.commit(createRequest(limitName, operationId.formatted(3)));
        List<LimitResponse> limitResponseAfterAllForFourthHold =
                liminatorService.getLastLimitsValues(List.of(limitName));

        // total: 1 hold ops, 2 commit;
        assertEquals(1, limitResponseAfterAllForFourthHold.size());
        assertEquals(300, limitResponseAfterAllForFourthHold.get(0).getTotalValue());
        assertEquals(200, limitResponseAfterAllForFourthHold.get(0).getCommitValue());
        assertEquals(limitName, limitResponseAfterAllForFourthHold.get(0).getLimitName());
    }

    @Test
    void holdCommitFewValueTest() throws TException {
        String limitNameFirst = "TestLimitHold";
        String limitNameSecond = "TestLimitHold2";
        String limitNameThird = "TestLimitHold3";
        String operationId = "OpHold";
        LimitRequest holdRequest = new LimitRequest()
                .setOperationId(operationId)
                .setLimitChanges(List.of(
                        new LimitChange(limitNameFirst, 500L),
                        new LimitChange(limitNameSecond, 500L),
                        new LimitChange(limitNameThird, 500L))
                );

        List<LimitResponse> holdResponse = liminatorService.hold(holdRequest);

        assertEquals(3, holdResponse.size());
        LimitResponse limitResponseFirst = getLimitResponseByLimitName(holdResponse, limitNameFirst);
        assertEquals(limitNameFirst, limitResponseFirst.getLimitName());
        assertEquals(500, limitResponseFirst.getTotalValue());
        assertEquals(0, limitResponseFirst.getCommitValue());
        LimitResponse limitResponseSecond = getLimitResponseByLimitName(holdResponse, limitNameSecond);
        assertEquals(limitNameSecond, limitResponseSecond.getLimitName());
        assertEquals(500, limitResponseSecond.getTotalValue());
        assertEquals(0, limitResponseSecond.getCommitValue());
        LimitResponse limitResponseThird = getLimitResponseByLimitName(holdResponse, limitNameThird);
        assertEquals(limitNameThird, limitResponseThird.getLimitName());
        assertEquals(500, limitResponseThird.getTotalValue());
        assertEquals(0, limitResponseThird.getCommitValue());

        liminatorService.commit(holdRequest);
        List<LimitResponse> lastLimitsValues =
                liminatorService.getLastLimitsValues(List.of(limitNameFirst, limitNameSecond, limitNameThird));
        assertEquals(3, lastLimitsValues.size());
        LimitResponse firstLimitResponseAfterCommit = getLimitResponseByLimitName(lastLimitsValues, limitNameFirst);
        assertEquals(limitNameFirst, firstLimitResponseAfterCommit.getLimitName());
        assertEquals(500, firstLimitResponseAfterCommit.getTotalValue());
        assertEquals(500, firstLimitResponseAfterCommit.getCommitValue());
        LimitResponse secondLimitResponseAfterCommit = getLimitResponseByLimitName(lastLimitsValues, limitNameSecond);
        assertEquals(limitNameSecond, secondLimitResponseAfterCommit.getLimitName());
        assertEquals(500, secondLimitResponseAfterCommit.getTotalValue());
        assertEquals(500, secondLimitResponseAfterCommit.getCommitValue());
        LimitResponse thirdLimitResponseAfterCommit = getLimitResponseByLimitName(lastLimitsValues, limitNameThird);
        assertEquals(limitNameThird, thirdLimitResponseAfterCommit.getLimitName());
        assertEquals(500, thirdLimitResponseAfterCommit.getTotalValue());
        assertEquals(500, thirdLimitResponseAfterCommit.getCommitValue());

    }

    @Test
    void commitWithAlreadyExistedCommitWithSameValueTest() throws TException {
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
                        new LimitChange(firstLimitName, 500L),
                        new LimitChange(secondLimitName, 500L)
                ));

        liminatorService.commit(commitRequest);

        LimitRequest newCommitRequest = new LimitRequest()
                .setOperationId(operationId)
                .setLimitChanges(List.of(
                        new LimitChange(firstLimitName, 500L),
                        new LimitChange(secondLimitName, 500L)
                ));

        assertDoesNotThrow(() -> liminatorService.commit(newCommitRequest));
    }

    @Test
    void rollbackWithAlreadyExistedCommitTest() throws TException {
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
                        new LimitChange(firstLimitName, 500L),
                        new LimitChange(secondLimitName, 500L)
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
    void commitWithAlreadyExistedCommitWithDifferentValueTest() throws TException {
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
                        new LimitChange(firstLimitName, 500L),
                        new LimitChange(secondLimitName, 500L)
                ));

        liminatorService.commit(commitRequest);

        LimitRequest newCommitRequest = new LimitRequest()
                .setOperationId(operationId)
                .setLimitChanges(List.of(
                        new LimitChange(firstLimitName, 1000L),
                        new LimitChange(secondLimitName, 1000L)
                ));

        assertThrows(OperationNotFound.class, () -> liminatorService.commit(newCommitRequest));
    }

    @Test
    void commitWithAlreadyExistedRollbackTest() throws TException {
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
                        new LimitChange(firstLimitName, 500L),
                        new LimitChange(secondLimitName, 500L)
                ));

        liminatorService.rollback(commitRequest);

        LimitRequest rollbackRequest = new LimitRequest()
                .setOperationId(operationId)
                .setLimitChanges(List.of(
                        new LimitChange(firstLimitName, 500L),
                        new LimitChange(secondLimitName, 500L)
                ));

        assertThrows(OperationNotFound.class, () -> liminatorService.commit(rollbackRequest));
    }

    @Test
    void rollbackWithAlreadyExistedRollbackTest() throws TException {
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

        LimitRequest rollbackRequest = new LimitRequest()
                .setOperationId(operationId)
                .setLimitChanges(List.of(
                        new LimitChange(firstLimitName, 500L),
                        new LimitChange(secondLimitName, 500L)
                ));

        liminatorService.rollback(rollbackRequest);

        LimitRequest newRollbackRequest = new LimitRequest()
                .setOperationId(operationId)
                .setLimitChanges(List.of(
                        new LimitChange(firstLimitName, 500L),
                        new LimitChange(secondLimitName, 500L)
                ));

        assertDoesNotThrow(() -> liminatorService.rollback(newRollbackRequest));
    }

    @Test
    void rollbackWithNotExistedHoldTest() {
        String firstLimitName = "TestLimit1";
        String secondLimitName = "TestLimit2";
        String operationId = "Op-123";

        LimitRequest rollbackRequest = new LimitRequest()
                .setOperationId(operationId)
                .setLimitChanges(List.of(
                        new LimitChange(firstLimitName, 500L),
                        new LimitChange(secondLimitName, 500L)
                ));

        assertThrows(InvalidRequest.class, () -> liminatorService.rollback(rollbackRequest));
    }


    private LimitResponse getLimitResponseByLimitName(List<LimitResponse> response, String limitName) {
        return response.stream()
                .filter(limitResponse -> limitResponse.getLimitName().equals(limitName))
                .findFirst()
                .get();
    }

    private LimitRequest createRequest(String limitName, String operationId) {
        return new LimitRequest()
                .setOperationId(operationId)
                .setLimitChanges(List.of(new LimitChange(limitName, 100L)));
    }
}
