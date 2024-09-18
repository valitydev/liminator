package com.empayre.liminator.service;

import com.empayre.liminator.config.PostgresqlSpringBootITest;
import dev.vality.liminator.CreateLimitRequest;
import dev.vality.liminator.LimitChange;
import dev.vality.liminator.LimitRequest;
import dev.vality.liminator.LimitResponse;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@PostgresqlSpringBootITest
public class LiminatorServiceTest {

    @Autowired
    private LiminatorService liminatorService;

    @Test
    void createLimitTest() throws TException {
        String limitName = "TestLimitCreate";
        CreateLimitRequest request = new CreateLimitRequest()
                .setLimitName(limitName);

        LimitResponse response = liminatorService.create(request);
        assertEquals(limitName, response.getLimitName());
        assertEquals(0, response.getHoldValue());
        assertEquals(0, response.getCommitValue());
    }

    @Test
    void holdValueTest() throws TException {
        String limitName = "TestLimitHold";
        CreateLimitRequest createRequest = new CreateLimitRequest()
                .setLimitName(limitName);
        liminatorService.create(createRequest);

        String operationId = "OpHold";
        LimitRequest holdRequest = new LimitRequest()
                .setOperationId(operationId)
                .setLimitChanges(List.of(new LimitChange(limitName, 500L)));
        List<LimitResponse> holdResponse = liminatorService.hold(holdRequest);
        assertEquals(1, holdResponse.size());
        LimitResponse response = holdResponse.get(0);
        assertEquals(500, response.getHoldValue());
        assertEquals(0, response.getCommitValue());
        assertEquals(limitName, response.getLimitName());
    }

    @Test
    void commitValueTest() throws TException {
        String limitName = "TestLimitCommit";
        CreateLimitRequest createRequest = new CreateLimitRequest()
                .setLimitName(limitName);
        liminatorService.create(createRequest);

        String operationId = "OpComit";
        LimitRequest holdRequest = new LimitRequest()
                .setOperationId(operationId)
                .setLimitChanges(List.of(new LimitChange(limitName, 500L)));
        liminatorService.hold(holdRequest);
        liminatorService.commit(holdRequest);

        List<LimitResponse> limitResponses = liminatorService.getLastLimitsValues(List.of(limitName));
        assertEquals(1, limitResponses.size());
        assertEquals(0, limitResponses.get(0).getHoldValue());
        assertEquals(500, limitResponses.get(0).getCommitValue());
        assertEquals(limitName, limitResponses.get(0).getLimitName());
    }

    @Test
    void rollbackValueTest() throws TException {
        String limitName = "TestLimitRollback";
        CreateLimitRequest createRequest = new CreateLimitRequest()
                .setLimitName(limitName);
        liminatorService.create(createRequest);

        String operationId = "Op-112";
        LimitRequest holdRequest = new LimitRequest()
                .setOperationId(operationId)
                .setLimitChanges(List.of(new LimitChange(limitName, 500L)));
        liminatorService.hold(holdRequest);
        liminatorService.rollback(holdRequest);

        List<LimitResponse> limitResponses = liminatorService.getLastLimitsValues(List.of(limitName));
        assertEquals(1, limitResponses.size());
        assertEquals(0, limitResponses.get(0).getHoldValue());
        assertEquals(0, limitResponses.get(0).getCommitValue());
        assertEquals(limitName, limitResponses.get(0).getLimitName());
    }
}
