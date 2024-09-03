package com.empayre.liminator.service;

import com.empayre.liminator.config.PostgresqlSpringBootITest;
import com.empayre.liminator.dao.impl.LimitDataDaoImpl;
import dev.vality.liminator.CreateLimitRequest;
import dev.vality.liminator.DuplicateLimitName;
import dev.vality.liminator.LimitRequest;
import dev.vality.liminator.LimitResponse;
import org.apache.thrift.TException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

import static com.empayre.liminator.domain.tables.LimitData.LIMIT_DATA;
import static com.empayre.liminator.domain.tables.Operation.OPERATION;
import static org.junit.jupiter.api.Assertions.*;

@PostgresqlSpringBootITest
public class LiminatorServiceTest {

    @Autowired
    private LiminatorService liminatorService;

    @Autowired
    private LimitDataDaoImpl limitDataDao;

    @BeforeEach
    void beforeEach() {
        limitDataDao.getDslContext().truncate(LIMIT_DATA);
        limitDataDao.getDslContext().truncate(OPERATION);
    }

    @Test
    void createLimitTest() throws TException {
        String limitName = "TestLimit";
        CreateLimitRequest request = new CreateLimitRequest()
                .setLimitName(limitName);

        LimitResponse response = liminatorService.create(request);
        assertEquals(limitName, response.getLimitName());
        assertEquals(0, response.getHoldValue());
        assertEquals(0, response.getCommitValue());
    }

    @Test
    void holdValueTest() throws TException {
        limitDataDao.getDslContext().truncate(LIMIT_DATA);
        limitDataDao.getDslContext().truncate(OPERATION);

        String limitName = "TestLimit";
        CreateLimitRequest createRequest = new CreateLimitRequest()
                .setLimitName(limitName);
        liminatorService.create(createRequest);

        String operationId = "Op-112";
        LimitRequest holdRequest = new LimitRequest()
                .setLimitName(limitName)
                .setOperationId(operationId)
                .setValue(500L);
        List<LimitResponse> holdResponse = liminatorService.hold(List.of(holdRequest));
        assertEquals(1, holdResponse.size());
        LimitResponse response = holdResponse.get(0);
        assertEquals(500, response.getHoldValue());
        assertEquals(0, response.getCommitValue());
        assertEquals(limitName, response.getLimitName());
    }

    @Test
    void commitValueTest() throws TException {
        limitDataDao.getDslContext().truncate(LIMIT_DATA);
        limitDataDao.getDslContext().truncate(OPERATION);

        String limitName = "TestLimit";
        CreateLimitRequest createRequest = new CreateLimitRequest()
                .setLimitName(limitName);
        liminatorService.create(createRequest);

        String operationId = "Op-112";
        LimitRequest holdRequest = new LimitRequest()
                .setLimitName(limitName)
                .setOperationId(operationId)
                .setValue(500L);
        liminatorService.hold(List.of(holdRequest));
        assertTrue(liminatorService.commit(List.of(holdRequest)));

        List<LimitResponse> limitResponses = liminatorService.get(List.of(limitName));
        assertEquals(1, limitResponses.size());
        assertEquals(0, limitResponses.get(0).getHoldValue());
        assertEquals(500, limitResponses.get(0).getCommitValue());
        assertEquals(limitName, limitResponses.get(0).getLimitName());
    }

    @Test
    void rollbackValueTest() throws TException {
        limitDataDao.getDslContext().truncate(LIMIT_DATA);
        limitDataDao.getDslContext().truncate(OPERATION);

        String limitName = "TestLimit";
        CreateLimitRequest createRequest = new CreateLimitRequest()
                .setLimitName(limitName);
        liminatorService.create(createRequest);

        String operationId = "Op-112";
        LimitRequest holdRequest = new LimitRequest()
                .setLimitName(limitName)
                .setOperationId(operationId)
                .setValue(500L);
        liminatorService.hold(List.of(holdRequest));
        assertTrue(liminatorService.rollback(List.of(holdRequest)));

        List<LimitResponse> limitResponses = liminatorService.get(List.of(limitName));
        assertEquals(1, limitResponses.size());
        assertEquals(0, limitResponses.get(0).getHoldValue());
        assertEquals(0, limitResponses.get(0).getCommitValue());
        assertEquals(limitName, limitResponses.get(0).getLimitName());
    }
}
