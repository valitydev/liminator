package com.empayre.liminator.transaction;

import com.empayre.liminator.dao.OperationStateHistoryDao;
import com.empayre.liminator.service.LiminatorService;
import dev.vality.liminator.CreateLimitRequest;
import dev.vality.liminator.LimitChange;
import dev.vality.liminator.LimitRequest;
import dev.vality.liminator.LimitResponse;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.SpyBean;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;

public class TransactionVisibilityTest extends AbstractIntegrationTestWithEmbeddedPostgres {

    @Autowired
    private LiminatorService liminatorService;

    @SpyBean
    private OperationStateHistoryDao operationStateHistoryDao;

    @Test
    public void rollbackTransactionTest() throws TException, InterruptedException {
        Mockito.doThrow(new RuntimeException()).when(operationStateHistoryDao).saveBatch(any());

        String limitName = "TestLimitCommit123";
        CreateLimitRequest createRequest = new CreateLimitRequest()
                .setLimitName(limitName);
        liminatorService.create(createRequest);

        List<LimitResponse> limitResponsesBefore = liminatorService.getLastLimitsValues(List.of(limitName));
        assertEquals(1, limitResponsesBefore.size());
        assertEquals(0, limitResponsesBefore.get(0).getHoldValue());
        assertEquals(0, limitResponsesBefore.get(0).getCommitValue());

        String operationId = "OpComit123";
        LimitRequest holdRequest = new LimitRequest()
                .setOperationId(operationId)
                .setLimitChanges(List.of(new LimitChange(limitName, 500L)));

        assertThrows(RuntimeException.class, () -> liminatorService.hold(holdRequest));

        Thread.sleep(1000L);

        List<LimitResponse> limitResponses = liminatorService.getLastLimitsValues(List.of(limitName));
        assertEquals(1, limitResponses.size());
        assertEquals(0, limitResponses.get(0).getHoldValue());
        assertEquals(0, limitResponses.get(0).getCommitValue());
        assertEquals(limitName, limitResponses.get(0).getLimitName());
    }
}
