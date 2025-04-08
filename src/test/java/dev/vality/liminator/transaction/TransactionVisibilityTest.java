package dev.vality.liminator.transaction;

import dev.vality.liminator.dao.OperationStateHistoryDao;
import dev.vality.liminator.service.LiminatorService;
import dev.vality.liminator.LimitChange;
import dev.vality.liminator.LimitRequest;
import dev.vality.liminator.LimitResponse;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;

class TransactionVisibilityTest extends AbstractIntegrationTestWithEmbeddedPostgres {

    @Autowired
    private LiminatorService liminatorService;

    @MockitoSpyBean
    private OperationStateHistoryDao operationStateHistoryDao;

    @Test
    void rollbackTransactionTest() throws TException, InterruptedException {
        Mockito.doThrow(new RuntimeException()).when(operationStateHistoryDao).saveBatch(any());

        String limitName = "TestLimitCommit123";
        String operationId = "operationId";
        long value = 123L;
        String limitId = "limitId";
        LimitRequest request = new LimitRequest()
                .setOperationId(operationId)
                .setLimitChanges(List.of(
                        new LimitChange()
                                .setLimitName(limitName)
                                .setLimitId(limitId)
                                .setValue(value)
                        )
                );

        assertThrows(RuntimeException.class, () -> liminatorService.hold(request));

        Thread.sleep(1000L);

        List<LimitResponse> limitResponses = liminatorService.getLastLimitsValues(List.of(limitName));
        assertEquals(0, limitResponses.size());

    }
}
