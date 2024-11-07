package dev.vality.liminator.service;

import dev.vality.liminator.converter.OperationStateHistoryConverter;
import dev.vality.liminator.dao.OperationStateHistoryDao;
import dev.vality.liminator.domain.enums.OperationState;
import dev.vality.liminator.domain.tables.pojos.OperationStateHistory;
import dev.vality.liminator.model.CurrentLimitValue;
import dev.vality.liminator.LimitRequest;
import dev.vality.liminator.OperationNotFound;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static dev.vality.liminator.domain.enums.OperationState.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class LimitOperationsHistoryService {

    private final OperationStateHistoryDao operationStateHistoryDao;
    private final OperationStateHistoryConverter operationStateHistoryConverter;

    public int[] writeOperations(LimitRequest request,
                                 OperationState state,
                                 Map<String, Long> limitNamesMap) {
        var operationsHistory = operationStateHistoryConverter.convert(request, state, limitNamesMap);
        return operationStateHistoryDao.saveBatch(operationsHistory);
    }

    public List<CurrentLimitValue> getCurrentLimitValue(List<String> limitNames) {
        return operationStateHistoryDao.getCurrentValues(limitNames);
    }

    public List<CurrentLimitValue> getCurrentLimitValue(List<String> limitNames, String operationId) {
        return operationStateHistoryDao.getCurrentValues(limitNames, operationId);
    }

    public List<OperationStateHistory> get(String operationId,
                                           Collection<Long> limitIds,
                                           List<OperationState> states) {
        return operationStateHistoryDao.get(operationId, limitIds, states);
    }

    public void checkExistedHoldOperations(LimitRequest request,
                                            Collection<Long> limitIds,
                                            OperationState state) throws TException {
        String logPrefix = state.getLiteral();
        String operationId = request.getOperationId();
        List<OperationState> operationFinalStates = List.of(COMMIT, ROLLBACK);
        var existedFinalOperations =
                operationStateHistoryDao.get(operationId, limitIds, operationFinalStates);
        if (limitIds.size() == existedFinalOperations.size()) {
            log.error("[{}] Existed hold operations with ID {} not found (request: {})",
                    logPrefix, operationId, request);
            throw new OperationNotFound();
        }
        var existedHoldOperations = operationStateHistoryDao.get(operationId, limitIds, List.of(HOLD));
        var existedActiveHoldOperationCount = existedHoldOperations.size() - existedFinalOperations.size();
        if (limitIds.size() != existedActiveHoldOperationCount) {
            log.error("[{}] Count of existed hold operations for limits is not equal to expected (existed size: {}, " +
                            "expected size: {}, request: {})", logPrefix, existedActiveHoldOperationCount,
                    limitIds.size(), request);
            throw new OperationNotFound();
        }
    }
}