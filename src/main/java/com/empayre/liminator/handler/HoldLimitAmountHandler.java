package com.empayre.liminator.handler;

import com.empayre.liminator.dao.OperationDao;
import com.empayre.liminator.domain.enums.OperationState;
import com.empayre.liminator.domain.tables.pojos.LimitData;
import com.empayre.liminator.domain.tables.pojos.Operation;
import com.empayre.liminator.model.LimitValue;
import com.empayre.liminator.service.DataConsistencyCheckingService;
import com.empayre.liminator.util.LimitDataUtils;
import dev.vality.liminator.LimitRequest;
import dev.vality.liminator.LimitResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class HoldLimitAmountHandler implements Handler<List<LimitRequest>, List<LimitResponse>> {

    private final OperationDao operationDao;
    private final DataConsistencyCheckingService dataConsistencyCheckingService;
    private final Converter<List<LimitValue>, List<LimitResponse>> currentLimitValuesToLimitResponseConverter;

    private static final String LOG_PREFIX = "HOLD";

    @Transactional
    @Override
    public List<LimitResponse> handle(List<LimitRequest> requestList) throws TException {
        if (CollectionUtils.isEmpty(requestList)) {
            return new ArrayList<>();
        }
        List<LimitData> limitData = dataConsistencyCheckingService.checkLimitsExistance(requestList, LOG_PREFIX);
        Map<String, Long> limitNamesMap = LimitDataUtils.createLimitNamesMap(limitData);
        List<Operation> operations = convertToOperation(requestList, limitNamesMap);
        operationDao.saveBatch(operations);
        List<String> limitNames = LimitDataUtils.getLimitNames(requestList);
        return currentLimitValuesToLimitResponseConverter.convert(operationDao.getCurrentLimitValue(limitNames));
    }

    private List<Operation> convertToOperation(List<LimitRequest> requestList, Map<String, Long> limitNamesMap) {
        return requestList.stream()
                .map(request -> convertToOperation(request, limitNamesMap.get(request.getLimitName())))
                .toList();
    }

    private Operation convertToOperation(LimitRequest request, Long limitId) {
        Operation operation = new Operation();
        operation.setLimitId(limitId);
        operation.setOperationId(request.getOperationId());
        operation.setAmount(request.getValue());
        operation.setCreatedAt(LocalDateTime.now());
        operation.setState(OperationState.HOLD);
        return operation;
    }
}
