package com.empayre.liminator.handler;

import com.empayre.liminator.converter.OperationConverter;
import com.empayre.liminator.dao.OperationDao;
import com.empayre.liminator.domain.tables.pojos.LimitData;
import com.empayre.liminator.domain.tables.pojos.Operation;
import com.empayre.liminator.model.LimitValue;
import com.empayre.liminator.service.LimitsGettingService;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class HoldLimitAmountHandler implements Handler<List<LimitRequest>, List<LimitResponse>> {

    private final OperationDao operationDao;
    private final LimitsGettingService limitsGettingService;
    private final Converter<List<LimitValue>, List<LimitResponse>> currentLimitValuesToLimitResponseConverter;
    private final OperationConverter operationConverter;

    private static final String LOG_PREFIX = "HOLD";

    @Transactional
    @Override
    public List<LimitResponse> handle(List<LimitRequest> requestList) throws TException {
        if (CollectionUtils.isEmpty(requestList)) {
            return new ArrayList<>();
        }
        List<LimitData> limitData = limitsGettingService.get(requestList, LOG_PREFIX);
        Map<String, Long> limitNamesMap = LimitDataUtils.createLimitNamesMap(limitData);
        List<Operation> operations = convertToOperation(requestList, limitNamesMap);
        operationDao.saveBatch(operations);
        List<String> limitNames = LimitDataUtils.getLimitNames(requestList);
        return currentLimitValuesToLimitResponseConverter.convert(operationDao.getCurrentLimitValue(limitNames));
    }

    private List<Operation> convertToOperation(List<LimitRequest> requestList, Map<String, Long> limitNamesMap) {
        return requestList.stream()
                .map(request -> operationConverter.convert(request, limitNamesMap.get(request.getLimitName())))
                .toList();
    }
}
