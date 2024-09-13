package com.empayre.liminator.handler.impl;

import com.empayre.liminator.dao.OperationDao;
import com.empayre.liminator.handler.Handler;
import com.empayre.liminator.model.LimitValue;
import dev.vality.liminator.LimitRequest;
import dev.vality.liminator.LimitResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class GetLimitsValuesHandler implements Handler<LimitRequest, List<LimitResponse>> {

    private final OperationDao operationDao;
    private final Converter<List<LimitValue>, List<LimitResponse>> currentLimitValuesToLimitResponseConverter;

    @Transactional
    @Override
    public List<LimitResponse> handle(LimitRequest request) throws TException {
        List<LimitValue> limitValues =
                operationDao.getCurrentLimitValue(request.getLimitNames(), request.getOperationId());
        return currentLimitValuesToLimitResponseConverter.convert(limitValues);
    }
}
