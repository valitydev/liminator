package com.empayre.liminator.handler;

import com.empayre.liminator.dao.OperationDao;
import com.empayre.liminator.model.LimitValue;
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
public class GetLimitAmountHandler implements Handler<List<String>, List<LimitResponse>> {

    private final OperationDao operationDao;
    private final Converter<List<LimitValue>, List<LimitResponse>> currentLimitValuesToLimitResponseConverter;

    @Override
    @Transactional
    public List<LimitResponse> handle(List<String> limitIdNames) throws TException {
        return currentLimitValuesToLimitResponseConverter.convert(operationDao.getCurrentLimitValue(limitIdNames));
    }
}
