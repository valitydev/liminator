package com.empayre.liminator.handler.impl;

import com.empayre.liminator.dao.LimitContextDao;
import com.empayre.liminator.dao.LimitDataDao;
import com.empayre.liminator.domain.tables.pojos.LimitContext;
import com.empayre.liminator.domain.tables.pojos.LimitData;
import com.empayre.liminator.handler.Handler;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.vality.liminator.CreateLimitRequest;
import dev.vality.liminator.LimitResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class CreateLimitHandler implements Handler<CreateLimitRequest, LimitResponse> {

    private final ObjectMapper mapper;
    private final Converter<CreateLimitRequest, LimitData> createLimitRequestToLimitDataConverter;
    private final LimitDataDao limitDataDao;
    private final LimitContextDao limitContextDao;

    private static final String LOG_PREFIX = "CREATE";
    private static final String EMPTY_JSON = "{}";

    @Transactional
    @Override
    public LimitResponse handle(CreateLimitRequest request) throws TException {
        String limitName = request.getLimitName();
        LimitData existedLimitData = limitDataDao.get(limitName);
        if (existedLimitData != null) {
            log.info("[{}] Limit {} already exists", LOG_PREFIX, limitName);
            return new LimitResponse(limitName, 0, 0);
        }

        LimitData limitData = createLimitRequestToLimitDataConverter.convert(request);
        Long limitId = limitDataDao.save(limitData);
        if (request.context != null) {
            limitContextDao.save(convertToLimitContext(limitId, request.context));
        }
        return new LimitResponse(limitName, 0, 0);
    }

    private LimitContext convertToLimitContext(Long limitId, Map<String, String> contextMap) {
        LimitContext context = new LimitContext();
        context.setLimitId(limitId);
        context.setContext(getContextString(contextMap));
        context.setWtime(LocalDateTime.now());
        return context;
    }

    private String getContextString(Map<String, String> contextMap) {
        try {
            return mapper.writeValueAsString(contextMap);
        } catch (JsonProcessingException e) {
            log.error("[{}] ContextJSON processing exception", LOG_PREFIX, e);
            return EMPTY_JSON;
        }
    }
}
