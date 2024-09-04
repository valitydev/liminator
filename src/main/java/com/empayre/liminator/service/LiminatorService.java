package com.empayre.liminator.service;

import com.empayre.liminator.handler.Handler;
import dev.vality.liminator.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class LiminatorService implements LiminatorServiceSrv.Iface {

    private final Handler<CreateLimitRequest, LimitResponse> createLimitHandler;
    private final Handler<List<LimitRequest>, List<LimitResponse>> holdLimitAmountHandler;
    private final Handler<List<LimitRequest>, Boolean> commitLimitAmountHandler;
    private final Handler<List<LimitRequest>, Boolean> rollbackLimitAmountHandler;
    private final Handler<List<String>, List<LimitResponse>> getLimitAmountHandler;

    @Override
    public LimitResponse create(CreateLimitRequest createLimitRequest) throws DuplicateLimitName, TException {
        return createLimitHandler.handle(createLimitRequest);
    }

    @Override
    public List<LimitResponse> hold(List<LimitRequest> list) throws LimitNotFound, TException {
        return holdLimitAmountHandler.handle(list);
    }

    @Override
    public boolean commit(List<LimitRequest> list) throws LimitNotFound, TException {
        try {
            commitLimitAmountHandler.handle(list);
            return true;
        } catch (Exception ex) {
            log.error("Commit execution exception. Request list: {}", list, ex);
            return false;
        }
    }

    @Override
    public boolean rollback(List<LimitRequest> list) throws LimitNotFound, TException {
        try {
            rollbackLimitAmountHandler.handle(list);
            return true;
        } catch (Exception ex) {
            log.error("Commit execution exception. Request list: {}", list, ex);
            return false;
        }
    }

    @Override
    public List<LimitResponse> get(List<String> limitNames) throws LimitNotFound, TException {
        return getLimitAmountHandler.handle(limitNames);
    }
}
