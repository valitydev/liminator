package com.empayre.liminator.service;

import com.empayre.liminator.handler.FinalizeOperationHandler;
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
    private final Handler<LimitRequest, List<LimitResponse>> holdLimitValueHandler;
    private final FinalizeOperationHandler<LimitRequest> commitLimitValueHandler;
    private final FinalizeOperationHandler<LimitRequest> rollbackLimitValueHandler;
    private final Handler<LimitRequest, List<LimitResponse>> getLimitsValuesHandler;
    private final Handler<List<String>, List<LimitResponse>> getLastLimitsValuesHandler;
    private final LimitOperationsLoggingService limitOperationsLoggingService;

    @Override
    public LimitResponse create(CreateLimitRequest createLimitRequest) throws DuplicateLimitName, TException {
        return createLimitHandler.handle(createLimitRequest);
    }

    @Override
    public List<LimitResponse> hold(LimitRequest limitRequest)
            throws LimitNotFound, DuplicateOperation, OperationAlreadyInFinalState, TException {
        List<LimitResponse> responses = holdLimitValueHandler.handle(limitRequest);
        limitOperationsLoggingService.writeHoldOperations(limitRequest);
        return responses;
    }

    @Override
    public void commit(LimitRequest limitRequest) throws LimitNotFound, OperationNotFound, TException {
        try {
            commitLimitValueHandler.handle(limitRequest);
            limitOperationsLoggingService.writeCommitOperations(limitRequest);
        } catch (Exception ex) {
            log.error("Commit execution exception. Request: {}", limitRequest, ex);
        }
    }

    @Override
    public void rollback(LimitRequest limitRequest) throws LimitNotFound, OperationNotFound, TException {
        try {
            rollbackLimitValueHandler.handle(limitRequest);
            limitOperationsLoggingService.writeRollbackOperations(limitRequest);
        } catch (Exception ex) {
            log.error("Commit execution exception. Request: {}", limitRequest, ex);
        }
    }

    @Override
    public List<LimitResponse> get(LimitRequest limitRequest) throws LimitNotFound, TException {
        return getLimitsValuesHandler.handle(limitRequest);
    }

    @Override
    public List<LimitResponse> getLastLimitsValues(List<String> limitNames) throws LimitNotFound, TException {
        return getLastLimitsValuesHandler.handle(limitNames);
    }
}
