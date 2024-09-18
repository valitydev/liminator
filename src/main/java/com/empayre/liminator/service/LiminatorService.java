package com.empayre.liminator.service;

import com.empayre.liminator.domain.enums.OperationState;
import com.empayre.liminator.handler.FinalizeOperationHandler;
import com.empayre.liminator.handler.Handler;
import com.empayre.liminator.handler.HoldOperationHandler;
import dev.vality.liminator.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.jooq.exception.DataAccessException;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class LiminatorService implements LiminatorServiceSrv.Iface {

    private final Handler<CreateLimitRequest, LimitResponse> createLimitHandler;
    private final HoldOperationHandler holdOperationHandler;
    private final FinalizeOperationHandler finalizeOperationHandler;
    private final Handler<LimitRequest, List<LimitResponse>> getLimitsValuesHandler;
    private final Handler<List<String>, List<LimitResponse>> getLastLimitsValuesHandler;

    @Override
    public LimitResponse create(CreateLimitRequest createLimitRequest) throws DuplicateLimitName, TException {
        return createLimitHandler.handle(createLimitRequest);
    }

    @Override
    public List<LimitResponse> hold(LimitRequest request)
            throws LimitNotFound, DuplicateOperation, OperationAlreadyInFinalState, TException {
        if (request == null || CollectionUtils.isEmpty(request.getLimitChanges())) {
            log.warn("[HOLD] LimitRequest or LimitNames is empty. Request: {}", request);
            return new ArrayList<>();
        }
        holdOperationHandler.handle(request);
        return get(request);
    }

    @Override
    public void commit(LimitRequest request) throws LimitNotFound, OperationNotFound, TException {
        try {
            finalizeOperationHandler.handle(request, OperationState.COMMIT);
        } catch (Exception ex) {
            log.error("Commit execution exception. Request: {}", request, ex);
        }
    }

    @Override
    public void rollback(LimitRequest request) throws LimitNotFound, OperationNotFound, TException {
        try {
            finalizeOperationHandler.handle(request, OperationState.ROLLBACK);
        } catch (Exception ex) {
            log.error("Rollback execution exception. Request: {}", request, ex);
        }
    }

    @Override
    public List<LimitResponse> get(LimitRequest request)
            throws LimitNotFound, LimitsValuesReadingException, TException {
        try {
            return getLimitsValuesHandler.handle(request);
        } catch (DataAccessException ex) {
            log.error("[GET] Received DaoException for getting limits operation (request: {})", request, ex);
            throw new LimitsValuesReadingException();
        }
    }

    @Override
    public List<LimitResponse> getLastLimitsValues(List<String> limitNames)
            throws LimitNotFound, LimitsValuesReadingException, TException {
        try {
            return getLastLimitsValuesHandler.handle(limitNames);
        } catch (DataAccessException ex) {
            log.error("[GET] Received DaoException for getting last limits operation (limitNames: {})", limitNames, ex);
            throw new LimitsValuesReadingException();
        }
    }
}
