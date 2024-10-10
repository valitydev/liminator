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
import java.util.Arrays;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class LiminatorService implements LiminatorServiceSrv.Iface {

    private final HoldOperationHandler holdOperationHandler;
    private final FinalizeOperationHandler finalizeOperationHandler;
    private final Handler<LimitRequest, List<LimitResponse>> getLimitsValuesHandler;
    private final Handler<List<String>, List<LimitResponse>> getLastLimitsValuesHandler;


    @Override
    public List<LimitResponse> hold(LimitRequest request)
            throws LimitNotFound, DuplicateOperation, OperationAlreadyInFinalState, TException {
        if (request == null || CollectionUtils.isEmpty(request.getLimitChanges())) {
            log.warn("[HOLD] LimitRequest or LimitNames is empty. Request: {}", request);
            return new ArrayList<>();
        }
        log.info("Start hold operation with request: {}", request);
        holdOperationHandler.handle(request);
        return get(request);
    }

    @Override
    public void commit(LimitRequest request) throws LimitNotFound, OperationNotFound, TException {
        try {
            log.info("Start commit operation with request: {}", request);
            finalizeOperationHandler.handle(request, OperationState.COMMIT);
            log.info("Finish commit operation with request: {}", request);
        } catch (Exception ex) {
            log.error("Commit execution exception. Request: {}", request, ex);
            throw ex;
        }
    }

    @Override
    public void rollback(LimitRequest request) throws LimitNotFound, OperationNotFound, TException {
        try {
            log.info("Start rollback operation with request: {}", request);
            finalizeOperationHandler.handle(request, OperationState.ROLLBACK);
            log.info("Finish rollback operation with request: {}", request);
        } catch (Exception ex) {
            log.error("Rollback execution exception. Request: {}", request, ex);
            throw ex;
        }
    }

    @Override
    public List<LimitResponse> get(LimitRequest request)
            throws LimitNotFound, LimitsValuesReadingException, TException {
        try {
            log.info("Get limits with request: {}", request);
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
            log.info("Get last limits for limits: {}", Arrays.toString(limitNames.toArray()));
            return getLastLimitsValuesHandler.handle(limitNames);
        } catch (DataAccessException ex) {
            log.error("[GET] Received DaoException for getting last limits operation (limitNames: {})", limitNames, ex);
            throw new LimitsValuesReadingException();
        }
    }
}
