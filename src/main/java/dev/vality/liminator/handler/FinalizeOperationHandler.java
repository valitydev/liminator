package dev.vality.liminator.handler;

import dev.vality.liminator.domain.enums.OperationState;
import dev.vality.liminator.LimitRequest;
import org.apache.thrift.TException;

public interface FinalizeOperationHandler {

    void handle(LimitRequest request, OperationState state) throws TException;
}
