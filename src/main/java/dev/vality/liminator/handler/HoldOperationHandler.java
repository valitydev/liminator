package dev.vality.liminator.handler;

import dev.vality.liminator.LimitRequest;
import org.apache.thrift.TException;

public interface HoldOperationHandler {

    void handle(LimitRequest request) throws TException;
}
