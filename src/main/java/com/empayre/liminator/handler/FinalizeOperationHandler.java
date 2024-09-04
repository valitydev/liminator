package com.empayre.liminator.handler;

import org.apache.thrift.TException;

public interface FinalizeOperationHandler<T> {

    void handle(T source) throws TException;
}
