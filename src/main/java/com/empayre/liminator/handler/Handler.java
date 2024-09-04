package com.empayre.liminator.handler;

import org.apache.thrift.TException;

public interface Handler<T, R> {

    R handle(T source) throws TException;
}
