package com.empayre.liminator.dao;

public interface CommonDao<T> {

    Long save(T domainObject);
}
