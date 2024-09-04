package com.empayre.liminator.dao;

import com.empayre.liminator.exception.DaoException;

public interface CommonDao<T> {

    Long save(T domainObject) throws DaoException;
}
