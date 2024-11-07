package dev.vality.liminator.dao;

public interface CommonDao<T> {

    Long save(T domainObject);
}
