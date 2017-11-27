package com.puhao.data.handling;

import org.springframework.data.repository.CrudRepository;

/**
 * Created by pu on 2017/11/26.
 */
public interface ItemRepository extends CrudRepository<ItemEntity, ItemEntity.ItemId> {
}
