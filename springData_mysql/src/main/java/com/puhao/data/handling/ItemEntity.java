package com.puhao.data.handling;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Table;
import java.io.Serializable;
import java.util.Date;
import java.util.Objects;

/**
 * Created by pu on 2017/11/26.
 */
@Entity
@IdClass(ItemEntity.ItemId.class)
@Table( name = "time_series_data", catalog = "test")
public class ItemEntity implements Serializable{
    public ItemEntity(){

    }
    public ItemEntity(String id, Date date, String code, double value){
        this.item_value = value;
        this.item_id = id;
        this.trading_date = date;
        this.stock_code = code;
    }

    /**
     * Since MySQL do not have UUID data type, will change UUID to String in java code to save to DB
     */
    @Id public String item_id;
    @Id public Date trading_date;
    @Id public String stock_code;

    public double item_value;


    public String toString(){
        return item_id + ", " + trading_date + ", " + stock_code + ", " + item_value;
    }

    public static class ItemId implements Serializable{
        public String getItem_id() {
            return item_id;
        }

        public Date getTrading_date() {
            return trading_date;
        }

        public String getStock_code() {
            return stock_code;
        }

        public void setItem_id(String item_id) {
            this.item_id = item_id;
        }

        public void setTrading_date(Date trading_date) {
            this.trading_date = trading_date;
        }

        public void setStock_code(String stock_code) {
            this.stock_code = stock_code;
        }

        private String item_id;
        private Date trading_date;
        private String stock_code;
        public ItemId(){

        }
        public ItemId(String item_id, Date trading_date, String stock_code){
            this.item_id = item_id;
            this.trading_date = trading_date;
            this.stock_code = stock_code;
        }
        @Override
        public boolean equals(Object o) {

            if (o == this) {
                return true;
            }
            if (!(o instanceof ItemId)) {
                return false;
            }
            ItemEntity otherId = (ItemEntity) o;
            return Objects.equals(this.item_id, otherId.item_id) &&
                    Objects.equals(this.trading_date, otherId.trading_date) &&
                    Objects.equals(this.stock_code, otherId.stock_code);
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.item_id, this.trading_date, this.stock_code);
        }
    }

}
