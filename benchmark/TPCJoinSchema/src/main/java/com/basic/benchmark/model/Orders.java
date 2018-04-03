package com.basic.benchmark.model;

import java.io.Serializable;

/**
 * locate com.basic.benchmark.model
 * Created by 79875 on 2017/10/20.
 */
public class Orders implements Serializable{
    private String ORDERKEY;
    private String CUSTKEY;
    private String ORDERSTATUS;
    private String TOTALPRICE;
    private String ORDERDATE;
    private String ORDERPRIORITY;
    private String CLERK;

    public Orders() {
    }

    public Orders(String ORDERKEY, String CUSTKEY, String ORDERSTATUS, String TOTALPRICE, String ORDERDATE, String ORDERPRIORITY, String CLERK) {
        this.ORDERKEY = ORDERKEY;
        this.CUSTKEY = CUSTKEY;
        this.ORDERSTATUS = ORDERSTATUS;
        this.TOTALPRICE = TOTALPRICE;
        this.ORDERDATE = ORDERDATE;
        this.ORDERPRIORITY = ORDERPRIORITY;
        this.CLERK = CLERK;
    }

    public String getORDERKEY() {
        return ORDERKEY;
    }

    public void setORDERKEY(String ORDERKEY) {
        this.ORDERKEY = ORDERKEY;
    }

    public String getCUSTKEY() {
        return CUSTKEY;
    }

    public void setCUSTKEY(String CUSTKEY) {
        this.CUSTKEY = CUSTKEY;
    }

    public String getORDERSTATUS() {
        return ORDERSTATUS;
    }

    public void setORDERSTATUS(String ORDERSTATUS) {
        this.ORDERSTATUS = ORDERSTATUS;
    }

    public String getTOTALPRICE() {
        return TOTALPRICE;
    }

    public void setTOTALPRICE(String TOTALPRICE) {
        this.TOTALPRICE = TOTALPRICE;
    }

    public String getORDERDATE() {
        return ORDERDATE;
    }

    public void setORDERDATE(String ORDERDATE) {
        this.ORDERDATE = ORDERDATE;
    }

    public String getORDERPRIORITY() {
        return ORDERPRIORITY;
    }

    public void setORDERPRIORITY(String ORDERPRIORITY) {
        this.ORDERPRIORITY = ORDERPRIORITY;
    }

    public String getCLERK() {
        return CLERK;
    }

    public void setCLERK(String CLERK) {
        this.CLERK = CLERK;
    }
}
