package com.basic.benchmark.model;

import java.io.Serializable;

/**
 * locate com.basic.benchmark.model
 * Created by 79875 on 2017/10/20.
 */
public class LineItem implements Serializable{
    private String ORDERKEY;
    private String PARTKEY;
    private String SUPPKEY;
    private String LINENUMBER;
    private String QUANTITY;
    private String EXTENDEDPRICEl;
    private String DISCOUNT;
    private String TAX;
    private String RETURNFLAG;
    private String LINESTATUS;

    public LineItem() {
    }

    public LineItem(String ORDERKEY, String PARTKEY, String SUPPKEY, String LINENUMBER, String QUANTITY, String EXTENDEDPRICEl, String DISCOUNT, String TAX, String RETURNFLAG, String LINESTATUS) {
        this.ORDERKEY = ORDERKEY;
        this.PARTKEY = PARTKEY;
        this.SUPPKEY = SUPPKEY;
        this.LINENUMBER = LINENUMBER;
        this.QUANTITY = QUANTITY;
        this.EXTENDEDPRICEl = EXTENDEDPRICEl;
        this.DISCOUNT = DISCOUNT;
        this.TAX = TAX;
        this.RETURNFLAG = RETURNFLAG;
        this.LINESTATUS = LINESTATUS;
    }

    public String getORDERKEY() {
        return ORDERKEY;
    }

    public void setORDERKEY(String ORDERKEY) {
        this.ORDERKEY = ORDERKEY;
    }

    public String getPARTKEY() {
        return PARTKEY;
    }

    public void setPARTKEY(String PARTKEY) {
        this.PARTKEY = PARTKEY;
    }

    public String getSUPPKEY() {
        return SUPPKEY;
    }

    public void setSUPPKEY(String SUPPKEY) {
        this.SUPPKEY = SUPPKEY;
    }

    public String getLINENUMBER() {
        return LINENUMBER;
    }

    public void setLINENUMBER(String LINENUMBER) {
        this.LINENUMBER = LINENUMBER;
    }

    public String getQUANTITY() {
        return QUANTITY;
    }

    public void setQUANTITY(String QUANTITY) {
        this.QUANTITY = QUANTITY;
    }

    public String getEXTENDEDPRICEl() {
        return EXTENDEDPRICEl;
    }

    public void setEXTENDEDPRICEl(String EXTENDEDPRICEl) {
        this.EXTENDEDPRICEl = EXTENDEDPRICEl;
    }

    public String getDISCOUNT() {
        return DISCOUNT;
    }

    public void setDISCOUNT(String DISCOUNT) {
        this.DISCOUNT = DISCOUNT;
    }

    public String getTAX() {
        return TAX;
    }

    public void setTAX(String TAX) {
        this.TAX = TAX;
    }

    public String getRETURNFLAG() {
        return RETURNFLAG;
    }

    public void setRETURNFLAG(String RETURNFLAG) {
        this.RETURNFLAG = RETURNFLAG;
    }

    public String getLINESTATUS() {
        return LINESTATUS;
    }

    public void setLINESTATUS(String LINESTATUS) {
        this.LINESTATUS = LINESTATUS;
    }
}
