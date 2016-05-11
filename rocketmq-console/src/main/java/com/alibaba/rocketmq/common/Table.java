package com.alibaba.rocketmq.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


/**
 * 
 * @author yankai913@gmail.com
 * @date 2014-2-17
 */
public class Table {

    private final static String DEFAULT_NULL_VALUE = "CELL_IS_NULL";

    private final int column;

    private final int row;

    private final List<Object[]> tbodyData;

    private String[] thead;

    private LinkedHashMap<String, String> extData = new LinkedHashMap<String, String>();


    public Table(int row, int column) {
        if (row <= 0 || column <= 0) {
            throw new IllegalArgumentException("invalid row or column");
        }
        this.row = row;
        this.column = column;
        this.tbodyData = new ArrayList<Object[]>(getRow());
    }


    public Table(String[] thead, List<Object[]> tbodyData) {
        checkTheadNotNull(thead);
        this.thead = thead;
        this.column = getThead().length;
        checkTbodyDataNotNull(tbodyData);
        checkTbodyDataValid(tbodyData, getColumn());
        this.row = tbodyData.size();
        this.tbodyData = tbodyData;
    }


    public Table(List<Object[]> tbodyData) {
        checkTbodyDataNotNull(tbodyData);
        this.row = tbodyData.size();
        this.column = tbodyData.get(0).length;
        checkTbodyDataValid(tbodyData, getColumn());
        this.tbodyData = tbodyData;
    }


    public Table(String[] thead, int row) {
        checkTheadNotNull(thead);
        this.thead = thead;
        this.column = getThead().length;
        this.row = row;
        this.tbodyData = new ArrayList<Object[]>(getRow());
    }


    private void checkTheadNotNull(String[] thead) {
        if (thead == null || thead.length <= 0) {
            throw new IllegalArgumentException("thead is blank");
        }
    }


    private void checkTbodyDataNotNull(List<Object[]> tbodyData) {
        if (tbodyData == null || tbodyData.size() <= 0) {
            throw new IllegalArgumentException("tbodyData is blank");
        }
        for (Object[] tr : tbodyData) {
            if (tr == null || tr.length <= 0) {
                throw new IllegalArgumentException("tbodyData`tr is blank");
            }
        }
    }


    private void checkTbodyDataValid(List<Object[]> tbodyData, int column) {
        for (Object[] tr : tbodyData) {
            if (tr.length != column) {
                throw new IllegalArgumentException("tbodyData`tr is invalid");
            }
        }
    }


    public Object[] createTR() {
        Object[] tr = new Object[getColumn()];
        Arrays.fill(tr, DEFAULT_NULL_VALUE);
        return tr;
    }


    public Table insertTR(Object[] tr) {
        if (tr.length != getColumn()) {
            throw new IllegalArgumentException("column not equal tr.length!");
        }
        getTbodyData().add(tr);
        return this;
    }


    public void removeTR(int index) {
        getTbodyData().remove(index);
    }


    public List<Object[]> getTbodyData() {
        return tbodyData;
    }


    public String[] getThead() {
        return thead;
    }


    public int getColumn() {
        return this.column;
    }


    public int getRow() {
        return row;
    }


    static String[] getTheadFromMap(Map<String, String> map) {
        int column = map.entrySet().size();
        String[] thead = new String[column];
        Iterator<String> ite = map.keySet().iterator();
        int i = 0;
        while (ite.hasNext()) {
            thead[i] = ite.next();
            i++;
        }
        return thead;
    }


    static Object[] getTbodyFromMap(Map<String, String> map) {
        int column = map.entrySet().size();
        Object[] tbody = new Object[column];
        Iterator<Map.Entry<String, String>> ite = map.entrySet().iterator();
        int i = 0;
        while (ite.hasNext()) {
            tbody[i] = ite.next().getValue();
            i++;
        }
        return tbody;
    }


    public static Table Map2VTable(Map<String, String> map) {
        int row = map.entrySet().size();
        Table table = new Table(row, 2);
        Iterator<Map.Entry<String, String>> ite = map.entrySet().iterator();
        while (ite.hasNext()) {
            Map.Entry<String, String> entry = ite.next();
            Object[] tr = table.createTR();
            tr[0] = entry.getKey();
            tr[1] = entry.getValue();
            table.insertTR(tr);
        }
        return table;
    }


    public static Table Map2HTable(Map<String, String> map) {
        List<Object[]> tbody = new ArrayList<Object[]>();
        Object[] tr = getTbodyFromMap(map);
        tbody.add(tr);
        String[] thead = getTheadFromMap(map);
        return new Table(thead, tbody);
    }


    public static Table Maps2HTable(List<Map<String, String>> list) {
        Table table = null;
        for (Map<String, String> map : list) {
            if (table == null) {
                String[] thead = getTheadFromMap(map);
                table = new Table(thead, list.size());
            }
            Object[] tr = getTbodyFromMap(map);
            table.insertTR(tr);
        }
        return table;
    }


    public LinkedHashMap<String, String> getExtData() {
        return extData;
    }


    public void addExtData(String key, String value) {
        extData.put(key, value);
    }
}
