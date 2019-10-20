package com.imooc.flink.java.course06;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

public class TableSQLAPI {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.getTableEnvironment(env);
        String filePath = "c:\\flink\\sales.csv";
        DataSet<Sales> csv = env.readCsvFile(filePath)
                .ignoreFirstLine()
                .pojoType(Sales.class,"transactionId","customerId","itemId","amountPaid");
        csv.print();
        Table sales = tableEnv.fromDataSet(csv);
        tableEnv.registerTable("sales",sales);
        Table resultTable = tableEnv.sqlQuery("select customerId ,sum(amountPaid) money from sales group by customerId");
        DataSet<Row> result =tableEnv.toDataSet(resultTable, Row.class);
        result.print();
        /** output:
         * 4,600.0
         * 3,510.0
         * 1,4600.0
         * 2,1005.0
         */
    }
    //POJO
    public static class Sales{
        public String transactionId ;
        public String customerId;
        public String itemId;
        public Double  amountPaid;

    }
}
