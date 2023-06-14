## 开窗函数
### ROW_NUMBER()
为结果集中的每一行分配一个唯一的整数值，常用于对结果集进行编号。
```sql
SELECT ROW_NUMBER() OVER (ORDER BY column_name) AS row_num, column_name
FROM table_name;
```
### RANK()
根据指定的排序顺序，为结果集中的每一行分配一个排名，如果有相同值，则会跳过下一个排名
```sql
SELECT RANK() OVER (ORDER BY column_name) AS rank_num, column_name
FROM table_name;
```
### DENSE_RANK() 
根据指定的排序顺序，为结果集中的每一行分配一个密集排名，如果有相同值，则会跳过下一个排名，但不会留下空缺。
```sql
SELECT DENSE_RANK() OVER (ORDER BY column_name) AS dense_rank_num, column_name
FROM table_name;
```
### SUM()
```sql
SELECT column_name, SUM(column_name) OVER (PARTITION BY partition_column) AS total_sum
FROM table_name;
```
### AVG()
```sql
SELECT column_name, AVG(column_name) OVER (PARTITION BY partition_column) AS average_value
FROM table_name;
```
