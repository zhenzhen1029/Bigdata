#### SP 
### 创建存储过程：
\' CREATE OR REPLACE PROCEDURE get_employee_details (emp_id IN NUMBER, emp_name OUT VARCHAR2, emp_salary OUT NUMBER) AS
BEGIN
  SELECT name, salary INTO emp_name, emp_salary FROM employees WHERE id = emp_id;
END;
/
\' 
> 上述示例创建了一个名为get_employee_details的存储过程，接受一个输入参数emp_id，并输出两个参数emp_name和emp_salary。
> 存储过程通过查询employees表获取指定员工的姓名和薪水，并将结果赋值给输出参数。
> 在使用这种SELECT INTO语句之前，要确保变量已经事先声明，并且与查询结果的列类型相匹配

### 调用存储过程：
\'
DECLARE
  emp_name VARCHAR2(100);
  emp_salary NUMBER;
BEGIN
  get_employee_details(1001, emp_name, emp_salary);
  DBMS_OUTPUT.PUT_LINE('Employee Name: ' || emp_name);
  DBMS_OUTPUT.PUT_LINE('Employee Salary: ' || emp_salary);
END;
/
\'
>上述示例使用DECLARE块定义了两个变量emp_name和emp_salary，然后调用了存储过程get_employee_details，将结果赋值给变量，并使用DBMS_OUTPUT.PUT_LINE函数输出结果。
>'1001'作为输入参数传递给存储过程get_employee_details的一个值
