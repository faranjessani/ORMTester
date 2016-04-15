###Results of a random run:

Results of 100 samples of 100 executions:

Case | Minimum | Lower Quantile | Median | Upper Quantile | Maximum
--- | --- | --- | --- | --- | ---
SQLDataReader SP By Row | 94 | 133 | 156 | 174 | 209
SQLDataReader Dynamic SQL By Row | 91 | 143 | 166 | 180 | 206
Dapper Dynamic SQL with POCO Mapping | 103 | 152 | 170 | 186 | 214
Dapper Stored Procedure | 96 | 153 | 172 | 185 | 217
Dapper Stored Procedure with POCO Mapping | 97 | 155 | 172 | 189 | 213
Dapper Dynamic SQL | 96 | 142 | 176 | 186 | 237
SQLDataReader Dynamic SQL Into DataSet | 96 | 157 | 181 | 193 | 231
SQLDataReader SP Into DataSet | 99 | 154 | 184 | 201 | 236
Entity Framework SP | 199 | 271 | 292 | 316 | 353
Entity Framework Dynamic SQL | 200 | 288 | 312 | 327 | 2038

-----------------------------------

http://blog.codinghorror.com/who-needs-stored-procedures-anyways/
