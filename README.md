# ZUMBR Data Model
Dimensional model for the product recommender project for Big Data Analytics course - UCI Spring 2017, using data from the ZUMBR travel customer database employed in the course.

Run deploy.sh to drop tables & replace them with their most recent versions (looping through the "sql" directory).
Source SQL for fact tables is under sql/facts, and SQL for dimension tables is under sql/dimensions.

Relies on environment variables set locally in conf/environment_vars.sh.
