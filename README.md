Pet project to load data from mysql to redshift. 
It is using select/insert, need to be improved to a [better/faster way](https://www.integrate.io/blog/moving-data-from-mysql-to-redshift/).

# Known issues

- it only checks if table exists, not if columns matches
- redshift doesn't enforce pk, so need to check if data exists prior to insert (or use a staging table)

# Todo / Next steps

- lint
- tests
- improve log config (or change logger)
- move import logic from `postgresDB` class