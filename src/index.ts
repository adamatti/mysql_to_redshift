import {MysqlDB} from './mysql';
import {PostgresDB} from './postgres';
import { Logger } from "tslog";
import {config} from './config';

const main = async () => {
  const logger = new Logger();
  logger.info("Started");

  const [mysqlDb, potsgresDb] = await Promise.all([
    new MysqlDB(process.env.MYSQL_URL as string).connect(),
    new PostgresDB(process.env.POSTGRES_URL as string).connect(),
  ]);

  const schemas = Object.keys(config.mysql.importSchemas);
  await Promise.all(schemas.map(schema => {
    const targetSchema = config.mysql.importSchemas[schema];
    return potsgresDb.importSchema(mysqlDb, schema, targetSchema);
  }))

  await Promise.all([
    mysqlDb.disconnect(),
    potsgresDb.disconnect(),
  ]);
}

main();