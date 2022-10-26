import { Pool, PoolClient } from 'pg';
import { Logger } from "tslog";
import {MysqlDB, MysqlTable} from './mysql';
import {config } from './config';

const maxRedshiftSize = 65535;

const okErrors: string[] = [
  '23505' // duplicate key
]

export class PostgresDB {
  private readonly logger = new Logger();
  private pool: Pool | undefined;
  private client: PoolClient;

  constructor (private readonly url: string){
    this.pool = new Pool ({
      max: 20,
      connectionString: this.url,
      idleTimeoutMillis: 30000
    });
  }

  async connect():Promise<PostgresDB>{
    try {
      this.client = await this.pool.connect();
      this.logger.info("Connected to postgres");
    } catch (error: any) {
      this.logger.error(`Unable to connect to postgres: ${error.message}`, error);
      throw error;
    }
    return this;
  }

  async disconnect():Promise<PostgresDB>{
    this.client.release();
    await this.pool?.end();
    return this;
  }

  private async createTable(table: MysqlTable, targetSchema: string): Promise<boolean> {    
    const columnDefinitions: string[] = (await table.getColumns()).map(it => it.getDefinition());
    const ddl = `CREATE TABLE ${targetSchema}.${table.name} (${columnDefinitions.join(", ")})`;
    try {
      await this.client.query(ddl);
    } catch (error: any) {
      this.logger.error(`Unable to create table ${table.schema}.${table.name}: ${error.message}`, error);
      this.logger.info('DDL: ', ddl);
      throw error;
    }
    return true;
  }

  private async checkDDL(table: MysqlTable, targetSchema: string): Promise<boolean> {
    const tableName = table.name;

    // FIXME move this query
    const sql = `SELECT table_schema, table_name FROM information_schema.tables where not table_schema in ('pg_catalog', 'information_schema') and table_name='${tableName}' and table_schema='${targetSchema}'`;
    const { rows } = await this.client.query(sql);
    if (!rows || rows.length ===0) {
      this.logger.warn(`Table ${targetSchema}.${tableName} not found on pg, creating`);
      return this.createTable(table, targetSchema);
    }

    // TODO check all fields here
    return true;
  }
 
  async importSchema(mysqlDb: MysqlDB, sourceSchema: string, targetSchema: string): Promise<PostgresDB>{
    const tables = (await mysqlDb.getTables(sourceSchema)).filter(table => {
      const { tablesToNotImport } = config.postgres;

      return !tablesToNotImport.includes(table.name) && !tablesToNotImport.includes(`${table.schema}.${table.name}`)
    });
    await Promise.all(tables.map(async table => {
      this.logger.info(`Table ${table.toString()}`);
      const shallImportData = await this.checkDDL(table, targetSchema);

      if (shallImportData) {
        return this.importData(table, targetSchema);
      }
    }))
    return this;
  }

  private async insert(mysqlTable: MysqlTable, row: any, targetSchema: string) {
    const keys = Object.keys(row).map(key => `"${key}"`);
    const indexes = Object.keys(row).map((value, index) => "$" + (index+1));
    const values = Object.values(row).map((value:any) => {
      const invalidValues = ['0000-00-00', "0000-00-00 00:00:00"];
      if (invalidValues.includes(value)) {
        return null;
      }
      if (value?.length > maxRedshiftSize) {
        return value.substring(0, maxRedshiftSize - 1)
      }
      return value;
    });
    const sql = `INSERT INTO ${targetSchema}.${mysqlTable.name}(${keys.join(",")}) VALUES (${indexes.join(",")})`;
    try {
      await this.client.query(sql, values);
    } catch (error: any) {
      if (okErrors.includes(error.code)) {
        return
      }
      this.logger.error(`Error running sql: ${error.message} - ${sql}, values[${values}]`, error);
      throw error;
    }
  }

  async importData(mysqlTable: MysqlTable, targetSchema: string): Promise<PostgresDB>{ 
    const {batchSize} = config.mysql;
    let offset = 0;
    while (true) {
      this.logger.info (`Reading ${mysqlTable.schema}.${mysqlTable.name} ${offset}/${mysqlTable.rows}`);
      const rows = await mysqlTable.getRows(batchSize, offset);      
      await Promise.all(rows.map(row => {
        return this.insert(mysqlTable, row, targetSchema);
      }))

      if (rows.length === 0 || rows.length < batchSize) {
        break;
      }

      offset+=batchSize;
    }
    this.logger.info(`Import completed for ${mysqlTable.schema}.${mysqlTable.name}`)
    return this;
  }
}