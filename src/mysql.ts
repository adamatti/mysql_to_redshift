import mysql, {Connection} from 'promise-mysql';
import { Logger } from "tslog";
import {config} from "./config";

export class MysqlColumn {
  name: string;
  dataType: string;
  type: string;
  nullable: boolean;
  pk: boolean;

  getPgType(): string {
    return config.mysql.conversionTypes[this.dataType.toLowerCase()] ?? this.type;
  }

  getDefinition():string {
    return `"${this.name}" ${this.getPgType()} ${!this.nullable ? 'NOT NULL' : ''} ${this.pk ? 'PRIMARY KEY' : ''}`.trim();
  }

  static fromInformationSchema(row: any): MysqlColumn {
    const column = new MysqlColumn();
    column.name = row.COLUMN_NAME;
    column.nullable = row.IS_NULLABLE;
    column.pk = row.COLUMN_KEY === 'PRI';
    column.dataType = row.DATA_TYPE;
    column.type = row.COLUMN_TYPE;
    return column;
  }
}

export class MysqlTable {
  db: MysqlDB;
  schema: string;
  name: string;
  rows: number;
  cachedColumns: MysqlColumn[] = null;

  toString(): string {
    return `${this.schema}.${this.name} [rows: ${this.rows}]`;
  }

  async getColumns(): Promise<MysqlColumn[]> {
    if (this.cachedColumns) {
      return this.cachedColumns;
    }

    const sql = `SELECT column_name, is_nullable, data_type, character_maximum_length, numeric_precision, column_type, column_key FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '${this.schema}' AND TABLE_NAME = '${this.name}' order by ordinal_position`

    const rows = await this.db.query(sql);
    this.cachedColumns = rows.map(MysqlColumn.fromInformationSchema);
    return this.cachedColumns;
  }

  async getRows(limit: number, offset: number): Promise<any[]> {
    const sql = `SELECT * FROM \`${this.schema}\`.${this.name} LIMIT ${limit} OFFSET ${offset}`;
    return this.db.query(sql)
  }

  static fromInformationSchema(row: any): MysqlTable {
    const table = new MysqlTable();
    table.schema = row.TABLE_SCHEMA;
    table.name = row.TABLE_NAME;
    table.rows = row.TABLE_ROWS;
    return table;
  }

  static build(args: Partial<MysqlTable>): MysqlTable {
    const table = new MysqlTable();    
    return Object.assign(table,args);
  }
}

export class MysqlDB {
  private logger = new Logger();
  private connection: Connection | undefined;

  constructor(
    private readonly url: string
  ) {
    if (!url) {
      throw new Error("Invalid mysql url provided")
    }
  }

  async query(sql: string) {
    if (!this.connection) {
      throw new Error("Not connectect to Mysql");
    }
    return this.connection.query(sql);
  }


  async connect(): Promise<MysqlDB> {
    this.connection = await mysql.createConnection(this.url);    
    this.logger.info("Connected to mysql")
    return this;
  }

  async disconnect () : Promise<MysqlDB> {
    await this.connection?.destroy();
    this.logger.info("Disconnected to mysql")
    return this;
  }

  async getTables(schema: string): Promise<MysqlTable[]> {
    if (!this.connection) {
      throw new Error("Not connectect to Mysql");
    }
    const sql = `SELECT table_schema, table_name, table_rows FROM information_schema.tables WHERE table_schema = '${schema}' and table_type <> 'VIEW' order by table_name`;
    const rows = await this.connection.query(sql)
    return rows.map((it: any) => {
      const table = MysqlTable.fromInformationSchema(it);
      table.db = this;
      return table;
    })
  }
}
