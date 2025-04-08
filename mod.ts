import {AllexEvent, IQueryable, IQueryRows, IQueryResult, IQueryResultAlike} from './deps.ts';
import pg from 'npm:pg';
const {Client} = pg;
type ClientType = typeof Client;


export interface IPgConfiguration {
  host: string;
  user: string;
  password: string;
  database: string;
}

interface Result {
  rows: IQueryRows;
  rowCount: number;
  affectedRows:number;
}

function pgQueryResult(result: Result) : IQueryResult {
  const retVal = result.rows as IQueryResultAlike;
  retVal.affectedRows = result.rowCount;
  return retVal;
}

export class PgQueryable implements IQueryable{
  private config: IPgConfiguration;
  private client: ClientType;
  private isConnected: boolean;
  public disconnected: AllexEvent<void> = new AllexEvent<void>;
  constructor(config: IPgConfiguration) {
    this.config = config;
    this.doClient();
    this.isConnected = false;
  }
  get statementDelimiter(): string {
    return ';\r';
  }
  get connected(): boolean {
    return this.client?._connected;
  }
  async connect(): Promise<boolean> {
    if (this.connected) {
      return true;
    }
    try {
      await this.client.connect();
      return true;
    } catch(e) {
      console.error(e);
      this.doClient();
      return false;
    }
  }
  disconnect(): Promise<void> {
    this.client?.end();
    return Promise.resolve();
  }
  async query(queryString: string): Promise<Array<IQueryResult>> {
    if (!this.connected) {
      throw new Error('Maybe we should not raise an error here, but what else?');
    }
    const results: Result|Array<Result> = await this.client.query(queryString);
    if (!results) {
      return [];
    }
    if (!Array.isArray(results)) {
      return [pgQueryResult(results)];
    }
    return results.map(r => pgQueryResult(r));
  }

  private doClient () {
    this.client = new Client(this.config);
    this.client.on('end', () => {
      this.isConnected = false;
      this.disconnected.fire();
    });
  }
}


