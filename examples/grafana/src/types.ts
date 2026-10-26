import { DataQuery, DataSourceJsonData } from '@grafana/data';

export interface MensuraQuery extends DataQuery {
  text?: string;
  ast?: unknown;
}

export interface MensuraOptions extends DataSourceJsonData {
  storeUrl?: string;
}
