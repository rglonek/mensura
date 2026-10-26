import { DataSourcePlugin } from '@grafana/data';
import { DataSource } from './datasource';
import { QueryEditor } from './QueryEditor';
import { MensuraOptions, MensuraQuery } from './types';

export const plugin = new DataSourcePlugin<DataSource, MensuraQuery, MensuraOptions>(DataSource).setQueryEditor(
  QueryEditor
);
