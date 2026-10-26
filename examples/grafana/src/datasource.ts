import { DataSourceInstanceSettings, ScopedVars } from '@grafana/data';
import { DataSourceWithBackend, getTemplateSrv } from '@grafana/runtime';
import { MensuraOptions, MensuraQuery } from './types';

export class DataSource extends DataSourceWithBackend<MensuraQuery, MensuraOptions> {
  constructor(instanceSettings: DataSourceInstanceSettings<MensuraOptions>) {
    super(instanceSettings);
  }

  applyTemplateVariables(query: MensuraQuery, scopedVars: ScopedVars): MensuraQuery {
    const srv = getTemplateSrv();
    const text = query.text ? srv.replace(query.text, scopedVars) : query.text;
    return { ...query, text };
  }

  async parse(text: string): Promise<{ ast?: unknown; error?: string; warnings?: unknown[] }> {
    return this.postResource('parse', { text });
  }
}
