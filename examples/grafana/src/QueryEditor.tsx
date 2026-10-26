import React, { ChangeEvent, useState } from 'react';
import { QueryEditorProps } from '@grafana/data';
import { InlineField, TextArea } from '@grafana/ui';
import { DataSource } from './datasource';
import { MensuraOptions, MensuraQuery } from './types';

type Props = QueryEditorProps<DataSource, MensuraQuery, MensuraOptions>;

export function QueryEditor({ query, onChange, onRunQuery, datasource }: Props) {
  const [error, setError] = useState('');

  const onText = (event: ChangeEvent<HTMLTextAreaElement>) => {
    onChange({ ...query, text: event.target.value });
  };

  const commit = async () => {
    const text = query.text ?? '';
    if (!text.trim()) {
      onRunQuery();
      return;
    }
    try {
      const parsed = await datasource.parse(text);
      if (parsed.error) {
        setError(parsed.error);
        return;
      }
      setError('');
      onChange({ ...query, text, ast: parsed.ast });
      onRunQuery();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  };

  return (
    <>
      <InlineField label="MQL" grow>
        <TextArea
          value={query.text ?? ''}
          onChange={onText}
          onBlur={commit}
          rows={6}
          placeholder={'FROM app SELECT requests_total RATE'}
        />
      </InlineField>
      {error ? <div>{error}</div> : null}
    </>
  );
}
