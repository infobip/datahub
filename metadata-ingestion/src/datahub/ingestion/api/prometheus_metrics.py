from prometheus_client import Counter

ingestionEntitiesCounter = Counter(name='ingested_total', documentation='Ingested entities counts',
                                   labelnames=['work_unit_class', 'entity_type'])

ingestionIssuesCounter = Counter(name='ingestion_issues_total', documentation='Ingested issues counts',
                                 labelnames=['issue_type', 'reason'])
