import os
import json

base_dir = "k8s/observability/grafana"
api_dash_path = os.path.join(base_dir, "api-dashboard.json")
ingest_dash_path = os.path.join(base_dir, "ingestion-dashboard.json")
output_path = os.path.join(base_dir, "provisioning.yaml")

with open(api_dash_path, "r", encoding="utf-8") as f:
    api_dash_json = f.read()

with open(ingest_dash_path, "r", encoding="utf-8") as f:
    ingest_dash_json = f.read()

# Indent JSON files for YAML multiline block format
def indent_multiline(text, indent=4):
    spaces = " " * indent
    return "".join(spaces + line for line in text.splitlines(keepends=True))

yaml_content = f"""apiVersion: v1
kind: ConfigMap
metadata:
  name: grafana-datasources
  namespace: grafana
data:
  datasources.yaml: |
    apiVersion: 1
    datasources:
      - name: Prometheus
        type: prometheus
        access: proxy
        url: http://prometheus-svc.prometheus:9090
        isDefault: true
      - name: PostgreSQL
        type: postgres
        url: postgres-svc.postgres:5432
        database: mlops
        user: mlops
        secureJsonData:
          password: mlops2026
        jsonData:
          sslmode: disable
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: grafana-dashboards-provider
  namespace: grafana
data:
  provider.yaml: |
    apiVersion: 1
    providers:
      - name: 'default'
        orgId: 1
        folder: ''
        type: file
        disableDeletion: false
        editable: true
        options:
          path: /var/lib/grafana/dashboards
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: grafana-dashboards-json
  namespace: grafana
data:
  api-dashboard.json: |
{indent_multiline(api_dash_json, 4)}
  ingestion-dashboard.json: |
{indent_multiline(ingest_dash_json, 4)}
"""

with open(output_path, "w", encoding="utf-8") as f:
    f.write(yaml_content)

print(f"Generated provisioning manifest at: {output_path}")
