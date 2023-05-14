# EKS enricher
This EKS enricher follows the Standardize Tagging [documentation](https://canvadev.atlassian.net/wiki/spaces/OB/pages/2869725127/Standardized+Telemetry+Tagging).

##  Static Tags
Static tags are tags that do not change during runtime. These values are parsed as environment variables in `mappings.go`.

These are the following tags that are being enriched: 

### Resource 
- `cloud.partition`
- `cloud.account.id`
- `cloud.account.name`
- `cloud.region`
- `cloud.account.function`
- `organization`
- `cloud.provider`
- `cloud.platform`

### Kubernetes
- `cluster.name`

## Dropping logs
EKS enricher is dropping logs if `log` field is empty.

## Missing Kubernetes Metadata
Refer to this [investigation](https://docs.google.com/document/d/1vRCUKMeo6ypnAq34iwQN7LtDsXxmlj0aYEfRofwV7A4/edit?usp=sharing).

EKS enricher is currently mitigating this issue by adding `container_name` with `_missing_metadata`.
