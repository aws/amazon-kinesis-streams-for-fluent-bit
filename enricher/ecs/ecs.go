package ecs

import (
	"os"
	"regexp"
	"strconv"
	"time"

	"github.com/canva/amazon-kinesis-streams-for-fluent-bit/enricher"
	"github.com/canva/amazon-kinesis-streams-for-fluent-bit/enricher/mappings"
	"github.com/sirupsen/logrus"
)

type Enricher struct {
	canvaAWSAccount string
	canvaAppName    string
	logGroup        string
	ecsTaskFamily   string
	ecsTaskRevision int
}

var _ enricher.IEnricher = (*Enricher)(nil)

func NewEnricher() *Enricher {
	ecsTaskDefinition := os.Getenv("ECS_TASK_DEFINITION")
	re := regexp.MustCompile(`^(?P<ecs_task_family>[^ ]*):(?P<ecs_task_revision>[\d]+)$`)
	ecsTaskDefinitionParts := re.FindStringSubmatch(ecsTaskDefinition)
	var (
		ecsTaskFamily   string
		ecsTaskRevision int
	)
	ecsTaskFamilyIndex := re.SubexpIndex("ecs_task_family")
	ecsTaskRevisionIndex := re.SubexpIndex("ecs_task_revision")

	if len(ecsTaskDefinitionParts) >= ecsTaskFamilyIndex {
		ecsTaskFamily = ecsTaskDefinitionParts[ecsTaskFamilyIndex]
	}
	if len(ecsTaskDefinitionParts) >= ecsTaskRevisionIndex {
		var err error
		ecsTaskRevision, err = strconv.Atoi(ecsTaskDefinitionParts[re.SubexpIndex("ecs_task_revision")])
		if err != nil {
			logrus.Warnf("[kinesis] ecs_task_revision not found for ECS_TASK_DEFINITION=%s", ecsTaskDefinition)
		}
	}

	return &Enricher{
		canvaAWSAccount: os.Getenv("CANVA_AWS_ACCOUNT"),
		canvaAppName:    os.Getenv("CANVA_APP_NAME"),
		logGroup:        os.Getenv("LOG_GROUP"),
		ecsTaskFamily:   ecsTaskFamily,
		ecsTaskRevision: ecsTaskRevision,
	}
}

// EnrichRecord modifies existing record.
func (enr *Enricher) EnrichRecord(r map[interface{}]interface{}, t time.Time) map[interface{}]interface{} {
	resource := map[interface{}]interface{}{
		mappings.RESOURCE_ACCOUNT_ID: enr.canvaAWSAccount,
		"service.name":               enr.canvaAppName,
		"cloud.platform":             "aws_ecs",
		"aws.ecs.launchtype":         "EC2",
		"aws.ecs.task.family":        enr.ecsTaskFamily,
		"aws.ecs.task.revision":      enr.ecsTaskRevision,
		"aws.log.group.names":        enr.logGroup,
	}
	body := make(map[interface{}]interface{})

	var (
		ok        bool
		strVal    string
		timestamp interface{}
	)
	for k, v := range r {
		strVal, ok = k.(string)
		if ok {
			switch strVal {
			case "ecs_task_definition":
				// Skip
			case "timestamp":
				timestamp = v
			case "ec2_instance_id":
				resource["host.id"] = v
			case "ecs_cluster":
				resource["aws.ecs.cluster.name"] = v
			case "ecs_task_arn":
				resource["aws.ecs.task.arn"] = v
			case "container_id":
				resource["container.id"] = v
			case "container_name":
				resource["container.name"] = v
			default:
				body[k] = v
			}
		}
	}
	return map[interface{}]interface{}{
		mappings.RESOURCE_FIELD_NAME: resource,
		"body":                       body,
		"timestamp":                  timestamp,
		mappings.OBSERVED_TIMESTAMP:  t.UnixMilli(),
	}
}
