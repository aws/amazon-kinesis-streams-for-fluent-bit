package enricher

import (
	"os"
	"regexp"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
)

// EnrichRecord modifies existing record.
func EnrichRecord(r map[interface{}]interface{}, t time.Time) map[interface{}]interface{} {
	return defaultEnricher.enrichRecord(r, t)
}

type enricher struct {
	enable          bool
	canvaAWSAccount string
	canvaAppName    string
	logGroup        string
	ecsTaskFamily   string
	ecsTaskRevision int
}

var defaultEnricher *enricher

// Init will initialise defaultEnricher instance.
func Init(enable bool) {
	if !enable {
		defaultEnricher = new(enricher)
		return
	}

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

	defaultEnricher = &enricher{
		enable:          true,
		canvaAWSAccount: os.Getenv("CANVA_AWS_ACCOUNT"),
		canvaAppName:    os.Getenv("CANVA_APP_NAME"),
		logGroup:        os.Getenv("LOG_GROUP"),
		ecsTaskFamily:   ecsTaskFamily,
		ecsTaskRevision: ecsTaskRevision,
	}
}

// enrichRecord modifies existing record.
func (enr *enricher) enrichRecord(r map[interface{}]interface{}, t time.Time) map[interface{}]interface{} {
	if !enr.enable {
		return r
	}

	resource := map[string]interface{}{
		"cloud.account.id":      enr.canvaAWSAccount,
		"service.name":          enr.canvaAppName,
		"cloud.platform":        "aws_ecs",
		"aws.ecs.launchtype":    "EC2",
		"aws.ecs.task.family":   enr.ecsTaskFamily,
		"aws.ecs.task.revision": enr.ecsTaskRevision,
		"aws.log.group.names":   enr.logGroup,
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
		"resource":          resource,
		"body":              body,
		"timestamp":         timestamp,
		"observedTimestamp": t.UnixMilli(),
	}
}
