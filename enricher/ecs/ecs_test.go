package ecs

import (
	"reflect"
	"testing"
	"time"

	"github.com/canva/amazon-kinesis-streams-for-fluent-bit/enricher"
	"github.com/canva/amazon-kinesis-streams-for-fluent-bit/enricher/mappings"
)

func TestEnrichRecords(t *testing.T) {
	type args struct {
		r map[interface{}]interface{}
		t time.Time
	}
	tests := []struct {
		name string
		enr  enricher.IEnricher
		args args
		want map[interface{}]interface{}
	}{
		{
			name: "enrich",
			enr: &Enricher{
				canvaAWSAccount: "canva_aws_account_val",
				canvaAppName:    "canva_app_name_val",
				logGroup:        "log_group_val",
				ecsTaskFamily:   "ecs_task_family_val",
				ecsTaskRevision: 10001,
			},
			args: args{
				map[interface{}]interface{}{
					"ec2_instance_id":     "ec2_instance_id_val",
					"ecs_cluster":         "ecs_cluster_val",
					"ecs_task_arn":        "ecs_task_arn_val",
					"container_id":        "container_id_val",
					"container_name":      "container_name_val",
					"other_key_1":         "other_value_1",
					"other_key_2":         "other_value_2",
					"other_key_3":         "other_value_3",
					"timestamp":           "1234567890",
					"ecs_task_definition": "ecs_task_definition_val",
				},
				time.Date(2009, time.November, 10, 23, 7, 5, 432000000, time.UTC),
			},
			want: map[interface{}]interface{}{
				"resource": map[interface{}]interface{}{
					mappings.RESOURCE_CLOUD_ACCOUNT_ID: "canva_aws_account_val",
					"service.name":                     "canva_app_name_val",
					"cloud.platform":                   "aws_ecs",
					"aws.ecs.launchtype":               "EC2",
					"aws.ecs.task.family":              "ecs_task_family_val",
					"aws.ecs.task.revision":            10001,
					"aws.log.group.names":              "log_group_val",
					"host.id":                          "ec2_instance_id_val",
					"aws.ecs.cluster.name":             "ecs_cluster_val",
					"aws.ecs.task.arn":                 "ecs_task_arn_val",
					"container.id":                     "container_id_val",
					"container.name":                   "container_name_val",
				},
				"body": map[interface{}]interface{}{
					"other_key_1": "other_value_1",
					"other_key_2": "other_value_2",
					"other_key_3": "other_value_3",
				},
				"timestamp":         "1234567890",
				"observedTimestamp": int64(1257894425432),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.enr.EnrichRecord(tt.args.r, tt.args.t)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("enricher.enrichRecord() = %+v, want %+v", got, tt.want)
			}
		})
	}
}
