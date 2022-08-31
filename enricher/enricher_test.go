package enricher

import (
	"reflect"
	"testing"
	"time"
)

func Test_enricher_enrichRecord(t *testing.T) {
	type args struct {
		r map[interface{}]interface{}
		t time.Time
	}
	tests := []struct {
		name string
		enr  *enricher
		args args
		want map[interface{}]interface{}
	}{
		{
			name: "enable",
			enr: &enricher{
				enable:          true,
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
				"resource": map[string]interface{}{
					"cloud.account.id":      "canva_aws_account_val",
					"service.name":          "canva_app_name_val",
					"cloud.platform":        "aws_ecs",
					"aws.ecs.launchtype":    "EC2",
					"aws.ecs.task.family":   "ecs_task_family_val",
					"aws.ecs.task.revision": 10001,
					"aws.log.group.names":   "log_group_val",
					"host.id":               "ec2_instance_id_val",
					"aws.ecs.cluster.name":  "ecs_cluster_val",
					"aws.ecs.task.arn":      "ecs_task_arn_val",
					"container.id":          "container_id_val",
					"container.name":        "container_name_val",
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
		{
			name: "disable",
			enr: &enricher{
				enable: false,
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
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.enr.enrichRecord(tt.args.r, tt.args.t)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("enricher.enrichRecord() = %+v, want %+v", got, tt.want)
			}
		})
	}
}
