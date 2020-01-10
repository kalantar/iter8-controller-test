/*

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package experiment

import (
	"context"
	"fmt"
	"os"

	iter8v1alpha1 "github.com/iter8-tools/iter8-controller/pkg/apis/iter8/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

var recordLevel = os.Getenv("RECORD_LEVEL")

// MarkTargetsError records the condition that the target components are missing
func (r *ExperimentReconciler) MarkTargetsError(context context.Context, instance *iter8v1alpha1.Experiment,
	messageFormat string, messageA ...interface{}) {
	reason := "TargetsNotFound"
	instance.Status.MarkTargetsError(reason, messageFormat, messageA...)
	Logger(context).Info(reason + ", " + fmt.Sprintf(messageFormat, messageA...))
	r.eventRecorder.Eventf(instance, corev1.EventTypeWarning, reason, messageFormat, messageA...)
}

func (r *ExperimentReconciler) MarkTargetsFound(context context.Context, instance *iter8v1alpha1.Experiment) bool {
	reason := "TargetsFound"
	//	Logger(context).Info(reason)
	value := instance.Status.MarkTargetsFound()
	if value {
		r.recordNormalEvent(true, instance, reason, "")
	}
	return value
}

func (r *ExperimentReconciler) MarkAnalyticsServiceError(context context.Context, instance *iter8v1alpha1.Experiment,
	messageFormat string, messageA ...interface{}) {
	reason := "AnalyticsServiceError"
	instance.Status.MarkAnalyticsServiceError(reason, messageFormat, messageA...)
	Logger(context).Info(reason + ", " + fmt.Sprintf(messageFormat, messageA...))
	r.eventRecorder.Eventf(instance, corev1.EventTypeWarning, reason, messageFormat, messageA...)
}

func (r *ExperimentReconciler) MarkAnalyticsServiceRunning(context context.Context, instance *iter8v1alpha1.Experiment) {
	reason := "AnalyticsServiceRunning"
	Logger(context).Info(reason)
	if instance.Status.MarkAnalyticsServiceRunning() {
		r.recordNormalEvent(true, instance, reason, "")
	}
}

func (r *ExperimentReconciler) MarkExperimentProgress(context context.Context, instance *iter8v1alpha1.Experiment,
	broadcast bool, messageFormat string, messageA ...interface{}) {
	reason := "ProgressUpdate"
	instance.Status.MarkExperimentNotCompleted(reason, messageFormat, messageA...)
	Logger(context).Info(reason + ", " + fmt.Sprintf(messageFormat, messageA...))
	r.recordNormalEvent(broadcast, instance, reason, messageFormat, messageA...)
}

func (r *ExperimentReconciler) MarkExperimentSucceeded(context context.Context, instance *iter8v1alpha1.Experiment,
	messageFormat string, messageA ...interface{}) {
	reason := "ExperimentSucceeded"
	instance.Status.MarkExperimentSucceeded(reason, messageFormat, messageA...)
	markExperimentCompleted(instance)
	Logger(context).Info(reason + ", " + fmt.Sprintf(messageFormat, messageA...))
	r.recordNormalEvent(true, instance, reason, messageFormat, messageA...)
}

func (r *ExperimentReconciler) MarkExperimentFailed(context context.Context, instance *iter8v1alpha1.Experiment,
	messageFormat string, messageA ...interface{}) {
	reason := "ExperimentFailed"
	instance.Status.MarkExperimentFailed(reason, messageFormat, messageA...)
	markExperimentCompleted(instance)
	Logger(context).Info(reason + ", " + fmt.Sprintf(messageFormat, messageA...))
	r.eventRecorder.Eventf(instance, corev1.EventTypeWarning, reason, messageFormat, messageA...)
}

func (r *ExperimentReconciler) MarkSyncMetricsError(context context.Context, instance *iter8v1alpha1.Experiment,
	messageFormat string, messageA ...interface{}) {
	reason := "SyncMetricsError"
	instance.Status.MarkMetricsSyncedError(reason, messageFormat, messageA...)
	Logger(context).Info(reason + ", " + fmt.Sprintf(messageFormat, messageA...))
	r.eventRecorder.Eventf(instance, corev1.EventTypeWarning, reason, messageFormat, messageA...)
}

func (r *ExperimentReconciler) MarkSyncMetrics(context context.Context, instance *iter8v1alpha1.Experiment) {
	reason := "SyncMetricsSucceeded"
	Logger(context).Info(reason)
	if instance.Status.MarkMetricsSynced() {
		r.recordNormalEvent(true, instance, reason, "")
	}
}

func (r *ExperimentReconciler) MarkRoutingRulesError(context context.Context, instance *iter8v1alpha1.Experiment,
	messageFormat string, messageA ...interface{}) {
	reason := "RoutingRulesError"
	Logger(context).Info(reason + ", " + fmt.Sprintf(messageFormat, messageA...))
	instance.Status.MarkRoutingRulesError(reason, messageFormat, messageA...)
	r.eventRecorder.Eventf(instance, corev1.EventTypeWarning, reason, messageFormat, messageA...)
}

func (r *ExperimentReconciler) MarkRoutingRulesReady(context context.Context, instance *iter8v1alpha1.Experiment,
	messageFormat string, messageA ...interface{}) {
	reason := "RoutingRulesReady"
	Logger(context).Info(reason + ", " + fmt.Sprintf(messageFormat, messageA...))
	if instance.Status.MarkRoutingRulesReady() {
		r.recordNormalEvent(true, instance, reason, messageFormat, messageA...)
	}
}

func (r *ExperimentReconciler) recordNormalEvent(broadcast bool, instance *iter8v1alpha1.Experiment, reason string,
	messageFormat string, messageA ...interface{}) {
	if broadcast || recordLevel == "verbose" {
		r.eventRecorder.Eventf(instance, corev1.EventTypeNormal, reason, messageFormat, messageA...)
	}
}
