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
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v2"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	iter8v1alpha1 "github.com/iter8-tools/iter8-controller/pkg/apis/iter8/v1alpha1"
)

const (
	MetricsConfigMap = "iter8-metrics"
	Iter8Namespace   = "iter8"

	Baseline    = "baseline"
	Candidate   = "candidate"
	Stable      = "stable"
	Progressing = "progressing"

	experimentInit  = "iter8-tools/init"
	experimentRole  = "iter8-tools/role"
	experimentLabel = "iter8-tools/experiment"
	experimentHost  = "iter8-tools/host"
)

func addFinalizerIfAbsent(context context.Context, c client.Client, instance *iter8v1alpha1.Experiment, fName string) (err error) {
	for _, finalizer := range instance.ObjectMeta.GetFinalizers() {
		if finalizer == fName {
			return
		}
	}

	instance.SetFinalizers(append(instance.GetFinalizers(), Finalizer))
	if err = c.Update(context, instance); err != nil {
		Logger(context).Info("setting finalizer failed. (retrying)", "error", err)
	}

	return
}

func removeFinalizer(context context.Context, c client.Client, instance *iter8v1alpha1.Experiment, fName string) (err error) {
	finalizers := make([]string, 0)
	for _, f := range instance.GetFinalizers() {
		if f != fName {
			finalizers = append(finalizers, f)
		}
	}
	instance.SetFinalizers(finalizers)
	if err = c.Update(context, instance); err != nil {
		Logger(context).Info("setting finalizer failed. (retrying)", "error", err)
	}

	Logger(context).Info("FinalizerRemoved")
	return
}

func getServiceNamespace(instance *iter8v1alpha1.Experiment) string {
	serviceNamespace := instance.Spec.TargetService.Namespace
	if serviceNamespace == "" {
		serviceNamespace = instance.Namespace
	}
	return serviceNamespace
}

func updateGrafanaURL(instance *iter8v1alpha1.Experiment, namespace string) {
	endTs := instance.Status.EndTimestamp
	if endTs == "" {
		endTs = "now"
	}
	instance.Status.GrafanaURL = instance.Spec.Analysis.GetGrafanaEndpoint() +
		"/d/eXPEaNnZz/iter8-application-metrics?" +
		"var-namespace=" + namespace +
		"&var-service=" + instance.Spec.TargetService.Name +
		"&var-baseline=" + instance.Spec.TargetService.Baseline +
		"&var-candidate=" + instance.Spec.TargetService.Candidate +
		"&from=" + instance.Status.StartTimestamp +
		"&to=" + endTs
}

func getStrategy(instance *iter8v1alpha1.Experiment) string {
	strategy := instance.Spec.TrafficControl.GetStrategy()
	if strategy != iter8v1alpha1.StrategyIncrementWithoutCheck &&
		(instance.Spec.Analysis.SuccessCriteria == nil || len(instance.Spec.Analysis.SuccessCriteria) == 0) {
		strategy = iter8v1alpha1.StrategyIncrementWithoutCheck
	}
	return strategy
}

func experimentSucceeded(instance *iter8v1alpha1.Experiment) bool {
	if getStrategy(instance) == iter8v1alpha1.StrategyIncrementWithoutCheck {
		return instance.Spec.Assessment == iter8v1alpha1.AssessmentOverrideSuccess ||
			instance.Spec.Assessment == iter8v1alpha1.AssessmentNull
	}
	return instance.Spec.Assessment == iter8v1alpha1.AssessmentOverrideSuccess ||
		instance.Status.AssessmentSummary.AllSuccessCriteriaMet && instance.Spec.Assessment == iter8v1alpha1.AssessmentNull
}

func markExperimentCompleted(instance *iter8v1alpha1.Experiment) {
	// Clear analysis state
	instance.Status.AnalysisState.Raw = []byte("{}")

	// Update grafana url
	ts := metav1.Now().UTC().UnixNano() / int64(time.Millisecond)
	instance.Status.EndTimestamp = strconv.FormatInt(ts, 10)
	updateGrafanaURL(instance, getServiceNamespace(instance))

	instance.Status.MarkExperimentCompleted()
}

func successMsg(instance *iter8v1alpha1.Experiment) string {
	if instance.Spec.Assessment == iter8v1alpha1.AssessmentOverrideSuccess {
		return "OverrideSuccess"
	} else if instance.Status.AssessmentSummary.AllSuccessCriteriaMet {
		return "AllSuccessCriteriaMet"
	} else {
		return "IterationsExhausted"
	}
}

func failureMsg(instance *iter8v1alpha1.Experiment) string {
	if instance.Spec.Assessment == iter8v1alpha1.AssessmentOverrideFailure {
		return "OverrideFailure"
	} else if !instance.Status.AssessmentSummary.AllSuccessCriteriaMet {
		return "NotAllSuccessCriteriaMet"
	} else {
		// Should not be reached
		return "UnexpectedCondition"
	}
}

// Metrics list of Metric
type Metrics []Metric

// Metric structure of cm/iter8_metric
type Metric struct {
	Name               string `yaml:"name"`
	IsCounter          bool   `yaml:"is_counter"`
	AbsentValue        string `yaml:"absent_value"`
	SampleSizeTemplate string `yaml:"sample_size_query_template"`
}

func readMetrics(context context.Context, c client.Client, instance *iter8v1alpha1.Experiment) error {
	log := Logger(context)
	cm := &corev1.ConfigMap{}
	err := c.Get(context, types.NamespacedName{Name: MetricsConfigMap, Namespace: Iter8Namespace}, cm)
	if err != nil {
		if err = c.Get(context, types.NamespacedName{Name: MetricsConfigMap, Namespace: instance.GetNamespace()}, cm); err != nil {
			log.Info("MetricsConfigMapNotFound")
			return err
		}
	}

	data := cm.Data
	var templates map[string]string
	metrics := Metrics{}

	err = yaml.Unmarshal([]byte(data["query_templates"]), &templates)
	if err != nil {
		log.Error(err, "FailToReadYaml", "query_templates", data["query_templates"])
		return err
	}

	err = yaml.Unmarshal([]byte(data["metrics"]), &metrics)
	if err != nil {
		log.Error(err, "FailToReadYaml", "metrics", data["metrics"])
		return err
	}

	instance.Metrics = make(map[string]iter8v1alpha1.ExperimentMetric)
	criteria := make(map[string]bool)

	for _, criterion := range instance.Spec.Analysis.SuccessCriteria {
		criteria[criterion.MetricName] = true
	}

	for _, metric := range metrics {
		if !criteria[metric.Name] {
			continue
		}

		m := iter8v1alpha1.ExperimentMetric{
			IsCounter:   metric.IsCounter,
			AbsentValue: metric.AbsentValue,
		}
		qTpl, ok := templates[metric.Name]
		if !ok {
			return fmt.Errorf("FailToReadQueryTemplateForMetric %s", metric.Name)
		}
		sTpl, ok := templates[metric.SampleSizeTemplate]
		if !ok {
			return fmt.Errorf("FailToReadSampleSizeTemplateForMetric %s", metric.Name)
		}
		m.QueryTemplate = qTpl
		m.SampleSizeTemplate = sTpl
		instance.Metrics[metric.Name] = m
	}

	err = c.Update(context, instance)
	return err
}

func (r *ExperimentReconciler) executeStatusUpdate(ctx context.Context, instance *iter8v1alpha1.Experiment) (err error) {
	trial := 3
	for trial > 0 {
		if err = r.Status().Update(ctx, instance); err == nil {
			break
		}
		time.Sleep(time.Second * 5)
		trial--
	}
	return
}

func removeExperimentLabel(objs ...runtime.Object) (err error) {
	for _, obj := range objs {
		accessor, err := meta.Accessor(obj)
		if err != nil {
			return err
		}
		labels := accessor.GetLabels()
		delete(labels, experimentLabel)
		if _, ok := labels[experimentInit]; ok {
			delete(labels, experimentInit)
		}
		accessor.SetLabels(labels)
	}

	return nil
}

func deleteObjects(context context.Context, client client.Client, objs ...runtime.Object) error {
	for _, obj := range objs {
		if err := client.Delete(context, obj); err != nil {
			return err
		}
	}
	return nil
}

func setLabels(obj runtime.Object, newLabels map[string]string) error {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return err
	}
	labels := accessor.GetLabels()
	for key, val := range newLabels {
		labels[key] = val
	}
	return nil
}

func validUpdateErr(err error) bool {
	benignMsg := "the object has been modified"
	return strings.Contains(err.Error(), benignMsg)
}

func withRecheckRequirement(instance *iter8v1alpha1.Experiment) bool {
	analyticsCondition := instance.Status.GetCondition(iter8v1alpha1.ExperimentConditionAnalyticsServiceNormal)

	if analyticsCondition != nil && analyticsCondition.Status == corev1.ConditionFalse {
		return true
	}

	rulesCondition := instance.Status.GetCondition(iter8v1alpha1.ExperimentConditionRoutingRulesReady)

	if rulesCondition != nil && rulesCondition.Status == corev1.ConditionFalse {
		return true
	}

	return false
}

func experimentCompleted(instance *iter8v1alpha1.Experiment) bool {
	return instance.Spec.TrafficControl.GetMaxIterations() < instance.Status.CurrentIteration
}
