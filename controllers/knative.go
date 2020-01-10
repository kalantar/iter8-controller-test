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
	"encoding/json"
	"time"

	servingv1alpha1 "github.com/knative/serving/pkg/apis/serving/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/iter8-tools/iter8-controller/pkg/analytics"
	"github.com/iter8-tools/iter8-controller/pkg/analytics/checkandincrement"
	"github.com/iter8-tools/iter8-controller/pkg/analytics/epsilongreedy"
	iter8v1alpha1 "github.com/iter8-tools/iter8-controller/pkg/apis/iter8/v1alpha1"
)

func (r *ExperimentReconciler) syncKnative(context context.Context, instance *iter8v1alpha1.Experiment) (reconcile.Result, error) {
	log := Logger(context)

	// Get Knative service
	serviceName := instance.Spec.TargetService.Name
	serviceNamespace := instance.Spec.TargetService.Namespace
	if serviceNamespace == "" {
		serviceNamespace = instance.Namespace
	}

	kservice := &servingv1alpha1.Service{}
	err := r.Get(context, types.NamespacedName{Name: serviceName, Namespace: serviceNamespace}, kservice)
	if err != nil {
		r.MarkTargetsError(context, instance, "Missing Service %s", serviceName)
		err = r.Status().Update(context, instance)
		if err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{RequeueAfter: 5 * time.Second}, nil
	}

	if kservice.Spec.Template == nil {
		r.MarkTargetsError(context, instance, "%s", "Missing Template")
		return reconcile.Result{}, r.Status().Update(context, instance)
	}

	// link service to this experiment. Only one experiment can control a service
	labels := kservice.GetLabels()
	if experiment, found := labels[experimentLabel]; found && experiment != instance.GetName() {
		r.MarkTargetsError(context, instance, "service is already controlled by experiment %s", experiment)
		return reconcile.Result{}, r.Status().Update(context, instance)
	}

	if labels == nil {
		labels = make(map[string]string)
	}

	if _, ok := labels[experimentLabel]; !ok {
		labels[experimentLabel] = instance.GetName()
		kservice.SetLabels(labels)
		if err = r.Update(context, kservice); err != nil {
			return reconcile.Result{}, err
		}
	}

	// Check the experiment targets existing traffic targets
	ksvctraffic := kservice.Spec.Traffic
	if ksvctraffic == nil {
		r.MarkTargetsError(context, instance, "%s", "MissingTraffic")
		return reconcile.Result{}, r.Status().Update(context, instance)
	}

	baseline := instance.Spec.TargetService.Baseline
	baselineTraffic := getTrafficByName(kservice, baseline)
	candidate := instance.Spec.TargetService.Candidate
	candidateTraffic := getTrafficByName(kservice, candidate)

	if baselineTraffic == nil {
		r.MarkTargetsError(context, instance, "Missing Baseline Revision: %s", baseline)
		instance.Status.TrafficSplit.Baseline = 0
		if candidateTraffic == nil {
			instance.Status.TrafficSplit.Candidate = 0
		} else {
			instance.Status.TrafficSplit.Candidate = int(*candidateTraffic.Percent)
		}

		return reconcile.Result{}, r.Status().Update(context, instance)
	}

	if candidateTraffic == nil {
		r.MarkTargetsError(context, instance, "Missing Candidate Revision: %s", candidate)
		instance.Status.TrafficSplit.Baseline = int(*baselineTraffic.Percent)
		instance.Status.TrafficSplit.Candidate = 0
		err = r.Status().Update(context, instance)
		return reconcile.Result{}, err
	}

	r.MarkTargetsFound(context, instance)

	traffic := instance.Spec.TrafficControl
	now := time.Now()
	interval, _ := traffic.GetIntervalDuration() // TODO: admissioncontrollervalidation

	// check experiment is finished
	if traffic.GetMaxIterations() <= instance.Status.CurrentIteration ||
		instance.Spec.Assessment != iter8v1alpha1.AssessmentNull {

		update := false
		if experimentSucceeded(instance) {
			// experiment is successful
			switch traffic.GetOnSuccess() {
			case "baseline":
				if *candidateTraffic.Percent != 0 {
					*candidateTraffic.Percent = 0
					update = true
				}
				if *baselineTraffic.Percent != 100 {
					*baselineTraffic.Percent = 100
					update = true
				}
				instance.Status.TrafficSplit.Baseline = 100
				instance.Status.TrafficSplit.Candidate = 0
			case "candidate":
				if *candidateTraffic.Percent != 100 {
					*candidateTraffic.Percent = 100
					update = true
				}
				if *baselineTraffic.Percent != 0 {
					*baselineTraffic.Percent = 0
					update = true
				}
				instance.Status.TrafficSplit.Baseline = 0
				instance.Status.TrafficSplit.Candidate = 100
			case "both":
			}
			r.MarkExperimentSucceeded(context, instance, "%s", successMsg(instance))
		} else {
			r.MarkExperimentFailed(context, instance, "%s", failureMsg(instance))

			// Switch traffic back to baseline
			if *candidateTraffic.Percent != 0 {
				*candidateTraffic.Percent = 0
				update = true
			}
			if *baselineTraffic.Percent != 100 {
				*baselineTraffic.Percent = 100
				update = true
			}
		}

		labels := kservice.GetLabels()
		_, has := labels[experimentLabel]
		if has || update {
			delete(labels, experimentLabel)
		}

		if has || update {
			err := r.Update(context, kservice)
			if err != nil {
				return reconcile.Result{}, err // retry
			}
		}

		instance.Status.TrafficSplit.Baseline = int(*baselineTraffic.Percent)
		instance.Status.TrafficSplit.Candidate = int(*candidateTraffic.Percent)
		return reconcile.Result{}, r.Status().Update(context, instance)
	}

	// Check if traffic should be updated.
	if now.After(instance.Status.LastIncrementTime.Add(interval)) {
		log.Info("process iteration.")

		newRolloutPercent := *candidateTraffic.Percent

		strategy := getStrategy(instance)
		if iter8v1alpha1.StrategyIncrementWithoutCheck == strategy {
			newRolloutPercent += int64(traffic.GetStepSize())
		} else {
			var analyticsService analytics.AnalyticsService
			switch getStrategy(instance) {
			case checkandincrement.Strategy:
				analyticsService = checkandincrement.GetService()
			case epsilongreedy.Strategy:
				analyticsService = epsilongreedy.GetService()
			}

			// Get underlying k8s services
			// TODO: should just get the service name. See issue #83
			baselineService, err := r.getServiceForRevision(context, kservice, baselineTraffic.RevisionName)
			if err != nil {
				// TODO: maybe we want another condition
				r.MarkTargetsError(context, instance, "Missing Core Service: %v", err)
				return reconcile.Result{}, r.Status().Update(context, instance)
			}

			candidateService, err := r.getServiceForRevision(context, kservice, candidateTraffic.RevisionName)
			if err != nil {
				// TODO: maybe we want another condition
				r.MarkTargetsError(context, instance, "Missing Core Service: %v", err)
				return reconcile.Result{}, r.Status().Update(context, instance)
			}

			// Get latest analysis
			payload, err := analyticsService.MakeRequest(instance, baselineService, candidateService)
			if err != nil {
				r.MarkAnalyticsServiceError(context, instance, "Can Not Compose Payload: %v", err)
				if err := r.Status().Update(context, instance); err != nil {
					return reconcile.Result{}, err
				}
				return reconcile.Result{RequeueAfter: 5 * time.Second}, r.Status().Update(context, instance)
			}
			response, err := analyticsService.Invoke(log, instance.Spec.Analysis.GetServiceEndpoint(), payload, analyticsService.GetPath())
			if err != nil {
				r.MarkAnalyticsServiceError(context, instance, "%s", err.Error())
				if err := r.Status().Update(context, instance); err != nil {
					return reconcile.Result{}, err
				}
				return reconcile.Result{RequeueAfter: 5 * time.Second}, err
			}

			if response.Assessment.Summary.AbortExperiment {
				log.Info("ExperimentAborted. Rollback to Baseline.")
				if *candidateTraffic.Percent != 0 || *baselineTraffic.Percent != 100 {
					*baselineTraffic.Percent = 100
					*candidateTraffic.Percent = 0
					err := r.Update(context, kservice)
					if err != nil {
						return reconcile.Result{}, err // retry
					}
				}

				instance.Status.TrafficSplit.Baseline = 100
				instance.Status.TrafficSplit.Candidate = 0

				r.MarkExperimentFailed(context, instance, "%s", "Aborted, Traffic: AllToBaseline.")
				err := r.Update(context, instance)
				if err != nil {
					return reconcile.Result{}, err // retry
				}
			}

			baselineTraffic := response.Baseline.TrafficPercentage
			candidateTraffic := response.Candidate.TrafficPercentage
			log.Info("NewTraffic", "baseline", baselineTraffic, "candidate", candidateTraffic)
			newRolloutPercent = int64(candidateTraffic)

			if response.LastState == nil {
				instance.Status.AnalysisState.Raw = []byte("{}")
			} else {
				lastState, err := json.Marshal(response.LastState)
				if err != nil {
					r.MarkAnalyticsServiceError(context, instance, "ErrorAnalyticsResponse: %v", err)
					return reconcile.Result{}, r.Status().Update(context, instance)
				}
				instance.Status.AnalysisState = runtime.RawExtension{Raw: lastState}
			}
			instance.Status.AssessmentSummary = response.Assessment.Summary
		}

		// Set traffic percentable on all routes
		needUpdate := false
		for i := range ksvctraffic {
			target := &ksvctraffic[i]
			if target.RevisionName == baseline {
				if *target.Percent != 100-int64(newRolloutPercent) {
					*target.Percent = 100 - int64(newRolloutPercent)
					needUpdate = true
				}
			} else if target.RevisionName == candidate {
				if *target.Percent != int64(newRolloutPercent) {
					*target.Percent = int64(newRolloutPercent)
					needUpdate = true
				}
			} else {
				if *target.Percent != 0 {
					*target.Percent = 0
					needUpdate = true
				}
			}
		}
		if needUpdate {
			log.Info("update traffic", "rolloutPercent", newRolloutPercent)
			r.MarkExperimentProgress(context, instance, true, "New Traffic, baseline: %d, candidate: %d",
				instance.Status.TrafficSplit.Baseline, instance.Status.TrafficSplit.Candidate)
			err = r.Update(context, kservice) // TODO: patch?
			if err != nil {
				// TODO: the analysis service will be called again upon retry. Maybe we do want that.
				return reconcile.Result{}, err
			}
		}

		instance.Status.CurrentIteration++
		instance.Status.LastIncrementTime = metav1.NewTime(now)
	}

	r.MarkExperimentProgress(context, instance, false, "Iteration %d Completed", instance.Status.CurrentIteration)
	instance.Status.TrafficSplit.Baseline = int(*baselineTraffic.Percent)
	instance.Status.TrafficSplit.Candidate = int(*candidateTraffic.Percent)
	return reconcile.Result{RequeueAfter: interval}, r.Status().Update(context, instance)
}

func getTrafficByName(service *servingv1alpha1.Service, name string) *servingv1alpha1.TrafficTarget {
	for i := range service.Spec.Traffic {
		traffic := &service.Spec.Traffic[i]
		if traffic.RevisionName == name {
			return traffic
		}
	}
	return nil
}

func (r *ExperimentReconciler) getServiceForRevision(context context.Context, ksvc *servingv1alpha1.Service, revisionName string) (*corev1.Service, error) {
	revision := &servingv1alpha1.Revision{}
	err := r.Get(context, types.NamespacedName{Name: revisionName, Namespace: ksvc.GetNamespace()}, revision)
	if err != nil {
		return nil, err
	}
	service := &corev1.Service{}
	err = r.Get(context, types.NamespacedName{Name: revision.Status.ServiceName, Namespace: ksvc.GetNamespace()}, service)
	if err != nil {
		return nil, err
	}
	return service, nil
}

func (r *ExperimentReconciler) finalizeKnative(context context.Context, instance *iter8v1alpha1.Experiment) (reconcile.Result, error) {
	completed := instance.Status.GetCondition(iter8v1alpha1.ExperimentConditionExperimentCompleted)
	if completed != nil && completed.Status != corev1.ConditionTrue {
		// Do a rollback

		// Get Knative service
		serviceName := instance.Spec.TargetService.Name
		serviceNamespace := getServiceNamespace(instance)

		kservice := &servingv1alpha1.Service{}
		err := r.Get(context, types.NamespacedName{Name: serviceName, Namespace: serviceNamespace}, kservice)
		if err != nil {
			return reconcile.Result{}, removeFinalizer(context, r, instance, Finalizer)
		}

		// Check the experiment targets existing traffic targets
		ksvctraffic := kservice.Spec.Traffic
		if ksvctraffic == nil {
			return reconcile.Result{}, removeFinalizer(context, r, instance, Finalizer)
		}

		baseline := instance.Spec.TargetService.Baseline
		baselineTraffic := getTrafficByName(kservice, baseline)
		if baselineTraffic == nil {
			return reconcile.Result{}, removeFinalizer(context, r, instance, Finalizer)
		}

		candidate := instance.Spec.TargetService.Candidate
		candidateTraffic := getTrafficByName(kservice, candidate)
		if candidateTraffic == nil {
			return reconcile.Result{}, removeFinalizer(context, r, instance, Finalizer)
		}

		if *baselineTraffic.Percent != 100 || *candidateTraffic.Percent != 0 {
			*baselineTraffic.Percent = 100
			*candidateTraffic.Percent = 0

			err = r.Update(context, kservice) // TODO: patch?
			if err != nil {
				return reconcile.Result{}, err
			}
		}
	}

	return reconcile.Result{}, removeFinalizer(context, r, instance, Finalizer)
}
