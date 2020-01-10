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
	"time"

	iter8v1alpha1 "github.com/iter8-tools/iter8-controller/pkg/apis/iter8/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	corev1 "k8s.io/api/core/v1"
)

func (r *ExperimentReconciler) syncKubernetes(context context.Context, instance *iter8v1alpha1.Experiment) (reconcile.Result, error) {
	updateStatus, err := r.checkOrInitRules(context, instance)
	if updateStatus {
		if err := r.Status().Update(context, instance); err != nil && !validUpdateErr(err) {
			log.Info("Fail to update status: %v", err)
			// End experiment
			return reconcile.Result{}, nil
		}
	}
	if err != nil {
		return reconcile.Result{}, err
	}

	updateStatus, err = r.detectTargets(context, instance)
	if updateStatus {
		if err := r.Status().Update(context, instance); err != nil && !validUpdateErr(err) {
			log.Info("Fail to update status: %v", err)
			// End experiment
			return reconcile.Result{}, nil
		}
	}
	if err != nil {
		// retry in 5 secs
		log.Info("retry in 5 secs")
		return reconcile.Result{RequeueAfter: 5 * time.Second}, nil
	}

	if completed, err := r.checkExperimentComplete(context, instance); completed {
		if err := r.Status().Update(context, instance); err != nil && !validUpdateErr(err) {
			log.Info("Fail to update status: %v", err)
			// End experiment
			return reconcile.Result{}, nil
		}
		// Experiment completed
		return reconcile.Result{}, err
	}

	now := time.Now()
	traffic := instance.Spec.TrafficControl
	// TODO: check err in getting the time value
	interval, _ := traffic.GetIntervalDuration()
	if now.After(instance.Status.LastIncrementTime.Add(interval)) || withRecheckRequirement(instance) {
		err := r.progressExperiment(context, instance)
		if err := r.Status().Update(context, instance); err != nil && !validUpdateErr(err) {
			log.Info("Fail to update status: %v", err)
			// End experiment
			return reconcile.Result{}, nil
		}

		if err != nil {
			// TODO: may need a better handling method
			// retry in 5 sec
			log.Info("retry in 5 secs", "err", err)
			return reconcile.Result{RequeueAfter: 5 * time.Second}, err
		}

		if experimentCompleted(instance) {
			return reconcile.Result{Requeue: true}, nil
		}

		// Next iteration
		return reconcile.Result{RequeueAfter: interval}, nil
	}

	return reconcile.Result{}, nil
}

func (r *ExperimentReconciler) finalizeIstio(context context.Context, instance *iter8v1alpha1.Experiment) (reconcile.Result, error) {
	completed := instance.Status.GetCondition(iter8v1alpha1.ExperimentConditionExperimentCompleted)
	if completed != nil && completed.Status != corev1.ConditionTrue {
		targetsFound := instance.Status.GetCondition(iter8v1alpha1.ExperimentConditionTargetsProvided)
		// clean up can be done only when all targets are presented
		if targetsFound != nil && targetsFound.Status == corev1.ConditionTrue {
			// Execute in failure condition
			instance.Spec.Assessment = iter8v1alpha1.AssessmentOverrideFailure
			if err := r.targets.Cleanup(context, instance, r.Client); err != nil {
				return reconcile.Result{}, err
			}
			if err := r.rules.Cleanup(instance, r.targets, r.istioClient); err != nil {
				return reconcile.Result{}, err
			}
		}
	}

	return reconcile.Result{}, removeFinalizer(context, r, instance, Finalizer)
}
