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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	iter8v1alpha1 "github.com/iter8-tools/iter8-controller/pkg/apis/iter8/v1alpha1"
)

type Targets struct {
	Service   *corev1.Service
	Baseline  *appsv1.Deployment
	Candidate *appsv1.Deployment
}

func InitTargets() *Targets {
	return &Targets{
		Service:   &corev1.Service{},
		Baseline:  &appsv1.Deployment{},
		Candidate: &appsv1.Deployment{},
	}
}

func (t *Targets) Cleanup(context context.Context, instance *iter8v1alpha1.Experiment, client client.Client) error {
	if instance.Spec.CleanUp == iter8v1alpha1.CleanUpDelete {
		if experimentSucceeded(instance) {
			// experiment is successful
			switch instance.Spec.TrafficControl.GetOnSuccess() {
			case "candidate":
				// delete baseline deployment
				if err := deleteObjects(context, client, t.Baseline); err != nil {
					return err
				}
			case "both":
				//no-op
			case "baseline":
				// delete candidate deployment
				if err := deleteObjects(context, client, t.Candidate); err != nil {
					return err
				}
			}
		} else {
			if err := deleteObjects(context, client, t.Candidate); err != nil {
				return err
			}
		}
	}

	return nil
}
