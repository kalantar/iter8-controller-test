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
	iter8v1alpha1 "github.com/iter8-tools/iter8-controller/pkg/apis/iter8/v1alpha1"

	"istio.io/client-go/pkg/apis/networking/v1alpha3"
	istioclient "istio.io/client-go/pkg/clientset/versioned"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type IstioRoutingRules struct {
	DestinationRule *v1alpha3.DestinationRule
	VirtualService  *v1alpha3.VirtualService
}

func (r *IstioRoutingRules) SetStableLabels() {
	r.DestinationRule.ObjectMeta.SetLabels(map[string]string{experimentRole: Stable})
	r.VirtualService.ObjectMeta.SetLabels(map[string]string{experimentRole: Stable})
	removeExperimentLabel(r.DestinationRule, r.VirtualService)
}

func (r *IstioRoutingRules) IsStable() bool {
	drRole, drok := r.DestinationRule.GetLabels()[experimentRole]
	vsRole, vsok := r.VirtualService.GetLabels()[experimentRole]

	return drok && vsok && drRole == Stable && vsRole == Stable
}

func (rules *IstioRoutingRules) IsInit() bool {
	_, drok := rules.DestinationRule.GetLabels()[experimentInit]
	_, vsok := rules.VirtualService.GetLabels()[experimentInit]

	return drok && vsok
}

func (r *IstioRoutingRules) StableToProgressing(targets *Targets, expName, serviceNamespace string, ic istioclient.Interface) error {
	r.DestinationRule = NewDestinationRuleBuilder(r.DestinationRule).
		WithStableToProgressing(targets.Baseline).
		WithExperimentRegisterd(expName).
		Build()

	if dr, err := ic.NetworkingV1alpha3().
		DestinationRules(r.DestinationRule.GetNamespace()).
		Update(r.DestinationRule); err != nil {
		return err
	} else {
		r.DestinationRule = dr
	}

	r.VirtualService = NewVirtualServiceBuilder(r.VirtualService).
		WithStableToProgressing(targets.Service.GetName(), serviceNamespace).
		WithExperimentRegisterd(expName).
		Build()
	if vs, err := ic.NetworkingV1alpha3().
		VirtualServices(r.VirtualService.GetNamespace()).
		Update(r.VirtualService); err != nil {
		return err
	} else {
		r.VirtualService = vs
	}

	return nil
}

func (r *IstioRoutingRules) DeleteAll(ic istioclient.Interface) (err error) {
	if err = ic.NetworkingV1alpha3().DestinationRules(r.DestinationRule.Namespace).
		Delete(r.DestinationRule.Name, &metav1.DeleteOptions{}); err != nil {
		return
	}

	if err = ic.NetworkingV1alpha3().VirtualServices(r.VirtualService.Namespace).
		Delete(r.VirtualService.Name, &metav1.DeleteOptions{}); err != nil {
		return
	}
	return
}

func (r *IstioRoutingRules) Cleanup(instance *iter8v1alpha1.Experiment, targets *Targets, ic istioclient.Interface) (err error) {
	if instance.Spec.CleanUp == iter8v1alpha1.CleanUpDelete && r.IsInit() {
		err = r.DeleteAll(ic)
	} else {
		serviceName := instance.Spec.TargetService.Name
		if experimentSucceeded(instance) {
			// experiment is successful
			switch instance.Spec.TrafficControl.GetOnSuccess() {
			case "baseline":
				r.ToStable(targets.Baseline, Baseline, serviceName, serviceName)
			case "candidate":
				r.ToStable(targets.Candidate, Candidate, serviceName, serviceName)
			case "both":
				r.SetStableLabels()
			}

		} else {
			r.ToStable(targets.Baseline, Baseline, serviceName, serviceName)
		}

		err = r.UpdateRemoveRules(ic)
	}
	return
}

func (r *IstioRoutingRules) ToStable(stableDep *appsv1.Deployment, stableName, serviceName, serviceNamespace string) {
	r.DestinationRule = NewDestinationRuleBuilder(r.DestinationRule).
		WithProgressingToStable(stableDep).
		RemoveExperimentLabel().
		Build()
	r.VirtualService = NewVirtualServiceBuilder(r.VirtualService).
		WithProgressingToStable(serviceName, serviceNamespace, stableName).
		RemoveExperimentLabel().
		Build()
}

func (r *IstioRoutingRules) UpdateRemoveRules(ic istioclient.Interface) error {
	if dr, err := ic.NetworkingV1alpha3().
		DestinationRules(r.DestinationRule.Namespace).
		Update(r.DestinationRule); err != nil {
		return err
	} else {
		r.DestinationRule = dr
	}

	if vs, err := ic.NetworkingV1alpha3().
		VirtualServices(r.VirtualService.Namespace).
		Update(r.VirtualService); err != nil {
		return err
	} else {
		r.VirtualService = vs
	}

	return nil
}

func (r *IstioRoutingRules) GetWeight(subset string) int32 {
	for _, route := range r.VirtualService.Spec.Http[0].Route {
		if route.Destination.Subset == subset {
			return route.Weight
		}
	}
	return 0
}

func (r *IstioRoutingRules) UpdateRolloutPercent(serviceName, serviceNamespace string, w int32, ic istioclient.Interface) error {
	vs := NewVirtualServiceBuilder(r.VirtualService).
		WithRolloutPercent(serviceName, serviceNamespace, w).
		Build()

	if vs, err := ic.NetworkingV1alpha3().VirtualServices(vs.Namespace).Update(vs); err != nil {
		return err
	} else {
		r.VirtualService = vs
	}

	return nil
}
