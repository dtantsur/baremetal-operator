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

package v1alpha1

import (
	"fmt"

	"k8s.io/apimachinery/pkg/util/errors"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
)

// bmcsubscriptionlog is for logging in this package.
var bmcsubscriptionlog = logf.Log.WithName("bmceventsubscription-resource")

func (s *BMCEventSubscription) SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(s).
		Complete()
}

//+kubebuilder:webhook:verbs=create;update,path=/validate-metal3-io-v1alpha1-bmceventsubscription,mutating=false,failurePolicy=fail,sideEffects=none,admissionReviewVersions=v1;v1beta,groups=metal3.io,resources=bmceventsubscriptions,versions=v1alpha1,name=bmceventsubscription.metal3.io

var _ webhook.Validator = &BMCEventSubscription{}

// ValidateCreate implements webhook.Validator so a webhook will be registered for the type
func (s *BMCEventSubscription) ValidateCreate() error {
	bmcsubscriptionlog.Info("validate create", "name", s.Name)
	return errors.NewAggregate(s.validateSubscription())
}

// ValidateUpdate implements webhook.Validator so a webhook will be registered for the type
func (s *BMCEventSubscription) ValidateUpdate(old runtime.Object) error {
	bmcsubscriptionlog.Info("validate update", "name", s.Name)
	return fmt.Errorf("subscriptions cannot be updated, please recreate it")
}

// ValidateDelete implements webhook.Validator so a webhook will be registered for the type
func (s *BMCEventSubscription) ValidateDelete() error {
	return nil
}
