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

package controller

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/go-logr/logr"
	hbasev1 "github.com/timoha/hbase-k8s-operator/api/v1"
	"github.com/tsuna/gohbase"
)

// HBaseReconciler reconciles a HBase object
type HBaseReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	Log     logr.Logger
	GhAdmin gohbase.AdminClient
}

//+kubebuilder:rbac:groups=hbase.elenskiy.co,resources=hbases,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=hbase.elenskiy.co,resources=hbases/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=hbase.elenskiy.co,resources=hbases/finalizers,verbs=update
//+kubebuilder:rbac:groups="",resources=configmaps,verbs=*
//+kubebuilder:rbac:groups="",resources=services,verbs=*
//+kubebuilder:rbac:groups="apps",resources=statefulsets,verbs=*
//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.17.2/pkg/reconcile
func (r *HBaseReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// TODO: use that instead of adding the logger to the HBaseReconciler struct
	// _ = log.FromContext(ctx)

	log := r.Log.WithValues("hbase_resource", req.NamespacedName)

	log.Info("Start reconciliation")

	// Fetch the App instance.
	app := &hbasev1.HBase{}
	err := r.Get(ctx, req.NamespacedName, app)
	if err != nil {
		if errors.IsNotFound(err) {
			r.Log.Error(err, "HBase CRD is not found")
			return ctrl.Result{}, nil
		}
		r.Log.Error(err, "Failed getting HBase CRD")
		return ctrl.Result{}, err
	}

	log.Info("got HBase CRD")

	serviceOk, err := r.ensureService(app)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !serviceOk {
		log.Info("HBase service reconfigured, reconciling again")
		return ctrl.Result{Requeue: true}, nil
	}
	log.Info("HBase headless service is in sync")

	// deploy configmap if it doesn't exist
	configMapName := getConfigMapName(app)
	cmOk, err := r.ensureConfigMap(app, configMapName)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !cmOk {
		log.Info("HBase ConfigMap reconfigured, reconciling again")
		return ctrl.Result{Requeue: true}, nil
	}
	log.Info("HBase ConfigMap is in sync")

	// update hbasemaster statefulset
	masterName := types.NamespacedName{Name: "hbasemaster", Namespace: app.Namespace}
	masterSts, masterUpdated, err := r.ensureStatefulSet(app, masterName, configMapName, app.Spec.MasterSpec)
	if err != nil {
		r.Log.Error(err, "Failed reconciling HBase Master StatefulSet")
		return ctrl.Result{}, err
	}
	if masterUpdated {
		log.Info("HBase Master StatefulSet updated, reconciling again")
		return ctrl.Result{Requeue: true}, nil
	}
	log.Info("HBase Master StatefulSet is in sync")

	// update regionserver statefulset
	rsName := types.NamespacedName{Name: "regionserver", Namespace: app.Namespace}
	rsSts, rsUpdated, err := r.ensureStatefulSet(app, rsName, configMapName, app.Spec.RegionServerSpec)
	if err != nil {
		r.Log.Error(err, "Failed reconciling HBase RegionServer StatefulSet")
		return ctrl.Result{}, err
	}
	if rsUpdated {
		log.Info("HBase RegionServer StatefulSet updated")
		return ctrl.Result{Requeue: true}, nil
	}
	log.Info("RegionServer StatefulSet is in sync")

	// make sure there are no regions in transition.
	// we want this to happen after we've deployed all manifests in order to
	// be able to fix incorrect config and not fight with operator
	rit, err := r.regionsInTransition()
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get regions in transition: %v", err)
	}
	if rit != 0 {
		log.Info("There are regions in transition, wait and restart reconciling", "regions", rit)
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	log.Info("There are no regions in transition")

	r.Log.Info("Reconciling Master pods")
	mastersOk, err := r.ensureStatefulSetPods(ctx, masterSts, r.pickMasterToDelete)
	if err != nil {
		r.Log.Error(err, "Failed reconciling HBase Master pods")
		return ctrl.Result{}, err
	}
	if !mastersOk {
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	r.Log.Info("Reconciling RegionServer Pods")
	rsOk, err := r.ensureStatefulSetPods(ctx, rsSts, r.pickRegionServerToDelete)
	if err != nil {
		r.Log.Error(err, "Failed reconciling HBase RegionServer pods")
		return ctrl.Result{}, err
	}
	if !rsOk {
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	r.Log.Info("Deleting unused config maps")
	if err := r.deleteUnusedConfigMaps(ctx, app, configMapName); err != nil {
		return ctrl.Result{}, err
	}

	r.Log.Info("Everything is up to date!")
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *HBaseReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&hbasev1.HBase{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&appsv1.StatefulSet{}).
		Complete(r)
}
