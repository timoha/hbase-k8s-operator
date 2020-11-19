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

package controllers

import (
	"context"
	"crypto/sha256"
	"fmt"
	"hash"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/go-logr/logr"
	hbasev1 "github.com/timoha/hbase-k8s-operator/api/v1"
	"github.com/tsuna/gohbase"
	"github.com/tsuna/gohbase/hrpc"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/pointer"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

/*
Copyright 2015 The Kubernetes Authors.
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

// DeepHashObject writes specified object to hash using the spew library
// which follows pointers and prints actual values of the nested objects
// ensuring the hash does not change when a pointer changes.
func DeepHashObject(hasher hash.Hash, objectToWrite interface{}) {
	hasher.Reset()
	printer := spew.ConfigState{
		Indent:         " ",
		SortKeys:       true,
		DisableMethods: true,
		SpewKeys:       true,
	}
	printer.Fprintf(hasher, "%#v", objectToWrite)
}

const (
	headlessServiceName = "hbase"
)

// HBaseReconciler reconciles a HBase object
type HBaseReconciler struct {
	client.Client
	Log      logr.Logger
	Scheme   *runtime.Scheme
	GhAdmin  gohbase.AdminClient
	GhClient gohbase.Client
}

// +kubebuilder:rbac:groups=hbase.elenskiy.co,namespace="hbase",resources=hbases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=hbase.elenskiy.co,namespace="hbase",resources=hbases/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",namespace="hbase",resources=pods,verbs=get;list;delete;watch
// +kubebuilder:rbac:groups="",namespace="hbase",resources=configmaps,verbs=*
// +kubebuilder:rbac:groups="",namespace="hbase",resources=services,verbs=*
// +kubebuilder:rbac:groups="apps",namespace="hbase",resources=statefulsets,verbs=*

func (r *HBaseReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	_ = context.Background()
	log := r.Log.WithValues("hbase", req.NamespacedName)

	log.Info("got request")

	// Fetch the App instance.
	app := &hbasev1.HBase{}
	err := r.Get(context.TODO(), req.NamespacedName, app)
	if err != nil {
		if errors.IsNotFound(err) {
			r.Log.Error(err, "hbase crd is not found")
			return ctrl.Result{}, nil
		}
		r.Log.Error(err, "failed getting hbase crd")
		return ctrl.Result{}, err
	}

	log.Info("got hbase crd")

	serviceOk, err := r.ensureService(app)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !serviceOk {
		return ctrl.Result{Requeue: true}, nil
	}
	log.Info("hbase headless service is in sync")

	// deploy configmap if it doesn't exist
	configMapName := getConfigMapName(app)
	cmOk, err := r.ensureConfigMap(app, configMapName)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !cmOk {
		return ctrl.Result{Requeue: true}, nil
	}
	log.Info("hbase configmap is in sync")

	// update hbasemaster statefulset
	masterName := types.NamespacedName{Name: "hbasemaster", Namespace: app.Namespace}
	masterSts, masterUpdated, err := r.ensureStatefulSet(app, masterName, configMapName, app.Spec.MasterSpec)
	if err != nil {
		r.Log.Error(err, "failed reconciling HBase Master StatefulSet")
		return ctrl.Result{}, err
	}
	if masterUpdated {
		log.Info("hbase master statefulset updated")
		return ctrl.Result{Requeue: true}, nil
	}
	log.Info("hbasemaster statefulset is in sync")

	// update regionserver statefulset
	rsName := types.NamespacedName{Name: "regionserver", Namespace: app.Namespace}
	rsSts, rsUpdated, err := r.ensureStatefulSet(app, rsName, configMapName, app.Spec.RegionServerSpec)
	if err != nil {
		r.Log.Error(err, "failed reconciling HBase RegionServer StatefulSet")
		return ctrl.Result{}, err
	}
	if rsUpdated {
		log.Info("hbase regionserver statefulset updated")
		return ctrl.Result{Requeue: true}, nil
	}
	log.Info("regionserver statefulset is in sync")

	// make sure there are no regions in transition.
	// we want this to happen after we've deployed all manifests in order to
	// be able to fix incorrect config and not fight with operator
	rit, err := r.isRegionsInTransition()
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get regions in transition: %v", err)
	}
	if rit {
		log.Info("there are regions in transition, wait")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	log.Info("there are no regions in transition")

	r.Log.Info("reconciling masters")
	mastersOk, err := r.ensureStatefulSetPods(context.TODO(), masterSts, r.pickMasterToDelete)
	if err != nil {
		r.Log.Error(err, "failed reconciling HBase Masters")
		return ctrl.Result{}, err
	}
	if !mastersOk {
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	r.Log.Info("reconciling regionservers")
	rsOk, err := r.ensureStatefulSetPods(context.TODO(), rsSts, r.pickRegionServerToDelete)
	if err != nil {
		r.Log.Error(err, "failed reconciling HBase RegionServers")
		return ctrl.Result{}, err
	}
	if !rsOk {
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	if err := r.deleteUnusedConfigMaps(context.TODO(), app, configMapName); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *HBaseReconciler) ensureConfigMap(hb *hbasev1.HBase, name types.NamespacedName) (bool, error) {
	configMap := &corev1.ConfigMap{}
	if err := r.Get(context.TODO(), name, configMap); err != nil {
		if errors.IsNotFound(err) {
			// deploy the configmap
			cm, err := r.configMap(hb, name)
			if err != nil {
				r.Log.Error(err, "failed generating config map")
				return false, err
			}
			if err := r.Create(context.TODO(), cm); err != nil {
				r.Log.Error(err, "failed creating config map")
				return false, err
			}
			r.Log.Info("created HBase ConfigMap", "configmap", name)
			return false, nil
		}
		r.Log.Error(err, "failed getting config map")
		return false, err
	}
	return true, nil
}

func (r *HBaseReconciler) ensureService(hb *hbasev1.HBase) (bool, error) {
	// create headless service if it doesn't exist
	foundService := &corev1.Service{}
	if err := r.Get(context.TODO(), types.NamespacedName{
		Name:      hb.Name,
		Namespace: hb.Namespace,
	}, foundService); err != nil {
		if errors.IsNotFound(err) {
			// Define and create a new headless service.
			svc := r.headlessService(hb)
			if err := r.Create(context.TODO(), svc); err != nil {
				r.Log.Error(err, "failed creating service")
				return false, err
			}
			r.Log.Info("created HBase Service")
			return false, nil
		}
		r.Log.Error(err, "failed getting service")
		return false, err
	}
	return true, nil
}

func (r *HBaseReconciler) deleteUnusedConfigMaps(ctx context.Context, hb *hbasev1.HBase,
	cmName types.NamespacedName) error {
	// clean up unused configmaps
	configMapList := &corev1.ConfigMapList{}
	listOpts := []client.ListOption{
		client.InNamespace(hb.Namespace),
		client.MatchingLabels(configMapLabels),
	}
	if err := r.List(context.TODO(), configMapList, listOpts...); err != nil {
		return err
	}

	for _, cm := range configMapList.Items {
		if cm.Name != cmName.Name {
			r.Log.Info("deleting unused ConfigMap", "name", cm.Name)
			if err := r.Delete(ctx, &cm); err != nil {
				return err
			}
		}
	}
	return nil
}

// getRegionsPerRegionServer can be fairly expensive as it scans entire hbase:meta
func (r *HBaseReconciler) getRegionsPerRegionServer(ctx context.Context) (map[string][][]byte, error) {
	// scan meta to get a regioninfo and server name
	scan, err := hrpc.NewScan(ctx,
		[]byte("hbase:meta"),
		hrpc.Families(map[string][]string{"info": []string{"sn"}}))
	if err != nil {
		return nil, err
	}

	result := map[string][][]byte{}
	scanner := r.GhClient.Scan(scan)
	for {
		res, err := scanner.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		if l := len(res.Cells); l != 1 {
			return nil, fmt.Errorf("got %v cells", l)
		}

		// get region name from row and server name from value
		// TODO: parse actual regioninfo value
		cell := res.Cells[0]
		result[string(cell.Value)] = append(
			result[string(cell.Value)],
			cell.Row[len(cell.Row)-33:len(cell.Row)-1])
	}
	return result, nil
}

// TODO: make parallel
// TODO: specify destination server
func (r *HBaseReconciler) moveRegions(ctx context.Context, regions [][]byte) error {
	for _, region := range regions {
		mr, err := hrpc.NewMoveRegion(ctx, region)
		if err != nil {
			return err
		}
		if err := r.GhAdmin.MoveRegion(mr); err != nil {
			return fmt.Errorf("failed to move region %q: %w", region, err)
		}
	}
	return nil
}

func (r *HBaseReconciler) pickRegionServerToDelete(ctx context.Context, td []*corev1.Pod) (*corev1.Pod, error) {
	if len(td) == 0 {
		// make sure the balancer is on
		sb, err := hrpc.NewSetBalancer(ctx, true)
		if err != nil {
			return nil, err
		}
		_, err = r.GhAdmin.SetBalancer(sb)
		if err != nil {
			return nil, err
		}
		return nil, nil
	}

	// make sure that balancer is off
	sb, err := hrpc.NewSetBalancer(ctx, false)
	if err != nil {
		return nil, err
	}
	_, err = r.GhAdmin.SetBalancer(sb)
	if err != nil {
		return nil, err
	}

	// pick the first regionserver
	p := td[0]

	// drain the regionserver
	rrs, err := r.getRegionsPerRegionServer(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get regions per regionservers: %w", err)
	}

	for rs, regions := range rrs {
		if strings.HasPrefix(rs, p.Name) {
			// match, drain it
			r.Log.Info("moving regions from RegionServer",
				"regionserver", rs, "pod", p.Name, "count", len(regions))
			if err := r.moveRegions(ctx, regions); err != nil {
				return nil, err
			}
			break
		}
	}
	return p, nil
}

func (r *HBaseReconciler) isRegionsInTransition() (bool, error) {
	cs, err := r.GhAdmin.ClusterStatus()
	if err != nil {
		return false, err
	}
	return len(cs.GetRegionsInTransition()) > 0, nil
}

func (r *HBaseReconciler) pickMasterToDelete(ctx context.Context, td []*corev1.Pod) (*corev1.Pod, error) {
	if len(td) == 0 {
		return nil, nil
	}

	cs, err := r.GhAdmin.ClusterStatus()
	if err != nil {
		return nil, err
	}

	// check if any of the pods to delete are backup masters
	r.Log.Info("got backup masters", "masters", cs.GetBackupMasters())
	for _, p := range td {
		for _, bm := range cs.GetBackupMasters() {
			if strings.HasPrefix(bm.GetHostName(), p.Name) {
				// match, delete it
				return p, nil
			}
		}
	}
	r.Log.Info("got active master", "master", cs.GetMaster())

	// otherwise find the pod of the active master
	for _, p := range td {
		if strings.HasPrefix(cs.GetMaster().GetHostName(), p.Name) {
			// match, delete it
			return p, nil
		}
	}
	// the pods aren't active or backup master, return error
	return nil, fmt.Errorf(
		"no active or backup masters in the list of pods to delete: active %v, backup %v",
		cs.GetMaster(), cs.GetBackupMasters())
}

func sprintPodList(l []*corev1.Pod) string {
	var podNames []string
	for _, p := range l {
		podNames = append(podNames, p.Name)
	}
	return fmt.Sprintf("%v", podNames)
}

func (r *HBaseReconciler) ensureStatefulSetPods(ctx context.Context, sts *appsv1.StatefulSet,
	pickToDelete func(ctx context.Context, td []*corev1.Pod) (*corev1.Pod, error)) (bool, error) {
	podList := &corev1.PodList{}
	listOpts := []client.ListOption{
		client.InNamespace(sts.Namespace),
		client.MatchingLabels(sts.Spec.Template.Labels),
	}
	if err := r.List(context.TODO(), podList, listOpts...); err != nil {
		return false, err
	}

	r.Log.Info("matched pods", "statefulset", sts.Name, "pods", len(podList.Items))

	// sort pods by name to have things restarted predictably
	sort.Slice(podList.Items, func(i, j int) bool {
		return podList.Items[i].Name < podList.Items[j].Name
	})

	// make sure that all pods are up by checking that all containers are ready.
	// the loop exists if any pod is not ready.
	var toDelete []*corev1.Pod
	for _, p := range podList.Items {
		p := p
		if p.DeletionTimestamp != nil {
			// if pod is already terminating, skip it
			// wait for it to terminate
			r.Log.Info("pod is terminating", "pod", p.Name)
			return false, nil
		}

		ssr, ok := p.Labels[appsv1.StatefulSetRevisionLabel]
		if !ok {
			return false, fmt.Errorf("no %q label defined in pod %q",
				appsv1.StatefulSetRevisionLabel, p.Name)
		}
		isRecent := ssr == sts.Status.UpdateRevision

		// check if all containers are ready.
		for _, s := range p.Status.ContainerStatuses {
			if s.Ready {
				continue
			}
			if isRecent {
				// if revision matches, wait for pod to become ready.
				// in case there's a misconfig, it will halt here and
				// won't proceed restarting any other pods
				r.Log.Info("pod is not ready", "name", p.Name)
				return false, nil
			}
			// otherwise, the pod isn't ready and has old revision,
			// we can remove it without hesitation
			r.Log.Info("deleting pod", "name", p.Name)
			return false, r.Delete(ctx, &p)
		}

		if !isRecent {
			// pod is healthy, but not of recent version, add to delete list
			toDelete = append(toDelete, &p)
		}
	}

	r.Log.Info("pick pod to delete", "statefulset", sts.Name, "pods", sprintPodList(toDelete))

	// delete one pod at a time
	p, err := pickToDelete(ctx, toDelete)
	if err != nil {
		r.Log.Error(err, "failed to pick pod to delete")
		return false, fmt.Errorf("failed to pick pod to delete: %w", err)
	}
	if p != nil {
		r.Log.Info("deleting pod", "name", p.Name)
		return false, r.Delete(ctx, p)
	}

	r.Log.Info("statefulsets pods are reconciled")
	// all is perfect, ensured
	return true, nil

}

func (r *HBaseReconciler) ensureStatefulSet(hb *hbasev1.HBase,
	stsName, cmName types.NamespacedName, ss hbasev1.ServerSpec) (*appsv1.StatefulSet, bool, error) {
	actual := &appsv1.StatefulSet{}
	expected, expectedRevision := r.statefulSet(stsName, cmName, ss)
	if err := r.Get(context.TODO(), stsName, actual); err != nil {
		if errors.IsNotFound(err) {
			if err = controllerutil.SetControllerReference(hb, expected, r.Scheme); err != nil {
				return nil, false, err
			}
			if err = r.Create(context.TODO(), expected); err != nil {
				return nil, false, err
			}
			r.Log.Info("created StatefulSet", "name", stsName)
			return expected, true, nil
		}
		return nil, false, err
	}

	if actual.Annotations != nil && actual.Annotations[HBaseControllerRevisionKey] == expectedRevision {
		r.Log.Info("StatefulSet is up-to-date", "name", stsName)
		return actual, false, nil
	}

	// update
	if err := r.Update(context.TODO(), expected); err != nil {
		r.Log.Info("StatefulSet is up-to-date", "name", stsName)
		return nil, false, err
	}
	r.Log.Info("updated StatefulSet", "name", stsName)
	return expected, true, nil

}

func configMapVolume(cmName types.NamespacedName) corev1.Volume {
	return corev1.Volume{
		Name: "config",
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				DefaultMode: pointer.Int32Ptr(420),
				LocalObjectReference: corev1.LocalObjectReference{
					Name: cmName.Name,
				},
			},
		},
	}
}

func cloneMap(orig map[string]string) map[string]string {
	out := make(map[string]string, len(orig))
	for k, v := range orig {
		out[k] = v
	}
	return out
}

const HBaseControllerRevisionKey = "hbase-controller-revision"

func (r *HBaseReconciler) statefulSet(
	stsName, cmName types.NamespacedName, ss hbasev1.ServerSpec) (*appsv1.StatefulSet, string) {
	spec := (&ss.PodSpec).DeepCopy()
	spec.Volumes = append(spec.Volumes, configMapVolume(cmName))
	stsSpec := appsv1.StatefulSetSpec{
		Replicas:            pointer.Int32Ptr(ss.Count),
		PodManagementPolicy: appsv1.ParallelPodManagement,
		// OnDelete because we managed the pod restarts ourselves
		UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
			Type: appsv1.OnDeleteStatefulSetStrategyType,
		},
		ServiceName: headlessServiceName,
		Selector: &metav1.LabelSelector{
			// TODO: make sure these are immutable
			MatchLabels: cloneMap(ss.Metadata.Labels),
		},
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      cloneMap(ss.Metadata.Labels),
				Annotations: cloneMap(ss.Metadata.Annotations),
			},
			Spec: *spec,
		},
	}
	h := sha256.New()
	DeepHashObject(h, &stsSpec)
	rev := fmt.Sprintf("%x", h.Sum(nil))
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      stsName.Name,
			Namespace: stsName.Namespace,
			Annotations: map[string]string{
				HBaseControllerRevisionKey: rev,
			},
		},
		Spec: stsSpec,
	}, rev
}

func getConfigMapName(hb *hbasev1.HBase) types.NamespacedName {
	h := sha256.New()
	DeepHashObject(h, hb.Spec.Config.Data)
	checksum := fmt.Sprintf("%x", h.Sum(nil))[:8]
	return types.NamespacedName{
		Name:      "config-" + checksum,
		Namespace: hb.Namespace,
	}
}

var configMapLabels = map[string]string{
	"config": "core",
}

func (r *HBaseReconciler) configMap(hb *hbasev1.HBase,
	name types.NamespacedName) (*corev1.ConfigMap, error) {
	immutable := true
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name.Name,
			Namespace: name.Namespace,
			Labels:    configMapLabels,
		},
		Immutable: &immutable,
		Data:      hb.Spec.Config.Data,
	}
	if err := controllerutil.SetControllerReference(hb, cm, r.Scheme); err != nil {
		return nil, err
	}
	return cm, nil
}

func (r *HBaseReconciler) headlessService(hb *hbasev1.HBase) *corev1.Service {
	srv := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      headlessServiceName,
			Namespace: hb.Namespace,
		},
		Spec: corev1.ServiceSpec{
			Type:      corev1.ServiceTypeClusterIP,
			ClusterIP: "None",
			Selector:  map[string]string{"app": "hbase"},
			Ports: []corev1.ServicePort{
				corev1.ServicePort{
					Name: "placeholder",
					Port: 1234,
				},
			},
		},
	}
	controllerutil.SetControllerReference(hb, srv, r.Scheme)
	return srv
}

func (r *HBaseReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&hbasev1.HBase{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&appsv1.StatefulSet{}).
		Complete(r)
}
