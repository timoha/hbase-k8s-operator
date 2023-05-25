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
	"bytes"
	"container/heap"
	"context"
	"crypto/sha256"
	"fmt"
	"hash"
	"sort"
	"strconv"
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
	Log     logr.Logger
	Scheme  *runtime.Scheme
	GhAdmin gohbase.AdminClient
}

// +kubebuilder:rbac:groups=hbase.elenskiy.co,namespace="hbase",resources=hbases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=hbase.elenskiy.co,namespace="hbase",resources=hbases/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",namespace="hbase",resources=pods,verbs=get;list;delete;watch
// +kubebuilder:rbac:groups="",namespace="hbase",resources=configmaps,verbs=*
// +kubebuilder:rbac:groups="",namespace="hbase",resources=services,verbs=*
// +kubebuilder:rbac:groups="apps",namespace="hbase",resources=statefulsets,verbs=*

func (r *HBaseReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
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

func (r *HBaseReconciler) getRegionsPerRegionServer(ctx context.Context) (map[string][][]byte, error) {
	// get regions via cluster status because this way we can get
	// regionservers that don't have any regions
	cs, err := r.GhAdmin.ClusterStatus()
	if err != nil {
		return nil, fmt.Errorf("getting cluster status: %w", err)
	}

	// if some fields are nil, just let it panic as it's not expected
	// and we won't be able to recover from that anyway
	result := map[string][][]byte{}
	for _, s := range cs.GetLiveServers() {
		sn := fmt.Sprintf("%s,%d,%d", s.Server.GetHostName(),
			s.Server.GetPort(), s.Server.GetStartCode())
		result[sn] = [][]byte{} // add even if there are no regions
		for _, r := range s.GetServerLoad().GetRegionLoads() {
			rn := r.GetRegionSpecifier().GetValue()
			if bytes.HasPrefix(rn, []byte("hbase:meta")) {
				continue
			}
			rn = rn[len(rn)-33 : len(rn)-1]
			result[sn] = append(result[sn], rn)
		}
	}
	return result, nil
}

type rsCount struct {
	serverName  string
	regionCount int
}

type regionServerTargets []*rsCount

func (rst regionServerTargets) Len() int { return len(rst) }
func (rst regionServerTargets) Less(i, j int) bool {
	// we want to pop regionserver with lowest number of regions
	return rst[i].regionCount < rst[j].regionCount
}

func (rst regionServerTargets) Swap(i, j int) {
	rst[i], rst[j] = rst[j], rst[i]
}

func (rst *regionServerTargets) Push(x interface{}) {
	item := x.(*rsCount)
	*rst = append(*rst, item)
}

func (rst *regionServerTargets) Pop() interface{} {
	old := *rst
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	*rst = old[0 : n-1]
	return item
}

// TODO: make parallel
func (r *HBaseReconciler) moveRegions(ctx context.Context, regions [][]byte, targets regionServerTargets) error {
	// important to understand that this heuristic to deside which regionserver to move
	// to does not account for the most recent state of the cluster. For example, if some
	// regionserver were to be restarted during region moving, the region counts will not be updated.
	var err error
	for _, region := range regions {
		var mr *hrpc.MoveRegion
		if targets.Len() > 0 {
			// get the regionserver with least regions
			rc := heap.Pop(&targets).(*rsCount)
			r.Log.Info("moving regions to regionserver with least regions", "region", string(region),
				"target", rc.serverName,
				"current_count", rc.regionCount)
			mr, err = hrpc.NewMoveRegion(ctx, region, hrpc.WithDestinationRegionServer(rc.serverName))

			// update the count and add it back to priority heap
			rc.regionCount++
			heap.Push(&targets, rc)
		} else {
			// moving regions without a particular target - this is not an error case and guaranteed to hit when draining the first regionserver in the cluster
			r.Log.Info("regionservers are balanced and there isn't a regionserver with least regions; moving regions without particular regionserver target", "region", string(region))
			mr, err = hrpc.NewMoveRegion(ctx, region)
		}
		if err != nil {
			return fmt.Errorf("creating request to move region %q: %w", region, err)
		}
		if err := r.GhAdmin.MoveRegion(mr); err != nil {
			if strings.Contains(err.Error(), "DoNotRetryIOException") {
				// means the region is not open
				continue
			}
			return fmt.Errorf("moving region %q: %w", region, err)
		}
	}
	return nil
}

func (r *HBaseReconciler) pickRegionServerToDelete(ctx context.Context, td, utd []*corev1.Pod) (*corev1.Pod, error) {
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

	rrs, err := r.getRegionsPerRegionServer(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get regions per regionservers: %w", err)
	}

	// pick the first regionserver
	p := td[0]

	// get regions to move and region count per up-to-date regionserver
	// TODO: this is n^2 for the case all other regionservers are up-to-date
	var toMove [][]byte
	var source string
	var targets regionServerTargets
	for rs, regions := range rrs {
		if strings.HasPrefix(rs, p.Name+".") {
			// move regions
			toMove = regions
			source = rs
		}
		// check if this is one of the up-to-date regionservers
		for _, up := range utd {
			if strings.HasPrefix(rs, up.Name+".") {
				targets = append(targets, &rsCount{
					serverName:  rs,
					regionCount: len(regions),
				})
				break
			}
		}
	}
	heap.Init(&targets)

	r.Log.Info("moving regions from RegionServer",
		"regionserver", source, "pod", p.Name, "count", len(toMove),
		"target_count", targets.Len())
	if err := r.moveRegions(ctx, toMove, targets); err != nil {
		return nil, err
	}

	return p, nil
}

// regionsInTransition returns the number of regions in transition
func (r *HBaseReconciler) regionsInTransition() (int, error) {
	cs, err := r.GhAdmin.ClusterStatus()
	if err != nil {
		return -1, err
	}
	return len(cs.GetRegionsInTransition()), nil
}

func (r *HBaseReconciler) pickMasterToDelete(ctx context.Context, td, utd []*corev1.Pod) (*corev1.Pod, error) {
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
			if strings.HasPrefix(bm.GetHostName(), p.Name+".") {
				// match, delete it
				return p, nil
			}
		}
	}
	r.Log.Info("got active master", "master", cs.GetMaster())

	// otherwise find the pod of the active master
	for _, p := range td {
		if strings.HasPrefix(cs.GetMaster().GetHostName(), p.Name+".") {
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

func orderPodListByName(pl *corev1.PodList) {
	// sort in reverse ordinal order to best simulate how statefulset controller
	// updates pods in a statefulset
	// https://kubernetes.io/docs/tutorials/stateful-application/basic-stateful-set/#rolling-update
	// sts pods are in format <sts name>-N
	sort.Slice(pl.Items, func(i, j int) bool {
		arg1, _ := strconv.Atoi(strings.Split(pl.Items[i].Name, "-")[1])
		arg2, _ := strconv.Atoi(strings.Split(pl.Items[j].Name, "-")[1])
		return arg1 > arg2
	})
}

func (r *HBaseReconciler) ensureStatefulSetPods(ctx context.Context, sts *appsv1.StatefulSet,
	pickToDelete func(ctx context.Context, td, utd []*corev1.Pod) (*corev1.Pod, error)) (bool, error) {
	podList := &corev1.PodList{}
	listOpts := []client.ListOption{
		client.InNamespace(sts.Namespace),
		client.MatchingLabels{HBaseControllerNameKey: sts.Spec.Template.Labels[HBaseControllerNameKey]},
	}
	if err := r.List(context.TODO(), podList, listOpts...); err != nil {
		return false, err
	}

	r.Log.Info("matched pods", "statefulset", sts.Name, "pods", len(podList.Items))

	orderPodListByName(podList)

	// make sure that all pods are up by checking that all containers are ready.
	// the loop exists if any pod is not ready.
	var toDelete, upToDate []*corev1.Pod
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
		} else {
			// pod is up-to-date, keep track of it
			upToDate = append(upToDate, &p)
		}
	}

	r.Log.Info("pick pod to delete", "StatefulSet", sts.Name, "pods", sprintPodList(toDelete))

	// delete one pod at a time
	p, err := pickToDelete(ctx, toDelete, upToDate)
	if err != nil {
		r.Log.Error(err, "failed to pick pod to delete")
		return false, fmt.Errorf("failed to pick pod to delete: %w", err)
	}
	if p != nil {
		r.Log.Info("deleting pod", "name", p.Name)
		return false, r.Delete(ctx, p)
	}

	r.Log.Info("pods are up to date", "StatefulSet", sts.Name)
	// all is perfect, ensured
	return true, nil

}

func (r *HBaseReconciler) ensureStatefulSet(hb *hbasev1.HBase,
	stsName, cmName types.NamespacedName, ss hbasev1.ServerSpec) (*appsv1.StatefulSet, bool, error) {
	actual := &appsv1.StatefulSet{}
	expected, expectedRevision := r.statefulSet(hb, stsName, cmName, ss)
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

	var actualRevision string
	if actual.Annotations != nil {
		actualRevision = actual.Annotations[HBaseControllerRevisionKey]
	}

	r.Log.Info("StatefulSet reconciliation",
		"name", stsName,
		"revision is up to date", actualRevision == expectedRevision,
		"expected replicas", *actual.Spec.Replicas,
		"actual replicas", *expected.Spec.Replicas)

	if *actual.Spec.Replicas != *expected.Spec.Replicas || actualRevision != expectedRevision {
		// Update StatefulSet to expected configuration
		if err := r.Update(context.TODO(), expected); err != nil {
			return nil, false, err
		}
		r.Log.Info("updated StatefulSet", "name", stsName)
		return expected, true, nil
	}

	r.Log.Info("StatefulSet is up to date", "name", stsName)
	return actual, false, nil
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

// cloneMap clones a map and applies patches without overwrites
func cloneMap(ms ...map[string]string) map[string]string {
	a := map[string]string{}
	for _, p := range ms {
		for k, v := range p {
			if _, ok := a[k]; ok {
				continue
			}
			a[k] = v
		}
	}
	return a
}

const (
	HBaseControllerNameKey     = "hbase-controller-name"
	HBaseControllerRevisionKey = "hbase-controller-revision"
)

var (
	ignoreTemplateMetadataAnnotations = map[string]struct{}{
		"kubectl.kubernetes.io/last-applied-configuration": struct{}{},
	}
)

func (r *HBaseReconciler) statefulSet(hb *hbasev1.HBase,
	stsName, cmName types.NamespacedName, ss hbasev1.ServerSpec) (*appsv1.StatefulSet, string) {
	spec := (&ss.PodSpec).DeepCopy()
	spec.Volumes = append(spec.Volumes, configMapVolume(cmName))

	templateMetadataAnnotations := cloneMap(ss.Metadata.Annotations, hb.Annotations)
	filteredTemplateMetadataAnnotations := make(map[string]string)
	for k, v := range templateMetadataAnnotations {
		if _, ok := ignoreTemplateMetadataAnnotations[k]; !ok {
			filteredTemplateMetadataAnnotations[k] = v
		}
	}

	stsSpec := appsv1.StatefulSetSpec{
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
				Labels: cloneMap(ss.Metadata.Labels, hb.Labels, map[string]string{
					HBaseControllerNameKey: stsName.Name,
				}),
				Annotations: filteredTemplateMetadataAnnotations,
			},
			Spec: *spec,
		},
	}

	h := sha256.New()
	DeepHashObject(h, &stsSpec)
	rev := fmt.Sprintf("%x", h.Sum(nil))

	// After revision hash calculation, actually update replica count
	stsSpec.Replicas = pointer.Int32Ptr(ss.Count)

	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      stsName.Name,
			Namespace: stsName.Namespace,
			Annotations: cloneMap(
				map[string]string{HBaseControllerRevisionKey: rev}, hb.Annotations),
			Labels: cloneMap(hb.Labels),
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
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name.Name,
			Namespace:   name.Namespace,
			Labels:      cloneMap(configMapLabels, hb.Labels),
			Annotations: cloneMap(hb.Annotations),
		},
		Immutable: pointer.BoolPtr(true),
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
			Name:        headlessServiceName,
			Namespace:   hb.Namespace,
			Labels:      cloneMap(hb.Labels),
			Annotations: cloneMap(hb.Annotations),
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
