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
	"errors"
	"path/filepath"
	"testing"
	"time"

	gomock "github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/tsuna/gohbase/pb"
	"github.com/tsuna/gohbase/test/mock"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/utils/pointer"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/envtest/printer"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	hbasev1 "github.com/timoha/hbase-k8s-operator/api/v1"
	// +kubebuilder:scaffold:imports
)

// These tests use Ginkgo (BDD-style Go testing framework). Refer to
// http://onsi.github.io/ginkgo/ to learn more about Ginkgo.

var cfg *rest.Config
var k8sClient client.Client
var testEnv *envtest.Environment
var ghAdmin *mock.MockAdminClient

func TestAPIs(t *testing.T) {
	RegisterFailHandler(Fail)

	RunSpecsWithDefaultAndCustomReporters(t,
		"Controller Suite",
		[]Reporter{printer.NewlineReporter{}})
}

func makeHBaseSpec(confData map[string]string) *hbasev1.HBase {
	return &hbasev1.HBase{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "hbase",
			Namespace: "default",
		},
		Spec: hbasev1.HBaseSpec{
			RegionServerSpec: hbasev1.ServerSpec{
				Count: 3,
				Metadata: hbasev1.ServerMetadata{
					Labels: map[string]string{
						"hbase": "regionserver",
					},
				},
				PodSpec: corev1.PodSpec{
					Containers: []corev1.Container{
						corev1.Container{
							Name: "server",
							VolumeMounts: []corev1.VolumeMount{
								corev1.VolumeMount{
									Name:      "config",
									MountPath: "/hbase/conf/hbase-site.xml",
									SubPath:   "hbase-site.xml",
								},
							},
						},
					},
				},
			},
			MasterSpec: hbasev1.ServerSpec{
				Count: 2,
				Metadata: hbasev1.ServerMetadata{
					Labels: map[string]string{
						"hbase": "master",
					},
				},
				PodSpec: corev1.PodSpec{
					Containers: []corev1.Container{
						corev1.Container{
							Name: "server",
							VolumeMounts: []corev1.VolumeMount{
								corev1.VolumeMount{
									Name:      "config",
									MountPath: "/hbase/conf/hbase-site.xml",
									SubPath:   "hbase-site.xml",
								},
							},
						},
					},
				},
			},
			Config: hbasev1.ConfigMap{
				Data: confData,
			},
		},
	}
}

var _ = BeforeSuite(func(done Done) {
	logf.SetLogger(zap.LoggerTo(GinkgoWriter, true))
	gomockCtrl := gomock.NewController(GinkgoT())
	defer gomockCtrl.Finish()
	ghAdmin = mock.NewMockAdminClient(gomockCtrl)
	ghAdmin.EXPECT().ClusterStatus().AnyTimes().Return(&pb.ClusterStatus{}, nil)
	ghAdmin.EXPECT().SetBalancer(gomock.Any()).AnyTimes()

	By("bootstrapping test environment")
	testEnv = &envtest.Environment{
		CRDDirectoryPaths: []string{filepath.Join("..", "config", "crd", "bases")},
	}

	var err error
	cfg, err = testEnv.Start()
	Expect(err).ToNot(HaveOccurred())
	Expect(cfg).ToNot(BeNil())

	err = hbasev1.AddToScheme(scheme.Scheme)
	Expect(err).NotTo(HaveOccurred())

	// +kubebuilder:scaffold:scheme

	k8sManager, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:    scheme.Scheme,
		Namespace: "default",
	})
	Expect(err).ToNot(HaveOccurred())

	err = (&HBaseReconciler{
		Client:  k8sManager.GetClient(),
		Log:     ctrl.Log.WithName("controllers").WithName("HBase"),
		Scheme:  k8sManager.GetScheme(),
		GhAdmin: ghAdmin,
	}).SetupWithManager(k8sManager)
	Expect(err).ToNot(HaveOccurred())

	go func() {
		err = k8sManager.Start(ctrl.SetupSignalHandler())
		Expect(err).ToNot(HaveOccurred())
	}()

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	Expect(err).ToNot(HaveOccurred())
	Expect(k8sClient).ToNot(BeNil())

	close(done)
}, 60)

var _ = Describe("HBase controller", func() {
	var (
		timeout   = time.Second * 10
		interval  = time.Second * 1
		namespace = "default"
		ctx       = context.Background()
	)

	Context("When deploying HBase CRD", func() {
		It("Should deploy all resources", func() {
			By("By creating a new HBase")
			hb := makeHBaseSpec(map[string]string{"hbase-site.xml": "conf"})
			Expect(k8sClient.Create(ctx, hb)).Should(Succeed())
			hbaseLookupKey := types.NamespacedName{Name: "hbase", Namespace: namespace}
			createdHBase := &hbasev1.HBase{}

			Eventually(func() error {
				return k8sClient.Get(ctx, hbaseLookupKey, createdHBase)
			}, timeout, interval).Should(Succeed())

			By("By checking HBase deployed hbase service")
			Eventually(func() error {
				createdService := &corev1.Service{}
				return k8sClient.Get(ctx, hbaseLookupKey, createdService)
			}, timeout, interval).Should(Succeed())

			var createdConfigMap *corev1.ConfigMap
			By("By checking HBase deployed one config map")
			Eventually(func() (int, error) {
				configMapList := &corev1.ConfigMapList{}
				listOpts := []client.ListOption{
					client.InNamespace(namespace),
					client.MatchingLabels(map[string]string{"config": "core"}),
				}
				if err := k8sClient.List(ctx, configMapList, listOpts...); err != nil {
					return 0, err
				}
				l := len(configMapList.Items)
				if l != 1 {
					return l, nil
				}
				createdConfigMap = &(configMapList.Items[0])
				return l, nil
			}, timeout, interval).Should(Equal(1))

			By("By checking HBase deployed master statefulset")
			existingMasterStatefulSet := &appsv1.StatefulSet{}
			Eventually(func() error {
				masterName := types.NamespacedName{Name: "hbasemaster", Namespace: namespace}
				return k8sClient.Get(ctx, masterName, existingMasterStatefulSet)
			}, timeout, interval).Should(Succeed())

			By("By checking master statefulset has correct number of replicas")
			Ω(hb.Spec.MasterSpec.Count).Should(Equal(int32(2)))

			By("By checking master statefulset has mounted correct confgmap")
			vs := existingMasterStatefulSet.Spec.Template.Spec.Volumes
			Ω(len(vs)).Should(Equal(1))

			Ω(vs[0]).Should(Equal(corev1.Volume{
				Name: "config",
				VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						DefaultMode: pointer.Int32Ptr(420),
						LocalObjectReference: corev1.LocalObjectReference{
							Name: createdConfigMap.Name,
						},
					},
				},
			}))

			By("By checking HBase deployed master statefulset has annotation")
			_, ok := existingMasterStatefulSet.Annotations["hbase-controller-revision"]
			Ω(ok).Should(BeTrue())

			By("By checking HBase deployed regionserver statefulset")
			createdRegionServerStatefulSet := &appsv1.StatefulSet{}
			Eventually(func() error {
				rsName := types.NamespacedName{Name: "regionserver", Namespace: namespace}
				return k8sClient.Get(ctx, rsName, createdRegionServerStatefulSet)
			}, timeout, interval).Should(Succeed())

			By("By checking regionserver statefulset has correct number of replicas")
			Ω(hb.Spec.RegionServerSpec.Count).Should(Equal(int32(3)))

			By("By checking regionserver statefulset has mounted correct confgmap")
			vs = createdRegionServerStatefulSet.Spec.Template.Spec.Volumes
			Ω(len(vs)).Should(Equal(1))

			Ω(vs[0]).Should(Equal(corev1.Volume{
				Name: "config",
				VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						DefaultMode: pointer.Int32Ptr(420),
						LocalObjectReference: corev1.LocalObjectReference{
							Name: createdConfigMap.Name,
						},
					},
				},
			}))

			By("By checking regionserver statefulset has annotation")
			_, ok = createdRegionServerStatefulSet.Annotations["hbase-controller-revision"]
			Ω(ok).Should(BeTrue())
		})
	})

	// TODO This test needs major refactoring
	Context("When updating config of HBase CRD", func() {
		It("Should redeploy or update resources", func() {
			By("By updating HBase")

			hbaseLookupKey := types.NamespacedName{Name: "hbase", Namespace: namespace}
			hb := &hbasev1.HBase{}
			Eventually(func() error {
				return k8sClient.Get(ctx, hbaseLookupKey, hb)
			}, timeout, interval).Should(Succeed())

			// --------------------------- TEST 1 ---------------------------
			// get old configmap
			var existingCms []corev1.ConfigMap
			Eventually(func() (int, error) {
				configMapList := &corev1.ConfigMapList{}
				listOpts := []client.ListOption{
					client.InNamespace(namespace),
					client.MatchingLabels(map[string]string{"config": "core"}),
				}
				if err := k8sClient.List(ctx, configMapList, listOpts...); err != nil {
					return 0, err
				}
				l := len(configMapList.Items)
				if l != 1 {
					return l, nil
				}
				existingCms = configMapList.Items
				return l, nil
			}, timeout, interval).Should(Equal(1))

			// get old master sts
			existingMasterStatefulSet := &appsv1.StatefulSet{}
			Eventually(func() error {
				masterName := types.NamespacedName{Name: "hbasemaster", Namespace: namespace}
				return k8sClient.Get(ctx, masterName, existingMasterStatefulSet)
			}, timeout, interval).Should(Succeed())
			oldMasterAnnotation := existingMasterStatefulSet.Annotations["hbase-controller-revision"]

			// get old rs sts
			existingRsStatefulSet := &appsv1.StatefulSet{}
			Eventually(func() error {
				rsName := types.NamespacedName{Name: "regionserver", Namespace: namespace}
				return k8sClient.Get(ctx, rsName, existingRsStatefulSet)
			}, timeout, interval).Should(Succeed())
			oldRsAnnotation := existingRsStatefulSet.Annotations["hbase-controller-revision"]

			// Different cm than initial spec, but preserve replica counts as initial spec
			// to test for revision SHA change
			newHB := makeHBaseSpec(map[string]string{"hbase-site.xml": "conf2"})
			hb.Spec.Config = newHB.Spec.Config
			hb.Spec.MasterSpec.Count = 2
			hb.Spec.RegionServerSpec.Count = 3
			Expect(k8sClient.Update(ctx, hb)).Should(Succeed())

			oldConfigMaps := existingCms
			By("By checking HBase deployed new config map")
			Eventually(func() ([]corev1.ConfigMap, error) {
				configMapList := &corev1.ConfigMapList{}
				listOpts := []client.ListOption{
					client.InNamespace(namespace),
					client.MatchingLabels(map[string]string{"config": "core"}),
				}
				if err := k8sClient.List(ctx, configMapList, listOpts...); err != nil {
					return nil, err
				}
				return configMapList.Items, nil
			}, timeout, interval).ShouldNot(Equal(oldConfigMaps))

			By("By checking HBase updated master statefulset revision")
			Eventually(func() (string, error) {
				masterName := types.NamespacedName{Name: "hbasemaster", Namespace: namespace}
				if err := k8sClient.Get(ctx, masterName, existingMasterStatefulSet); err != nil {
					return oldMasterAnnotation, err
				}
				masterAnnotation, ok := existingMasterStatefulSet.Annotations["hbase-controller-revision"]
				if !ok {
					return oldMasterAnnotation, errors.New("no annotation")
				}
				return masterAnnotation, nil
			}, timeout, interval).ShouldNot(Equal(oldMasterAnnotation))

			By("By checking master statefulset has not updated replicas")
			Eventually(func() (int, error) {
				rsName := types.NamespacedName{Name: "hbasemaster", Namespace: namespace}
				if err := k8sClient.Get(ctx, rsName, existingMasterStatefulSet); err != nil {
					return 0, err
				}
				return int(*existingMasterStatefulSet.Spec.Replicas), nil
			}, timeout, interval).Should(Equal(2))

			By("By checking HBase updated regionserver sts revision annotation")
			Eventually(func() (string, error) {
				rsName := types.NamespacedName{Name: "regionserver", Namespace: namespace}
				if err := k8sClient.Get(ctx, rsName, existingRsStatefulSet); err != nil {
					return oldRsAnnotation, err
				}
				rsAnnotation, ok := existingRsStatefulSet.Annotations["hbase-controller-revision"]
				if !ok {
					return oldRsAnnotation, errors.New("no annotation")
				}
				return rsAnnotation, nil
			}, timeout, interval).ShouldNot(Equal(oldRsAnnotation))

			By("By checking regionserver statefulset has not updated replicas")
			Eventually(func() (int, error) {
				rsName := types.NamespacedName{Name: "regionserver", Namespace: namespace}
				if err := k8sClient.Get(ctx, rsName, existingRsStatefulSet); err != nil {
					return 0, err
				}
				return int(*existingRsStatefulSet.Spec.Replicas), nil
			}, timeout, interval).Should(Equal(3))

			// --------------------------- TEST 2 ---------------------------
			// get old configmap
			Eventually(func() (int, error) {
				configMapList := &corev1.ConfigMapList{}
				listOpts := []client.ListOption{
					client.InNamespace(namespace),
					client.MatchingLabels(map[string]string{"config": "core"}),
				}
				if err := k8sClient.List(ctx, configMapList, listOpts...); err != nil {
					return 0, err
				}
				l := len(configMapList.Items)
				if l != 1 {
					return l, nil
				}
				existingCms = configMapList.Items
				return l, nil
			}, timeout, interval).Should(Equal(1))

			// get old master sts
			existingMasterStatefulSet = &appsv1.StatefulSet{}
			Eventually(func() error {
				masterName := types.NamespacedName{Name: "hbasemaster", Namespace: namespace}
				return k8sClient.Get(ctx, masterName, existingMasterStatefulSet)
			}, timeout, interval).Should(Succeed())
			oldMasterAnnotation = existingMasterStatefulSet.Annotations["hbase-controller-revision"]

			// get old rs sts
			existingRsStatefulSet = &appsv1.StatefulSet{}
			Eventually(func() error {
				rsName := types.NamespacedName{Name: "regionserver", Namespace: namespace}
				return k8sClient.Get(ctx, rsName, existingRsStatefulSet)
			}, timeout, interval).Should(Succeed())
			oldRsAnnotation = existingRsStatefulSet.Annotations["hbase-controller-revision"]

			// No cm updated (or other conf) so SHA should remain the same.
			// Update counts to get new replica counts
			hb.Spec.MasterSpec.Count = 1
			hb.Spec.RegionServerSpec.Count = 5
			Expect(k8sClient.Update(ctx, hb)).Should(Succeed())

			oldConfigMaps = existingCms
			By("By checking HBase configmap was not updated")
			Eventually(func() ([]corev1.ConfigMap, error) {
				configMapList := &corev1.ConfigMapList{}
				listOpts := []client.ListOption{
					client.InNamespace(namespace),
					client.MatchingLabels(map[string]string{"config": "core"}),
				}
				if err := k8sClient.List(ctx, configMapList, listOpts...); err != nil {
					return nil, err
				}
				return configMapList.Items, nil
			}, timeout, interval).Should(Equal(oldConfigMaps))

			By("By checking HBase master sts revision was not updated")
			Eventually(func() (string, error) {
				masterName := types.NamespacedName{Name: "hbasemaster", Namespace: namespace}
				if err := k8sClient.Get(ctx, masterName, existingMasterStatefulSet); err != nil {
					return oldMasterAnnotation, err
				}
				masterAnnotation, ok := existingMasterStatefulSet.Annotations["hbase-controller-revision"]
				if !ok {
					return oldMasterAnnotation, errors.New("no annotation")
				}
				return masterAnnotation, nil
			}, timeout, interval).Should(Equal(oldMasterAnnotation))

			By("By checking HBase master statefulset updated replicas")
			Eventually(func() (int, error) {
				rsName := types.NamespacedName{Name: "hbasemaster", Namespace: namespace}
				if err := k8sClient.Get(ctx, rsName, existingMasterStatefulSet); err != nil {
					return 0, err
				}
				return int(*existingMasterStatefulSet.Spec.Replicas), nil
			}, timeout, interval).Should(Equal(1))

			By("By checking HBase regionserver sts revision was not updated")
			Eventually(func() (string, error) {
				rsName := types.NamespacedName{Name: "regionserver", Namespace: namespace}
				if err := k8sClient.Get(ctx, rsName, existingRsStatefulSet); err != nil {
					return oldRsAnnotation, err
				}
				rsAnnotation, ok := existingRsStatefulSet.Annotations["hbase-controller-revision"]
				if !ok {
					return oldRsAnnotation, errors.New("no annotation")
				}
				return rsAnnotation, nil
			}, timeout, interval).Should(Equal(oldRsAnnotation))

			By("By checking HBase regionserver sts updated replicas")
			Eventually(func() (int, error) {
				rsName := types.NamespacedName{Name: "regionserver", Namespace: namespace}
				if err := k8sClient.Get(ctx, rsName, existingRsStatefulSet); err != nil {
					return 0, err
				}
				return int(*existingRsStatefulSet.Spec.Replicas), nil
			}, timeout, interval).Should(Equal(5))

			// --------------------------- TEST 3 ---------------------------
			// get old configmap
			Eventually(func() (int, error) {
				configMapList := &corev1.ConfigMapList{}
				listOpts := []client.ListOption{
					client.InNamespace(namespace),
					client.MatchingLabels(map[string]string{"config": "core"}),
				}
				if err := k8sClient.List(ctx, configMapList, listOpts...); err != nil {
					return 0, err
				}
				l := len(configMapList.Items)
				if l != 1 {
					return l, nil
				}
				existingCms = configMapList.Items
				return l, nil
			}, timeout, interval).Should(Equal(1))

			// get old master sts
			existingMasterStatefulSet = &appsv1.StatefulSet{}
			Eventually(func() error {
				masterName := types.NamespacedName{Name: "hbasemaster", Namespace: namespace}
				return k8sClient.Get(ctx, masterName, existingMasterStatefulSet)
			}, timeout, interval).Should(Succeed())
			oldMasterAnnotation = existingMasterStatefulSet.Annotations["hbase-controller-revision"]

			// get old rs sts
			existingRsStatefulSet = &appsv1.StatefulSet{}
			Eventually(func() error {
				rsName := types.NamespacedName{Name: "regionserver", Namespace: namespace}
				return k8sClient.Get(ctx, rsName, existingRsStatefulSet)
			}, timeout, interval).Should(Succeed())
			oldRsAnnotation = existingRsStatefulSet.Annotations["hbase-controller-revision"]

			// Update configmap and replica counts
			newHB = makeHBaseSpec(map[string]string{"hbase-site.xml": "conf3"})
			hb.Spec.Config = newHB.Spec.Config
			hb.Spec.MasterSpec.Count = 2
			hb.Spec.RegionServerSpec.Count = 3
			Expect(k8sClient.Update(ctx, hb)).Should(Succeed())

			oldConfigMaps = existingCms
			By("By checking HBase configmap is updated")
			Eventually(func() ([]corev1.ConfigMap, error) {
				configMapList := &corev1.ConfigMapList{}
				listOpts := []client.ListOption{
					client.InNamespace(namespace),
					client.MatchingLabels(map[string]string{"config": "core"}),
				}
				if err := k8sClient.List(ctx, configMapList, listOpts...); err != nil {
					return nil, err
				}
				return configMapList.Items, nil
			}, timeout, interval).ShouldNot(Equal(oldConfigMaps))

			By("By checking HBase master sts revision was updated")
			Eventually(func() (string, error) {
				masterName := types.NamespacedName{Name: "hbasemaster", Namespace: namespace}
				if err := k8sClient.Get(ctx, masterName, existingMasterStatefulSet); err != nil {
					return oldMasterAnnotation, err
				}
				masterAnnotation, ok := existingMasterStatefulSet.Annotations["hbase-controller-revision"]
				if !ok {
					return oldMasterAnnotation, errors.New("no annotation")
				}
				return masterAnnotation, nil
			}, timeout, interval).ShouldNot(Equal(oldMasterAnnotation))

			By("By checking HBase master statefulset updated replicas")
			Eventually(func() (int, error) {
				rsName := types.NamespacedName{Name: "hbasemaster", Namespace: namespace}
				if err := k8sClient.Get(ctx, rsName, existingMasterStatefulSet); err != nil {
					return 0, err
				}
				return int(*existingMasterStatefulSet.Spec.Replicas), nil
			}, timeout, interval).Should(Equal(2))

			By("By checking HBase regionserver sts revision was updated")
			Eventually(func() (string, error) {
				rsName := types.NamespacedName{Name: "regionserver", Namespace: namespace}
				if err := k8sClient.Get(ctx, rsName, existingRsStatefulSet); err != nil {
					return oldRsAnnotation, err
				}
				rsAnnotation, ok := existingRsStatefulSet.Annotations["hbase-controller-revision"]
				if !ok {
					return oldRsAnnotation, errors.New("no annotation")
				}
				return rsAnnotation, nil
			}, timeout, interval).ShouldNot(Equal(oldRsAnnotation))

			By("By checking HBase regionserver sts updated replicas")
			Eventually(func() (int, error) {
				rsName := types.NamespacedName{Name: "regionserver", Namespace: namespace}
				if err := k8sClient.Get(ctx, rsName, existingRsStatefulSet); err != nil {
					return 0, err
				}
				return int(*existingRsStatefulSet.Spec.Replicas), nil
			}, timeout, interval).Should(Equal(3))

		})
	})
})

var _ = AfterSuite(func() {
	By("tearing down the test environment")
	err := testEnv.Stop()
	Expect(err).ToNot(HaveOccurred())
})
