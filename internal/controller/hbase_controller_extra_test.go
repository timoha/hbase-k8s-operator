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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestOrderPodList(t *testing.T) {
	createPodListByName := func(names []string) *corev1.PodList {
		pl := &corev1.PodList{}
		for _, name := range names {
			pl.Items = append(pl.Items, corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: name,
				},
			})
		}
		return pl
	}

	tests := []struct {
		given  *corev1.PodList
		expect *corev1.PodList
	}{
		// Empty list
		{
			given:  createPodListByName([]string{}),
			expect: createPodListByName([]string{}),
		},
		// Single pod list
		{
			given:  createPodListByName([]string{"regionserver-0"}),
			expect: createPodListByName([]string{"regionserver-0"}),
		},
		// Two pods
		// (We don't test for two pods with the same name since that would normally
		// never exist within a statefulset podlist)
		{
			given:  createPodListByName([]string{"regionserver-0", "regionserver-1"}),
			expect: createPodListByName([]string{"regionserver-1", "regionserver-0"}),
		},
		// Multiple pods
		{
			given:  createPodListByName([]string{"regionserver-1", "regionserver-10", "regionserver-11"}),
			expect: createPodListByName([]string{"regionserver-11", "regionserver-10", "regionserver-1"}),
		},
		// Multiple pods
		{
			given:  createPodListByName([]string{"regionserver-0", "regionserver-1", "regionserver-2", "regionserver-3", "regionserver-9", "regionserver-10", "regionserver-11", "regionserver-12"}),
			expect: createPodListByName([]string{"regionserver-12", "regionserver-11", "regionserver-10", "regionserver-9", "regionserver-3", "regionserver-2", "regionserver-1", "regionserver-0"}),
		},
	}

	equalPodListByName := func(pl1 *corev1.PodList, pl2 *corev1.PodList) bool {
		if (pl1.Items == nil) == (pl2.Items == nil) {
			return true
		}

		if len(pl1.Items) != len(pl2.Items) {
			return false
		}
		for i, _ := range pl1.Items {
			if pl1.Items[i].Name != pl2.Items[i].Name {
				return false
			}
		}
		return true
	}

	for _, test := range tests {
		orderPodListByName(test.given)
		if !equalPodListByName(test.given, test.expect) {
			t.Error()
		}
	}
}
