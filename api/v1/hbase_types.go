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

package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// HBaseSpec defines the desired state of HBase
type HBaseSpec struct {
	// MasterSpec is definition of HBase Master server
	MasterSpec ServerSpec `json:"masterSpec,omitempty"`
	// RegionServerSpec is definition of HBase RegionServer
	RegionServerSpec ServerSpec `json:"regionServerSpec,omitempty"`

	// Config is config map for HBase and Hadoop.
	// The config map will be automatically added to each pod as "config" volume.
	// Typical files are:
	// hbase-site.xml - main configuration file
	// hbase-env.sh - script to set up the working environment, including the location of Java, Java options,
	// and other environment variables.
	// log4j.properties - configuration file for logging via log4j.
	// core-site.xml - global Hadoop configuration file
	// hdfs-site.xml - HDFS specific configuration file
	// hadoop-env.sh - script to set up the working environment for hadoop, including the location of Java,
	// Java options, and other environment variables.
	Config ConfigMap `json:"config,omitempty"`
}

// ConfigMap holds configuration data for HBase
type ConfigMap struct {
	// Data where key is name of file, value is data
	Data map[string]string `json:"data,omitempty"`
}

// ServerMetadata allows to specify labels and annotations to resulting statefulsets and pods
type ServerMetadata struct {
	// Labels.
	Labels map[string]string `json:"labels,omitempty"`
	// Annotations.
	Annotations map[string]string `json:"annotations,omitempty"`
}

// ServerSpec is a specification for an HBase server (Master or Regionserver)
type ServerSpec struct {
	// PodSpec aprovides customisation options (affinity rules, resource requests, and so on).
	// +kubebuilder:validation:Optional
	PodSpec corev1.PodSpec `json:"podSpec,omitempty"`
	// Metadata provides customisation options (labels, annotations)
	Metadata ServerMetadata `json:"metadata,omitempty"`
	// Count of replicas to deploy.
	// +kubebuilder:validation:Optional
	Count int32 `json:"count,omitempty"`
}

// HBaseStatus defines the observed state of HBase
type HBaseStatus struct {
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// HBase is the Schema for the hbases API
type HBase struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   HBaseSpec   `json:"spec,omitempty"`
	Status HBaseStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// HBaseList contains a list of HBase
type HBaseList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []HBase `json:"items"`
}

func init() {
	SchemeBuilder.Register(&HBase{}, &HBaseList{})
}
