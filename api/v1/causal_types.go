package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// CausalConsistencyGroupVersion is the group version used to register these objects
var CausalConsistencyGroupVersion = schema.GroupVersion{
	Group:   "causal.zugkraftdb.io",
	Version: "v1",
}

// SchemeBuilder is used to add go types to the GroupVersionKind scheme
var SchemeBuilder = runtime.NewSchemeBuilder(
	func(scheme *runtime.Scheme) error {
		scheme.AddKnownTypes(CausalConsistencyGroupVersion,
			&CausalConsistency{},
			&CausalConsistencyList{},
		)
		metav1.AddToGroupVersion(scheme, CausalConsistencyGroupVersion)
		return nil
	},
)

// CausalConsistencySpec defines the desired state of CausalConsistency
type CausalConsistencySpec struct {
	// StoreType specifies the underlying eventually consistent store (e.g., "dynamodb", "redis")
	StoreType string `json:"storeType"`
	// StoreConfig provides connection details for the store
	StoreConfig map[string]string `json:"storeConfig"`
	// MaxBufferSize limits the number of buffered writes in the shim
	MaxBufferSize int `json:"maxBufferSize,omitempty"`
	// AsyncFetchInterval defines how often to fetch missing dependencies (in seconds)
	AsyncFetchInterval int `json:"asyncFetchInterval,omitempty"`
}

// CausalConsistencyStatus defines the observed state of CausalConsistency
type CausalConsistencyStatus struct {
	// Ready indicates if the shim is operational
	Ready bool `json:"ready"`
	// LastReconciledTime tracks the last reconciliation
	LastReconciledTime *metav1.Time `json:"lastReconciledTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// CausalConsistency is the Schema for the causalconsistencies API
type CausalConsistency struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CausalConsistencySpec   `json:"spec,omitempty"`
	Status CausalConsistencyStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// CausalConsistencyList contains a list of CausalConsistency
type CausalConsistencyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CausalConsistency `json:"items"`
}

func init() {
	SchemeBuilder.Register(&CausalConsistency{}, &CausalConsistencyList{})
}
