package controller

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "causal-consistency-shim/api/v1"
	shim "causal-consistency-shim/internal/shim"
	store "causal-consistency-shim/internal/store/mock"
)

// CausalConsistencyReconciler reconciles a CausalConsistency object
type CausalConsistencyReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	shims  map[string]*shim.CausalShim
}

// +kubebuilder:rbac:groups=causal.consistency.shim,resources=causalconsistencies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=causal.consistency.shim,resources=causalconsistencies/status,verbs=get;update;patch

func (r *CausalConsistencyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// Fetch the CausalConsistency instance
	var cc v1.CausalConsistency
	if err := r.Get(ctx, req.NamespacedName, &cc); err != nil {
		log.Error(err, "unable to fetch CausalConsistency")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Initialize shim if not exists
	shimKey := req.NamespacedName.String()
	if _, exists := r.shims[shimKey]; !exists {
		// Initialize store (mock for simplicity; replace with real store in production)
		s := store.NewMockStore()
		fetchInterval := time.Duration(cc.Spec.AsyncFetchInterval) * time.Second
		r.shims[shimKey] = shim.NewCausalShim(s, fetchInterval)
		log.Info("Initialized new shim", "shim", shimKey)
	}

	// Update status
	cc.Status.Ready = true
	cc.Status.LastReconciledTime = &metav1.Time{Time: time.Now()}
	if err := r.Status().Update(ctx, &cc); err != nil {
		log.Error(err, "failed to update CausalConsistency status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager
func (r *CausalConsistencyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.shims = make(map[string]*shim.CausalShim)
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1.CausalConsistency{}).
		Complete(r)
}
