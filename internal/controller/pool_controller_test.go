package controller

import (
	"context"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"durarun-operator/api/v1alpha1"
)

func newPool(name, ns string) *v1alpha1.SandboxPool {
	return &v1alpha1.SandboxPool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			UID:       types.UID("pool-uid-1"),
		},
		Spec: v1alpha1.SandboxPoolSpec{
			MinSize:            2,
			MaxSize:            10,
			ScaleUpThreshold:   0.3,
			ScaleDownThreshold: 0.8,
			ScaleUpStep:        2,
			CooldownSeconds:    0, // no cooldown for tests
			Template: v1alpha1.PoolPodTemplate{
				Image: "sandbox:latest",
				Resources: v1alpha1.ResourceSpec{
					CPULimit:    "1",
					MemoryLimit: "512Mi",
				},
			},
		},
	}
}

func makePoolPod(name, ns, poolName, state string, phase corev1.PodPhase) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels: map[string]string{
				"durarun.io/pool":       poolName,
				"durarun.io/pool-state": state,
				"durarun.io/managed-by": "pool-controller",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "agent",
				Image: "sandbox:latest",
			}},
		},
		Status: corev1.PodStatus{
			Phase: phase,
		},
	}
}

// TestPoolReconcile_ScaleUp verifies that reconciling a SandboxPool with
// minSize=2 and no existing pods creates 2 pods.
func TestPoolReconcile_ScaleUp(t *testing.T) {
	scheme := buildScheme()
	pool := newPool("test-pool", "default")

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool).
		WithStatusSubresource(&v1alpha1.SandboxPool{}).
		Build()

	r := &PoolReconciler{
		Client: cl,
		Scheme: scheme,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "test-pool", Namespace: "default"}}
	_, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Reconcile returned error: %v", err)
	}

	// Verify pods were created.
	var podList corev1.PodList
	if err := cl.List(context.Background(), &podList,
		client.InNamespace("default"),
		client.MatchingLabels{"durarun.io/pool": "test-pool"},
	); err != nil {
		t.Fatalf("failed to list pods: %v", err)
	}

	if len(podList.Items) != 2 {
		t.Errorf("expected 2 pods created, got %d", len(podList.Items))
	}

	for _, pod := range podList.Items {
		if pod.Labels["durarun.io/pool-state"] != "idle" {
			t.Errorf("expected pod state 'idle', got %q", pod.Labels["durarun.io/pool-state"])
		}
		if pod.Labels["durarun.io/managed-by"] != "pool-controller" {
			t.Errorf("expected managed-by 'pool-controller', got %q", pod.Labels["durarun.io/managed-by"])
		}
		if pod.Spec.Containers[0].Image != "sandbox:latest" {
			t.Errorf("expected image 'sandbox:latest', got %q", pod.Spec.Containers[0].Image)
		}
		// No command specified, should default to sleep infinity.
		if len(pod.Spec.Containers[0].Command) != 2 ||
			pod.Spec.Containers[0].Command[0] != "sleep" ||
			pod.Spec.Containers[0].Command[1] != "infinity" {
			t.Errorf("expected command [sleep infinity], got %v", pod.Spec.Containers[0].Command)
		}
	}
}

// TestPoolReconcile_ScaleDown verifies that reconciling a pool with too many
// idle pods deletes excess pods down to minSize.
func TestPoolReconcile_ScaleDown(t *testing.T) {
	scheme := buildScheme()
	pool := newPool("sd-pool", "default")
	pool.Spec.MinSize = 1
	pool.Spec.MaxSize = 5
	pool.Spec.ScaleDownThreshold = 0.8

	// Pre-create 5 idle running pods.
	pods := make([]client.Object, 5)
	for i := 0; i < 5; i++ {
		pods[i] = makePoolPod(
			fmt.Sprintf("sd-pool-pod-%d", i),
			"default", "sd-pool", "idle", corev1.PodRunning,
		)
	}

	objs := append([]client.Object{pool}, pods...)
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&v1alpha1.SandboxPool{}).
		Build()

	r := &PoolReconciler{
		Client: cl,
		Scheme: scheme,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "sd-pool", Namespace: "default"}}
	_, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Reconcile returned error: %v", err)
	}

	// After reconcile, the remaining pods should equal minSize.
	var podList corev1.PodList
	if err := cl.List(context.Background(), &podList,
		client.InNamespace("default"),
		client.MatchingLabels{"durarun.io/pool": "sd-pool"},
	); err != nil {
		t.Fatalf("failed to list pods: %v", err)
	}

	if len(podList.Items) != pool.Spec.MinSize {
		t.Errorf("expected %d pods after scale down, got %d", pool.Spec.MinSize, len(podList.Items))
	}
}

// TestPoolReconcile_StatusUpdate verifies that after reconcile, the SandboxPool
// status reflects actual pod counts.
func TestPoolReconcile_StatusUpdate(t *testing.T) {
	scheme := buildScheme()
	pool := newPool("status-pool", "default")
	pool.Spec.MinSize = 0
	pool.Spec.MaxSize = 10
	pool.Spec.ScaleUpThreshold = 0.0 // never triggers scale up

	// Pre-create a mix of idle and claimed pods.
	objs := []client.Object{
		pool,
		makePoolPod("status-pool-1", "default", "status-pool", "idle", corev1.PodRunning),
		makePoolPod("status-pool-2", "default", "status-pool", "idle", corev1.PodRunning),
		makePoolPod("status-pool-3", "default", "status-pool", "claimed", corev1.PodRunning),
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&v1alpha1.SandboxPool{}).
		Build()

	r := &PoolReconciler{
		Client: cl,
		Scheme: scheme,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "status-pool", Namespace: "default"}}
	_, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Reconcile returned error: %v", err)
	}

	// Verify status fields.
	var updatedPool v1alpha1.SandboxPool
	if err := cl.Get(context.Background(), req.NamespacedName, &updatedPool); err != nil {
		t.Fatalf("failed to get updated pool: %v", err)
	}

	if updatedPool.Status.TotalPods != 3 {
		t.Errorf("expected TotalPods=3, got %d", updatedPool.Status.TotalPods)
	}
	if updatedPool.Status.IdlePods != 2 {
		t.Errorf("expected IdlePods=2, got %d", updatedPool.Status.IdlePods)
	}
	if updatedPool.Status.ClaimedPods != 1 {
		t.Errorf("expected ClaimedPods=1, got %d", updatedPool.Status.ClaimedPods)
	}
	if !updatedPool.Status.Ready {
		t.Error("expected Ready=true")
	}
}

// TestClaimPoolPod verifies that ClaimPoolPod finds an idle pod and patches
// its labels to "claimed".
func TestClaimPoolPod(t *testing.T) {
	scheme := buildScheme()

	pods := []client.Object{
		makePoolPod("claim-pool-1", "default", "claim-pool", "idle", corev1.PodRunning),
		makePoolPod("claim-pool-2", "default", "claim-pool", "idle", corev1.PodRunning),
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pods...).
		Build()

	ctx := context.Background()
	pod, err := ClaimPoolPod(ctx, cl, "claim-pool", "default", "my-job")
	if err != nil {
		t.Fatalf("ClaimPoolPod returned error: %v", err)
	}
	if pod == nil {
		t.Fatal("expected a pod to be claimed, got nil")
	}

	// Verify the claimed pod's labels were updated.
	if pod.Labels["durarun.io/pool-state"] != "claimed" {
		t.Errorf("expected pool-state 'claimed', got %q", pod.Labels["durarun.io/pool-state"])
	}
	if pod.Labels["durarun.io/job"] != "my-job" {
		t.Errorf("expected job label 'my-job', got %q", pod.Labels["durarun.io/job"])
	}

	// Re-fetch the pod from the fake client and verify persistence.
	var fetched corev1.Pod
	if err := cl.Get(ctx, types.NamespacedName{Name: pod.Name, Namespace: "default"}, &fetched); err != nil {
		t.Fatalf("failed to get claimed pod: %v", err)
	}
	if fetched.Labels["durarun.io/pool-state"] != "claimed" {
		t.Errorf("persisted pod state: expected 'claimed', got %q", fetched.Labels["durarun.io/pool-state"])
	}
}

// TestClaimPoolPod_NoIdlePods verifies that ClaimPoolPod returns nil when
// no idle pods are available.
func TestClaimPoolPod_NoIdlePods(t *testing.T) {
	scheme := buildScheme()

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		Build()

	ctx := context.Background()
	pod, err := ClaimPoolPod(ctx, cl, "empty-pool", "default", "my-job")
	if err != nil {
		t.Fatalf("ClaimPoolPod returned error: %v", err)
	}
	if pod != nil {
		t.Errorf("expected nil pod when no idle pods, got %s", pod.Name)
	}
}

// TestPoolReconcile_NotFound verifies that reconciling a deleted pool returns
// without error.
func TestPoolReconcile_NotFound(t *testing.T) {
	scheme := buildScheme()
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		Build()

	r := &PoolReconciler{
		Client: cl,
		Scheme: scheme,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "gone", Namespace: "default"}}
	result, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Reconcile returned error for deleted pool: %v", err)
	}
	if result.Requeue {
		t.Error("expected no immediate requeue for deleted pool")
	}
}
