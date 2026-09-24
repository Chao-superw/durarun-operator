package executor

import (
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"durarun-operator/api/v1alpha1"
)

// BuildNetworkPolicy creates a NetworkPolicy that default-denies all egress
// for pods belonging to the given AgentJob, then selectively allows DNS
// resolution and egress to the artifact gateway service.
func BuildNetworkPolicy(job *v1alpha1.AgentJob, gatewayServiceName string) *networkingv1.NetworkPolicy {
	dnsPort := intstr.FromInt32(53)
	protocolUDP := corev1.ProtocolUDP
	protocolTCP := corev1.ProtocolTCP

	return &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      job.Name + "-egress",
			Namespace: job.Namespace,
			Labels: map[string]string{
				"durarun.io/job": job.Name,
			},
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{
				MatchLabels: map[string]string{
					"durarun.io/job": job.Name,
				},
			},
			PolicyTypes: []networkingv1.PolicyType{
				networkingv1.PolicyTypeEgress,
			},
			Egress: []networkingv1.NetworkPolicyEgressRule{
				// Allow DNS (UDP + TCP port 53) to kube-dns.
				{
					Ports: []networkingv1.NetworkPolicyPort{
						{
							Protocol: &protocolUDP,
							Port:     &dnsPort,
						},
						{
							Protocol: &protocolTCP,
							Port:     &dnsPort,
						},
					},
					To: []networkingv1.NetworkPolicyPeer{
						{
							// kube-dns typically runs in kube-system with label k8s-app=kube-dns
							NamespaceSelector: &metav1.LabelSelector{},
							PodSelector: &metav1.LabelSelector{
								MatchLabels: map[string]string{
									"k8s-app": "kube-dns",
								},
							},
						},
					},
				},
				// Allow egress to the artifact gateway service.
				{
					To: []networkingv1.NetworkPolicyPeer{
						{
							PodSelector: &metav1.LabelSelector{
								MatchLabels: map[string]string{
									"app": gatewayServiceName,
								},
							},
						},
					},
				},
			},
		},
	}
}
