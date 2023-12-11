package waitingpod

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"sync"
	"time"
)

type Handle interface {
	// GetWaitingPod returns a waiting pod given its UID.
	GetWaitingPod(uid types.UID) *WaitingPod
}

// WaitingPod represents a pod waiting in the permit phase.
type WaitingPod struct {
	pod            *v1.Pod
	pendingPlugins map[string]*time.Timer
	s              chan *framework.Status
	mu             sync.RWMutex
}
