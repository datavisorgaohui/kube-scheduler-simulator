package scheduler

import (
	"context"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	utiltrace "k8s.io/utils/trace"
	"math/rand"
	"sigs.k8s.io/kube-scheduler-simulator/scheduler/queue"
	"sigs.k8s.io/kube-scheduler-simulator/scheduler/waitingpod"
)

type Scheduler struct {
	SchedulingQueue *queue.SchedulingQueue
	client          clientset.Interface

	waitingPods map[types.UID]*waitingpod.WaitingPod

	filterPlugins   []framework.FilterPlugin
	preScorePlugins []framework.PreScorePlugin
	scorePlugins    []framework.ScorePlugin
	permitPlugins   []framework.PermitPlugin
}

func New(client clientset.Interface, informerFactory informers.SharedInformerFactory) (*Scheduler, error) {
	sched := &Scheduler{
		client:      client,
		waitingPods: map[types.UID]*waitingpod.WaitingPod{},
	}
	return sched, nil
}

func (sched *Scheduler) scheduleOne(ctx context.Context) {
	pod := &v1.Pod{} // get pod from queue
	klog.Info("Scheduling", utiltrace.Field{Key: "namespace", Value: pod.Namespace}, utiltrace.Field{Key: "name", Value: pod.Name})
	// update node snapshot
	//schueule_one.go 106
	state := framework.NewCycleState()
	state.SetRecordPluginMetrics(rand.Intn(100) < 10)
	// Initialize an empty podsToActivate struct, which will be filled up by plugins or stay empty.
	podsToActivate := framework.NewPodsToActivate()
	state.Write(framework.PodsToActivateKey, podsToActivate)
	// get node list
	nodes, err := sched.client.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		klog.Error(err)
		return
	}
	klog.Info("get node list successfully")
	klog.Info("nodes: %v", nodes)

	// filter
	fasibleNodes, err := sched.RunFilterPlugins(context.Background(), state, pod, nodes.Items)
	if err != nil {
		klog.Error(err)
		return
	}
	klog.Info("feasible nodes: %v", fasibleNodes)

	// pre score
	// schedule_one.go 424
	//priorityList, err := prioritizeNodes(ctx, sched.Extenders, fwk, state, pod, feasibleNodes)
	status := sched.RunPreScorePlugins(ctx, state, pod, fasibleNodes)
	if !status.IsSuccess() {
		klog.Error(status.AsError())
		return
	}
	klog.Info("minischeduler: ran pre score plugins successfully")
	// score  schedule_one.go 714
	score, status := sched.RunScorePlugins(ctx, state, pod, fasibleNodes)
	if !status.IsSuccess() {
		klog.Error(status.AsError())
		return
	}
	klog.Info("minischeduler: ran score plugins successfully")
	klog.Info("minischeduler: score results", score)

	for _, nodeScore := range score {
		klog.Info("Plugin scored node for pod", "pod", klog.KObj(pod), "node", nodeScore.Name, "score", nodeScore.Score)
	}
	// select host
	// schedule_one.go 429
	nodename, err := sched.selectHost(score)
	klog.Info("minischeduler: pod " + pod.Name + " will be bound to node " + nodename)

	status = sched.RunPermitPlugins(ctx, state, pod, nodename)
	if status.Code() != framework.Wait && !status.IsSuccess() {
		klog.Error(status.AsError())
		return
	}

	// assume
	// assume modifies `assumedPod` by setting NodeName=scheduleResult.SuggestedHost
	// schedulr_one.go 196
	// TODO

	// run permit plugin
	// schedulr_one.go 228
	status = sched.RunPermitPlugins(ctx, state, pod, nodename)
	if status.Code() != framework.Wait && !status.IsSuccess() {
		klog.Error(status.AsError())
		return
	}

	go func() {
		ctx := ctx
		status := sched.WaitOnPermit(ctx, pod)
		if !status.IsSuccess() {
			klog.Error(status.AsError())
			return
		}

		if err := sched.Bind(ctx, nil, pod, nodename); err != nil {
			klog.Error(err)
			return
		}
		klog.Info("minischeduler: Bind Pod successfully")
	}()

}

func (sched *Scheduler) Run(ctx context.Context) {
	wait.UntilWithContext(ctx, sched.scheduleOne, 0)
}

// schedule_one.go 228
func (sched *Scheduler) RunFilterPlugins(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodes []v1.Node) ([]*v1.Node, error) {
	feasibleNodes := make([]*v1.Node, 0, len(nodes))
	diagnosis := framework.Diagnosis{
		NodeToStatusMap: make(framework.NodeToStatusMap),
	}
	for _, n := range nodes {
		n := n
		nodeInfo := framework.NewNodeInfo()
		nodeInfo.SetNode(&n)

		status := framework.NewStatus(framework.Success)
		for _, pl := range sched.filterPlugins {
			// schedule_one.go 562
			// ~/k8s.io/kubernetes/pkg/scheduler/framework/runtime/framework.go 839
			status = pl.Filter(ctx, state, pod, nodeInfo)
			if !status.IsSuccess() {
				status.SetFailedPlugin(pl.Name())
				diagnosis.UnschedulablePlugins.Insert(status.FailedPlugin())
				break
			}
		}
		if status.IsSuccess() {
			feasibleNodes = append(feasibleNodes, nodeInfo.Node())
		}
	}
	return nil, nil
}
func (sched *Scheduler) RunPreScorePlugins(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodes []*v1.Node) *framework.Status {
	return &framework.Status{}
}

func (sched *Scheduler) RunScorePlugins(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodes []*v1.Node) (framework.NodeScoreList, *framework.Status) {
	return nil, nil
}

func (sched *Scheduler) RunPermitPlugins(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (status *framework.Status) {
	return nil
}

func (sched *Scheduler) Bind(ctx context.Context, state *framework.CycleState, p *v1.Pod, nodeName string) error {
	return nil
}

func (sched *Scheduler) selectHost(nodeScoreList framework.NodeScoreList) (string, error) {
	return "", nil
}

func (sched *Scheduler) WaitOnPermit(ctx context.Context, pod *v1.Pod) *framework.Status {
	return nil
}

func (sched *Scheduler) printItem() string {
	return ""
}
