package scheduler

import (
	"context"
	"fmt"
	"github.com/sanposhiho/mini-kube-scheduler/minisched/plugins/score/nodenumber"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodeunschedulable"
	utiltrace "k8s.io/utils/trace"
	"math/rand"
	"sigs.k8s.io/kube-scheduler-simulator/scheduler/queue"
	"sigs.k8s.io/kube-scheduler-simulator/scheduler/waitingpod"
	"time"
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
	filterP, err := createFilterPlugins(sched)
	if err != nil {
		return nil, fmt.Errorf("create filter plugins: %w", err)
	}
	sched.filterPlugins = filterP

	preScoreP, err := createPreScorePlugins(sched)
	if err != nil {
		return nil, fmt.Errorf("create pre score plugins: %w", err)
	}
	sched.preScorePlugins = preScoreP

	scoreP, err := createScorePlugins(sched)
	if err != nil {
		return nil, fmt.Errorf("create score plugins: %w", err)
	}
	sched.scorePlugins = scoreP

	permitP, err := createPermitPlugins(sched)
	if err != nil {
		return nil, fmt.Errorf("create permit plugins: %w", err)
	}
	sched.permitPlugins = permitP

	events, err := eventsToRegister(sched)
	if err != nil {
		return nil, fmt.Errorf("create gvks: %w")
	}

	sched.SchedulingQueue = queue.New(events)

	addAllEventHandlers(sched, informerFactory, unionedGVKs(events))

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
			// it will run
			// 1. CSILimiter
			// 2. Fit (if node has sufficient resources)
			// 3. InterPodAffinity (checks inter pod affinity)
			// 4. MatchFilterPlugin (is a filter plugin which return Success when the evaluated pod and node
			//    have the same name; otherwise return Unschedulable.)
			// 5. nodeAffinity (checks if a pod node selector matches the node label)
			// 6. node_name (checks if a pod spec node name matches the current node)
			// 7. node_ports (checks if a node has free ports for the requested pod ports)
			// 8. node_Unschedulable (filters nodes that set node.Spec.Unschedulable=true unless
			//    the pod tolerates {key=node.kubernetes.io/unschedulable, effect:NoSchedule} taint.)
			// 9. PodTopologySpread (ensures pod's topologySpreadConstraints拓扑扩展约束 is satisfied)
			// 10. TaintToleration (checks if a pod tolerates a node's taints)
			// 11. VolumeBinding (created for the pod and used in Reserve and PreBind phases)
			// 12. VolumeRestrictions (checks volume restrictions限制)
			// 13. VolumeZone (checks volume zone)
			// 14. dynamicResources (ensures that ResourceClaims are allocated)
			// 15. instrumentedFilterPlugin
			// 16. nonCSILimits contains information to check the max number of volumes for a plugin
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
	return feasibleNodes, nil
}

func (sched *Scheduler) RunPreScorePlugins(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodes []*v1.Node) *framework.Status {
	for _, pl := range sched.preScorePlugins {
		// ~/k8s.io/kubernetes/pkg/scheduler/framework/runtime/framework.go 1030
		status := pl.PreScore(ctx, state, pod, nodes)
		if !status.IsSuccess() {
			return status
		}
	}
	return nil
}

func (sched *Scheduler) RunScorePlugins(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodes []*v1.Node) (framework.NodeScoreList, *framework.Status) {
	scoresMap := sched.createPluginToNodeScores(nodes)

	for index, n := range nodes {
		for _, pl := range sched.scorePlugins {
			// ~/k8s.io/kubernetes/pkg/scheduler/framework/runtime/framework.go 1052
			score, status := pl.Score(ctx, state, pod, n.Name)
			if !status.IsSuccess() {
				return nil, status
			}
			scoresMap[pl.Name()][index] = framework.NodeScore{
				Name:  n.Name,
				Score: score,
			}

			if pl.ScoreExtensions() != nil {
				status := pl.ScoreExtensions().NormalizeScore(ctx, state, pod, scoresMap[pl.Name()])
				if !status.IsSuccess() {
					return nil, status
				}
			}
		}
	}

	// TODO: plugin weight

	result := make(framework.NodeScoreList, 0, len(nodes))

	for i := range nodes {
		result = append(result, framework.NodeScore{Name: nodes[i].Name, Score: 0})
		for j := range scoresMap {
			result[i].Score += scoresMap[j][i].Score
		}
	}
	return result, nil
}

func (sched *Scheduler) RunPermitPlugins(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (status *framework.Status) {
	pluginsWaitTime := make(map[string]time.Duration)
	statusCode := framework.Success
	for _, pl := range sched.permitPlugins {
		// ~/k8s.io/kubernetes/pkg/scheduler/framework/runtime/framework.go 1435
		status, timeout := pl.Permit(ctx, state, pod, nodeName)
		if !status.IsSuccess() {
			// reject
			if status.IsUnschedulable() {
				klog.InfoS("Pod rejected by permit plugin", "pod", klog.KObj(pod), "plugin", pl.Name(), "status", status.Message())
				status.SetFailedPlugin(pl.Name())
				return status
			}

			// wait
			if status.Code() == framework.Wait {
				pluginsWaitTime[pl.Name()] = timeout
				statusCode = framework.Wait
				continue
			}

			// other errors
			err := status.AsError()
			klog.ErrorS(err, "Failed running Permit plugin", "plugin", pl.Name(), "pod", klog.KObj(pod))
			return framework.AsStatus(fmt.Errorf("running Permit plugin %q: %w", pl.Name(), err)).WithFailedPlugin(pl.Name())
		}
	}

	if statusCode == framework.Wait {
		waitingPod := waitingpod.NewWaitingPod(pod, pluginsWaitTime)
		sched.waitingPods[pod.UID] = waitingPod
		msg := fmt.Sprintf("one or more plugins asked to wait and no plugin rejected pod %q", pod.Name)
		klog.InfoS("One or more plugins asked to wait and no plugin rejected pod", "pod", klog.KObj(pod))
		return framework.NewStatus(framework.Wait, msg)
	}

	return nil
}

// schedule_one.go 905
func (sched *Scheduler) Bind(ctx context.Context, state *framework.CycleState, p *v1.Pod, nodeName string) error {
	binding := &v1.Binding{
		ObjectMeta: metav1.ObjectMeta{Namespace: p.Namespace, Name: p.Name, UID: p.UID},
		Target:     v1.ObjectReference{Kind: "Node", Name: nodeName},
	}

	err := sched.client.CoreV1().Pods(binding.Namespace).Bind(ctx, binding, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	return nil
}

// schedule_one.go 810
func (sched *Scheduler) selectHost(nodeScoreList framework.NodeScoreList) (string, error) {
	if len(nodeScoreList) == 0 {
		return "", fmt.Errorf("empty priorityList")
	}
	maxScore := nodeScoreList[0].Score
	selected := nodeScoreList[0].Name
	cntOfMaxScore := 1
	for _, ns := range nodeScoreList[1:] {
		if ns.Score > maxScore {
			maxScore = ns.Score
			selected = ns.Name
			cntOfMaxScore = 1
		} else if ns.Score == maxScore {
			cntOfMaxScore++
			if rand.Intn(cntOfMaxScore) == 0 {
				// Replace the candidate with probability of 1/cntOfMaxScore
				selected = ns.Name
			}
		}
	}
	return selected, nil
}

// ~/k8s.io/kubernetes/pkg/scheduler/framework/runtime/framework.go 1441
func (sched *Scheduler) WaitOnPermit(ctx context.Context, pod *v1.Pod) *framework.Status {
	waitingPod := sched.waitingPods[pod.UID]
	if waitingPod == nil {
		return nil
	}
	defer delete(sched.waitingPods, pod.UID)

	klog.InfoS("Pod waiting on permit", "pod", klog.KObj(pod))

	s := waitingPod.GetSignal()

	if !s.IsSuccess() {
		if s.IsUnschedulable() {
			klog.InfoS("Pod rejected while waiting on permit", "pod", klog.KObj(pod), "status", s.Message())

			s.SetFailedPlugin(s.FailedPlugin())
			return s
		}

		err := s.AsError()
		klog.ErrorS(err, "Failed waiting on permit for pod", "pod", klog.KObj(pod))
		return framework.AsStatus(fmt.Errorf("waiting on permit for pod: %w", err)).WithFailedPlugin(s.FailedPlugin())
	}
	return nil
}

func (sched *Scheduler) createPluginToNodeScores(nodes []*v1.Node) framework.PluginToNodeScores {
	pluginToNodeScores := make(framework.PluginToNodeScores, len(sched.scorePlugins))
	for _, pl := range sched.scorePlugins {
		pluginToNodeScores[pl.Name()] = make(framework.NodeScoreList, len(nodes))
	}

	return pluginToNodeScores
}

func createFilterPlugins(h waitingpod.Handle) ([]framework.FilterPlugin, error) {
	// nodeunschedulable is FilterPlugin.
	nodeunschedulableplugin, err := createNodeUnschedulablePlugin()
	if err != nil {
		return nil, fmt.Errorf("create nodeunschedulable plugin: %w", err)
	}

	// We use nodeunschedulable plugin only.
	filterPlugins := []framework.FilterPlugin{
		nodeunschedulableplugin.(framework.FilterPlugin),
	}

	return filterPlugins, nil
}

func createPreScorePlugins(h waitingpod.Handle) ([]framework.PreScorePlugin, error) {
	// nodenumber is FilterPlugin.
	nodenumberplugin, err := createNodeNumberPlugin(h)
	if err != nil {
		return nil, fmt.Errorf("create nodenumber plugin: %w", err)
	}

	// We use nodenumber plugin only.
	preScorePlugins := []framework.PreScorePlugin{
		nodenumberplugin.(framework.PreScorePlugin),
	}

	return preScorePlugins, nil
}

func createScorePlugins(h waitingpod.Handle) ([]framework.ScorePlugin, error) {
	// nodenumber is FilterPlugin.
	nodenumberplugin, err := createNodeNumberPlugin(h)
	if err != nil {
		return nil, fmt.Errorf("create nodenumber plugin: %w", err)
	}

	// We use nodenumber plugin only.
	filterPlugins := []framework.ScorePlugin{
		nodenumberplugin.(framework.ScorePlugin),
	}

	return filterPlugins, nil
}

func createPermitPlugins(h waitingpod.Handle) ([]framework.PermitPlugin, error) {
	// nodenumber is PermitPlugin.
	nodenumberplugin, err := createNodeNumberPlugin(h)
	if err != nil {
		return nil, fmt.Errorf("create nodenumber plugin: %w", err)
	}

	// We use nodenumber plugin only.
	permitPlugins := []framework.PermitPlugin{
		nodenumberplugin.(framework.PermitPlugin),
	}

	return permitPlugins, nil
}

var (
	nodeunschedulableplugin framework.Plugin
	nodenumberplugin        framework.Plugin
)

func createNodeUnschedulablePlugin() (framework.Plugin, error) {
	if nodeunschedulableplugin != nil {
		return nodeunschedulableplugin, nil
	}

	p, err := nodeunschedulable.New(nil, nil)
	nodeunschedulableplugin = p

	return p, err
}

func createNodeNumberPlugin(h waitingpod.Handle) (framework.Plugin, error) {
	if nodenumberplugin != nil {
		return nodenumberplugin, nil
	}

	p, err := nodenumber.New(nil, h)
	nodenumberplugin = p

	return p, err
}

func eventsToRegister(h waitingpod.Handle) (map[framework.ClusterEvent]sets.String, error) {
	nunschedulablePlugin, err := createNodeUnschedulablePlugin()
	if err != nil {
		return nil, fmt.Errorf("create node unschedulable plugin: %w", err)
	}
	nnumberPlugin, err := createNodeNumberPlugin(h)
	if err != nil {
		return nil, fmt.Errorf("create node number plugin: %w", err)
	}

	clusterEventMap := make(map[framework.ClusterEvent]sets.String)
	nunschedulablePluginEvents := nunschedulablePlugin.(framework.EnqueueExtensions).EventsToRegister()
	registerClusterEvents(nunschedulablePlugin.Name(), clusterEventMap, nunschedulablePluginEvents)
	nnumberPluginEvents := nnumberPlugin.(framework.EnqueueExtensions).EventsToRegister()
	registerClusterEvents(nunschedulablePlugin.Name(), clusterEventMap, nnumberPluginEvents)

	return clusterEventMap, nil
}

func registerClusterEvents(name string, eventToPlugins map[framework.ClusterEvent]sets.String, evts []framework.ClusterEvent) {
	for _, evt := range evts {
		if eventToPlugins[evt] == nil {
			eventToPlugins[evt] = sets.NewString(name)
		} else {
			eventToPlugins[evt].Insert(name)
		}
	}
}

func unionedGVKs(m map[framework.ClusterEvent]sets.String) map[framework.GVK]framework.ActionType {
	gvkMap := make(map[framework.GVK]framework.ActionType)
	for evt := range m {
		if _, ok := gvkMap[evt.Resource]; ok {
			gvkMap[evt.Resource] |= evt.ActionType
		} else {
			gvkMap[evt.Resource] = evt.ActionType
		}
	}
	return gvkMap
}

func addAllEventHandlers(sched *Scheduler, informerFactory informers.SharedInformerFactory, gvkMap map[framework.GVK]framework.ActionType) {
	// unscheduled pod
	informerFactory.Core().V1().Pods().Informer().AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				switch t := obj.(type) {
				case *v1.Pod:
					return !assignedPod(t)
				default:
					return false
				}
			},
			Handler: cache.ResourceEventHandlerFuncs{
				// Consider only adding.
				AddFunc: sched.addPodToSchedulingQueue,
			},
		},
	)

	buildEvtResHandler := func(at framework.ActionType, gvk framework.GVK, shortGVK string) cache.ResourceEventHandlerFuncs {
		funcs := cache.ResourceEventHandlerFuncs{}
		if at&framework.Add != 0 {
			evt := framework.ClusterEvent{Resource: gvk, ActionType: framework.Add, Label: fmt.Sprintf("%vAdd", shortGVK)}
			funcs.AddFunc = func(_ interface{}) {
				sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(evt)
			}
		}
		if at&framework.Update != 0 {
			evt := framework.ClusterEvent{Resource: gvk, ActionType: framework.Update, Label: fmt.Sprintf("%vUpdate", shortGVK)}
			funcs.UpdateFunc = func(_, _ interface{}) {
				sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(evt)
			}
		}
		if at&framework.Delete != 0 {
			evt := framework.ClusterEvent{Resource: gvk, ActionType: framework.Delete, Label: fmt.Sprintf("%vDelete", shortGVK)}
			funcs.DeleteFunc = func(_ interface{}) {
				sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(evt)
			}
		}
		return funcs
	}

	for gvk, at := range gvkMap {
		switch gvk {
		case framework.Node:
			informerFactory.Core().V1().Nodes().Informer().AddEventHandler(
				buildEvtResHandler(at, framework.Node, "Node"),
			)
			//case framework.CSINode:
			//case framework.CSIDriver:
			//case framework.CSIStorageCapacity:
			//case framework.PersistentVolume:
			//case framework.PersistentVolumeClaim:
			//case framework.StorageClass:
			//case framework.Service:
			//default:
		}

	}
}

// assignedPod selects pods that are assigned (scheduled and running).
func assignedPod(pod *v1.Pod) bool {
	return len(pod.Spec.NodeName) != 0
}

func (sched *Scheduler) addPodToSchedulingQueue(obj interface{}) {
	pod := obj.(*v1.Pod)

	if err := sched.SchedulingQueue.Add(pod); err != nil {
		utilruntime.HandleError(fmt.Errorf("unable to queue %T: %v", obj, err))
	}
}

func (sched *Scheduler) GetWaitingPod(uid types.UID) *waitingpod.WaitingPod {
	return sched.waitingPods[uid]
}
