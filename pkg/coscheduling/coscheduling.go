/*
Copyright 2020 The Kubernetes Authors.

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

package coscheduling

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	framework "k8s.io/kubernetes/pkg/scheduler/framework/v1alpha1"

	"sigs.k8s.io/scheduler-plugins/pkg/apis/config"
	"sigs.k8s.io/scheduler-plugins/pkg/coscheduling/core"
	pgclientset "sigs.k8s.io/scheduler-plugins/pkg/generated/clientset/versioned"
	pgformers "sigs.k8s.io/scheduler-plugins/pkg/generated/informers/externalversions"
	"sigs.k8s.io/scheduler-plugins/pkg/util"
)

// Coscheduling is a plugin that schedules pods in a group.
type Coscheduling struct {
	frameworkHandler framework.FrameworkHandle
	pgMgr            core.Manager
	scheduleTimeout  *time.Duration
	jobCreationTimestamps *map[string]metav1.Time
	jobTypes *map[string]map[string]bool
}

var _ framework.QueueSortPlugin = &Coscheduling{}
var _ = framework.ScorePlugin(&Coscheduling{})
var _ = framework.FilterPlugin(&Coscheduling{})

const (
	// Name is the name of the plugin used in Registry and configurations.
	Name      = "Coscheduling"
	MAX_SCORE = 100
	MIDDLE_SCORE = 50
	FIRST_PERCENTILE_SCORE = 25
	THIRD_PERCENTILE_SCORE = 75
	MIN_SCORE = 0
)

// New initializes and returns a new Coscheduling plugin.
func New(obj runtime.Object, handle framework.FrameworkHandle) (framework.Plugin, error) {
	args, ok := obj.(*config.CoschedulingArgs)
	if !ok {
		return nil, fmt.Errorf("want args to be of type CoschedulingArgs, got %T", obj)
	}

	conf, err := clientcmd.BuildConfigFromFlags(args.KubeMaster, args.KubeConfigPath)
	if err != nil {
		return nil, fmt.Errorf("failed to init rest.Config: %v", err)
	}
	pgClient := pgclientset.NewForConfigOrDie(conf)
	pgInformerFactory := pgformers.NewSharedInformerFactory(pgClient, 0)
	pgInformer := pgInformerFactory.Scheduling().V1alpha1().PodGroups()

	fieldSelector, err := fields.ParseSelector(",status.phase!=" + string(v1.PodSucceeded) + ",status.phase!=" + string(v1.PodFailed))
	if err != nil {
		klog.Fatalf("ParseSelector failed %+v", err)
	}
	informerFactory := informers.NewSharedInformerFactoryWithOptions(handle.ClientSet(), 0, informers.WithTweakListOptions(func(opt *metav1.ListOptions) {
		opt.LabelSelector = util.PodGroupLabel
		opt.FieldSelector = fieldSelector.String()
	}))
	podInformer := informerFactory.Core().V1().Pods()

	scheduleTimeDuration := time.Duration(args.PermitWaitingTimeSeconds) * time.Second
	deniedPGExpirationTime := time.Duration(args.DeniedPGExpirationTimeSeconds) * time.Second

	jobCreationTimeStamps := make(map[string]metav1.Time, 0)
	jobTypes := make(map[string]map[string]bool, 0)

	ctx := context.TODO()

	pgMgr := core.NewPodGroupManager(pgClient, handle.SnapshotSharedLister(), &scheduleTimeDuration, &deniedPGExpirationTime, pgInformer, podInformer)
	plugin := &Coscheduling{
		frameworkHandler: handle,
		pgMgr:            pgMgr,
		scheduleTimeout:  &scheduleTimeDuration,
		jobCreationTimestamps: &jobCreationTimeStamps,
		jobTypes: &jobTypes,
	}
	pgInformerFactory.Start(ctx.Done())
	informerFactory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), pgInformer.Informer().HasSynced, podInformer.Informer().HasSynced) {
		klog.Error("Cannot sync caches")
		return nil, fmt.Errorf("WaitForCacheSync failed")
	}
	return plugin, nil
}

// Name returns name of the plugin. It is used in logs, etc.
func (cs *Coscheduling) Name() string {
	return Name
}

// Less is used to sort pods in the scheduling queue in the following order.
// 1. Compare the priorities of Pods.
// 2. Compare the initialization timestamps of jobIds.
// 3. Compare the keys of PodGroups/Pods: <namespace>/<podname>.
func (cs *Coscheduling) Less(podInfo1, podInfo2 *framework.QueuedPodInfo) bool {
	klog.Infof("Coscheduling less called")
	klog.Flush()
	podId1 := getPodId(podInfo1.Pod.Name)
	podId2 := getPodId(podInfo2.Pod.Name)

	cs.addJobTimeStampToMap(podInfo1.Pod.Name, podInfo1.Pod.CreationTimestamp)
	cs.addJobTimeStampToMap(podInfo2.Pod.Name, podInfo2.Pod.CreationTimestamp)
	prio1 := podutil.GetPodPriority(podInfo1.Pod)
	prio2 := podutil.GetPodPriority(podInfo2.Pod)
	if prio1 != prio2 {
		return prio1 > prio2
	}
	creationTime1 := (*cs.jobCreationTimestamps)[podId1]
	creationTime2 := (*cs.jobCreationTimestamps)[podId2]
	if creationTime1.Equal(&creationTime2) {
		return GetNamespacedName(podInfo1.Pod) < GetNamespacedName(podInfo2.Pod)
	}
	return creationTime1.Before(&creationTime2)
}

func GetNamespacedName(obj metav1.Object) string {
	return fmt.Sprintf("%v/%v", obj.GetNamespace(), obj.GetName())
}


func (cs *Coscheduling) addJobTypeToJobTypeMap(podName string) {
	podId := getPodId(podName)
	podType, err := getPodType(podName)
	if err == nil {
		(*cs.jobTypes)[podId][podType] = true
	}
}

func getPodType(name string) (string, error){
	if strings.Contains(name, "worker") {
		return "worker", nil
	}
	if strings.Contains(name, "server") {
		return "server", nil
	}
	if strings.Contains(name, "scheduler") {
		return "scheduler", nil
	}
	return "invalid", errors.New("not DL pod")
}

func (cs *Coscheduling) addJobTimeStampToMap(podName string, timeStamp metav1.Time) {
	podId := getPodId(podName)

	// podId exists
	if prevTimeStamp, ok := (*cs.jobCreationTimestamps)[podId]; ok {

		// previous time stamp was after the received time stamp
		if timeStamp.Before(&prevTimeStamp) {
			(*cs.jobCreationTimestamps)[podId] = timeStamp
		}
	} else {
		(*cs.jobCreationTimestamps)[podId] = timeStamp
	}
}

func (cs *Coscheduling) NormalizeScore(ctx context.Context, state *framework.CycleState, p *v1.Pod, scores framework.NodeScoreList) *framework.Status {
	klog.Infof("Coscheduling normalize score called")
	for i := range scores {
		scores[i].Score /= MAX_SCORE
	}
	return nil
}

func (cs *Coscheduling) Score(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	podName := pod.Name

	klog.Infof("Coscheduling score called")
	klog.Flush()

	// Ignore non-DL pods (scheduler, controllers etc..)
	if !isDLPod(podName) {
		return MAX_SCORE, framework.NewStatus(framework.Success, "")
	}
	//
	nodePods := cs.pgMgr.GetPodsHostedOnNode(nodeName)
	//
	for _, nodePod := range nodePods {
		nodePodName := nodePod.Pod.Name
		// only schedule pods with same id together
		if isDLPod(nodePodName) && getPodId(podName) != getPodId(nodePodName) {
			return MIN_SCORE, framework.NewStatus(framework.Success, "")
		}
	}
	return MAX_SCORE, framework.NewStatus(framework.Success, "")
}


func (cs *Coscheduling) Filter(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	klog.Infof("Coscheduling filter called")
	klog.Flush()
	podName := pod.Name
	if !isDLPod(podName) {
		return framework.NewStatus(framework.Success, "")
	}
	cs.addJobTimeStampToMap(podName, pod.CreationTimestamp)
	cs.addJobTypeToJobTypeMap(podName)
	if cs.fullDDLGroupExists(getPodId(podName)){
		return framework.NewStatus(framework.Success, "")
	}
	return framework.NewStatus(framework.Unschedulable, "waiting for all pod types to exist")
}

func (cs *Coscheduling) fullDDLGroupExists(id string) bool {
	return len((*cs.jobTypes)[id]) == 3
}


func (cs *Coscheduling) ScoreExtensions() framework.ScoreExtensions {
	return cs
}


func getPodId(name string) string {
	if len(name) < 10 {
		return name
	}
	id := name[7:16]
	klog.Infof("Found podid %s", id)
	return name[7:16]
}
func isServerPod(name string) bool {
	return strings.Contains(name, "server")
}

func isWorkerPod(name string) bool {
	return strings.Contains(name, "worker")
}

func isSchedulerPod(name string) bool {
	return strings.Contains(name, "scheduler")
}

func containsDLType(name string) bool {
	return isWorkerPod(name) || isServerPod(name) || isSchedulerPod(name)
}

func isDLPod(name string) bool {
	return len(name) > 16 && containsDLType(name) && !containsDLType(getPodId(name))
}
