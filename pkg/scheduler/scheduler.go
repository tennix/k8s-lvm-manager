package scheduler

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"

	restful "github.com/emicklei/go-restful"
	"github.com/golang/glog"
	"github.com/tennix/k8s-lvm-manager/pkg/util"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	schedulerapiv1 "k8s.io/kubernetes/pkg/scheduler/api/v1"
)

var (
	errFailToRead  = restful.NewError(http.StatusBadRequest, "unable to read request body")
	errFailToWrite = restful.NewError(http.StatusInternalServerError, "unable to write response")
)

type Scheduler interface {
	Filter(*schedulerapiv1.ExtenderArgs) (*schedulerapiv1.ExtenderFilterResult, error)
	Priority(*schedulerapiv1.ExtenderArgs) (schedulerapiv1.HostPriorityList, error)
}

type lvmScheduler struct {
	kubeCli      kubernetes.Interface
	domainName   string
	storageClass string
}

var _ Scheduler = &lvmScheduler{}

func NewLVMScheduler(kubeCli kubernetes.Interface, domainName, storageClass string) Scheduler {
	return &lvmScheduler{
		kubeCli:      kubeCli,
		domainName:   domainName,
		storageClass: storageClass,
	}
}

func (ls *lvmScheduler) Filter(args *schedulerapiv1.ExtenderArgs) (*schedulerapiv1.ExtenderFilterResult, error) {
	pod := &args.Pod
	ns := pod.GetNamespace()
	podName := pod.GetName()
	glog.Infof("start scheduling pod %s/%s", ns, podName)
	var pvcName string
	for _, vol := range pod.Spec.Volumes {
		if vol.PersistentVolumeClaim != nil {
			pvcName = vol.PersistentVolumeClaim.ClaimName
			break
		}
	}
	if pvcName == "" {
		glog.Infof("empty pvc in pod %s/%s spec", ns, podName)
		return nil, errors.New("empty pvc")
	}
	pvc, err := ls.kubeCli.CoreV1().PersistentVolumeClaims(ns).Get(pvcName, metav1.GetOptions{})
	if err != nil {
		glog.Errorf("can't get pvc: %v", err)
		return nil, err
	}

	if *pvc.Spec.StorageClassName != ls.storageClass { // storage-class not match, return as it is
		glog.Infof("pvc storage class name: %s != %s", *pvc.Spec.StorageClassName, ls.storageClass)
		return &schedulerapiv1.ExtenderFilterResult{
			Nodes: args.Nodes,
		}, nil
	}

	if pvc.Annotations == nil {
		pvc.Annotations = make(map[string]string)
	}

	annNode := pvc.Annotations[util.AnnProvisionerNode]
	annHostPath := pvc.Annotations[util.AnnProvisionerHostPath]
	if annNode != "" && annHostPath != "" {
		for _, node := range args.Nodes.Items {
			if annNode == node.GetName() {
				glog.Infof("pod %s/%s will be scheduled on node %s", ns, podName, annNode)
				return &schedulerapiv1.ExtenderFilterResult{
					Nodes: &apiv1.NodeList{Items: []apiv1.Node{node}},
				}, nil
			}
		}
		return &schedulerapiv1.ExtenderFilterResult{
			Error: fmt.Sprintf("invalid nodeName: %s and hostPath: %s", annNode, annHostPath),
		}, nil
	}

	var vgName string
	var size string
	// NOTE: only support one PVC
	for _, container := range pod.Spec.Containers {
		for resourceName, quantity := range container.Resources.Requests {
			rn := resourceName.String()
			if strings.HasPrefix(rn, ls.domainName) {
				vgName = strings.Split(rn, "/")[1]
				size = quantity.String()
				break
			}
		}
	}

	nodeName := pvc.Annotations[util.AnnProvisionerNode]
	if nodeName == "" {
		nodeName = args.Nodes.Items[0].GetName() // simply select the first node
	}
	lvName := ns + "-" + pvcName
	pvc.Annotations[util.AnnProvisionerLVName] = lvName
	pvc.Annotations[util.AnnProvisionerVGName] = vgName
	pvc.Annotations[util.AnnProvisionerNode] = nodeName
	pvc.Annotations[util.AnnProvisionerPodName] = podName
	pvc.Annotations[util.AnnProvisionerHostPath] = ""
	pvc.Annotations[util.AnnProvisionerLVSize] = size
	_, err = ls.kubeCli.CoreV1().PersistentVolumeClaims(ns).Update(pvc)
	if err != nil {
		glog.Errorf("failed to update pvc %s annotation: %v", pvc.Name, err)
		return nil, err
	}
	return &schedulerapiv1.ExtenderFilterResult{Error: "waiting for pvc bound with pv"}, nil
}

func (ls *lvmScheduler) Priority(args *schedulerapiv1.ExtenderArgs) (schedulerapiv1.HostPriorityList, error) {
	return schedulerapiv1.HostPriorityList{}, nil
}

type server struct {
	scheduler Scheduler
	lock      sync.Mutex
}

func StartServer(kubeCli kubernetes.Interface, port int, domainName, storageClass string) {
	s := NewLVMScheduler(kubeCli, domainName, storageClass)
	svr := &server{scheduler: s}

	ws := new(restful.WebService)
	ws.
		Path("/scheduler").
		Consumes(restful.MIME_JSON).
		Produces(restful.MIME_JSON)
	ws.Route(ws.POST("/filter").To(svr.filterNode).
		Doc("filter nodes").
		Operation("filterNodes").
		Writes(schedulerapiv1.ExtenderFilterResult{}))
	ws.Route(ws.POST("/prioritize").To(svr.prioritizeNode).
		Doc("prioritize nodes").
		Operation("prioritizeNode").
		Writes(schedulerapiv1.HostPriorityList{}))
	restful.Add(ws)

	addr := fmt.Sprintf("0.0.0.0:%d", port)
	glog.Infof("start scheduler extender server, listening on %s", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		panic(err)
	}
}

func (svr *server) filterNode(req *restful.Request, resp *restful.Response) {
	svr.lock.Lock()
	defer svr.lock.Unlock()

	args := &schedulerapiv1.ExtenderArgs{}
	if err := req.ReadEntity(args); err != nil {
		errorResponse(resp, errFailToRead)
		return
	}

	filterResult, err := svr.scheduler.Filter(args)
	if err != nil {
		errorResponse(resp, restful.NewError(http.StatusInternalServerError,
			fmt.Sprintf("unable to filter nodes: %v", err)))
		return
	}

	if err := resp.WriteEntity(filterResult); err != nil {
		errorResponse(resp, errFailToWrite)
	}
}

func (svr *server) prioritizeNode(req *restful.Request, resp *restful.Response) {
	args := &schedulerapiv1.ExtenderArgs{}
	if err := req.ReadEntity(args); err != nil {
		errorResponse(resp, errFailToRead)
		return
	}

	priorityResult, err := svr.scheduler.Priority(args)
	if err != nil {
		errorResponse(resp, restful.NewError(http.StatusInternalServerError,
			fmt.Sprintf("unable to priority nodes: %v", err)))
		return
	}

	if err := resp.WriteEntity(priorityResult); err != nil {
		errorResponse(resp, errFailToWrite)
	}
}

func errorResponse(resp *restful.Response, err restful.ServiceError) {
	glog.Error(err.Message)
	if err := resp.WriteServiceError(err.Code, err); err != nil {
		glog.Errorf("unable to write error: %v", err)
	}
}
