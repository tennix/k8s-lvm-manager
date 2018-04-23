package main

import (
	"errors"
	"flag"
	"fmt"
	"net/http"
	"sync"
	"time"

	restful "github.com/emicklei/go-restful"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-operator/pkg/util"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	schedulerapiv1 "k8s.io/kubernetes/plugin/pkg/scheduler/api/v1"
)

const (
	AnnProvisionerNode     = "volume-provisioner.pingcap.com/node"
	AnnProvisionerHostPath = "volume-provisioner.pingcap.com/hostpath"
)

var (
	errFailToRead  = restful.NewError(http.StatusBadRequest, "unable to read request body")
	errFailToWrite = restful.NewError(http.StatusInternalServerError, "unable to write response")
	duration       = 5 * time.Second
	kubeconfig     string
	port           int
	storageClass   string
)

func init() {
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig file, omit this if run in cluster")
	flag.StringVar(&storageClass, "storage-class", "lvm-volume-provisioner", "storage class for volume provisioner")
	flag.IntVar(&port, "port", 10262, "The port that the tidb scheduler's http service runs on (default 10262)")
	flag.Parse()
}

type Scheduler interface {
	Filter(*schedulerapiv1.ExtenderArgs) (*schedulerapiv1.ExtenderFilterResult, error)
	Priority(*schedulerapiv1.ExtenderArgs) (schedulerapiv1.HostPriorityList, error)
}

type lvmScheduler struct {
	kubeCli kubernetes.Interface
}

var _ Scheduler = &lvmScheduler{}

func NewLVMScheduler(kubeCli kubernetes.Interface) Scheduler {
	return &lvmScheduler{
		kubeCli: kubeCli,
	}
}

func (ls *lvmScheduler) Filter(args *schedulerapiv1.ExtenderArgs) (*schedulerapiv1.ExtenderFilterResult, error) {
	pod := &args.Pod
	ns := pod.GetNamespace()
	podName := pod.GetName()
	fmt.Printf("start scheduling pod %s/%s", ns, podName)
	var pvcName string
	for _, vol := range pod.Spec.Volumes {
		if vol.PersistentVolumeClaim != nil {
			pvcName = vol.PersistentVolumeClaim.ClaimName
			break
		}
	}
	if pvcName == "" {
		fmt.Printf("empty pvc in pod %s/%s spec", ns, podName)
		return nil, errors.New("empty pvc")
	}
	pvc, err := ls.kubeCli.CoreV1().PersistentVolumeClaims(ns).Get(pvcName, metav1.GetOptions{})
	if err != nil {
		fmt.Printf("can't get pvc: %v", err)
		return nil, err
	}

	if *pvc.Spec.StorageClassName != storageClass { // storage-class not match, return as it is
		return &schedulerapiv1.ExtenderFilterResult{
			Nodes: args.Nodes,
		}, nil
	}

	annNode := pvc.Annotations[AnnProvisionerNode]
	annHostPath := pvc.Annotations[AnnProvisionerHostPath]
	if annNode != "" && annHostPath != "" {
		for _, node := range args.Nodes.Items {
			if annNode == node.GetName() {
				fmt.Printf("pod %s/%s will be scheduled on node %s", ns, podName, annNode)
				return &schedulerapiv1.ExtenderFilterResult{
					Nodes: &apiv1.NodeList{Items: []apiv1.Node{node}},
				}, nil
			}
		}
		return &schedulerapiv1.ExtenderFilterResult{
			Error: fmt.Sprintf("invalid nodeName: %s and hostPath: %s", annNode, annHostPath),
		}, nil
	}

	node := args.Nodes.Items[0] // simply select the first nodes
	pvc.Annotations[AnnProvisionerNode] = node.GetName()
	_, err = ls.kubeCli.CoreV1().PersistentVolumeClaims(ns).Update(pvc)
	if err != nil {
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

func StartServer(kubeCli kubernetes.Interface, port int) {
	s := NewLVMScheduler(kubeCli)
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
	log.Infof("start scheduler extender server, listening on %s", addr)
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
	log.Error(err.Message)
	if err := resp.WriteServiceError(err.Code, err); err != nil {
		log.Errorf("unable to write error: %v", err)
	}
}

func main() {
	var cfg *rest.Config
	var err error
	if kubeconfig == "" {
		cfg, err = rest.InClusterConfig()
	} else {
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	if err != nil {
		log.Fatalf("failed to build rest.Config: %v", err)
	}
	cfg.QPS = util.ClientCfgQPS
	cfg.Burst = util.ClientCfgBurst

	kubeCli, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("failed to get kubernetes Clientset: %v", err)
	}

	wait.Forever(func() {
		StartServer(kubeCli, port)
	}, duration)
}
