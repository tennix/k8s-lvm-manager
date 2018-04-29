package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.com/kubernetes-incubator/external-storage/lib/controller"
	"github.com/tennix/k8s-lvm-manager/pkg/provisioner"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	kubeconfig  string
	kubeVersion string
	domainName  string
	duration    = 5 * time.Second
)

func init() {
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig file, omit this if run in cluster")
	flag.StringVar(&kubeVersion, "kube-version", "v1.7", "kubernetes version")
	flag.StringVar(&domainName, "domain-name", "pingcap.com", "domain name of extended resource")
	flag.Parse()
}

func main() {
	provisionerName := fmt.Sprintf("%s/lvm-volume-provisioner", domainName)

	var err error
	var cfg *rest.Config
	if kubeconfig == "" {
		cfg, err = rest.InClusterConfig()
	} else {
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	if err != nil {
		glog.Fatalf("failed to get kube config: %v", err)
	}

	kubeCli, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		glog.Fatalf("failed to get kubernetes clientset: %v", err)
	}
	prov := provisioner.New(kubeCli)

	pc := controller.NewProvisionController(
		kubeCli,
		provisionerName,
		prov,
		kubeVersion,
	)
	wait.Forever(func() {
		pc.Run(wait.NeverStop)
	}, duration)
}
