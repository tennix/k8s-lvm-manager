package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path"
	"strings"
	"time"

	"github.com/pingcap/tidb-operator/pkg/util/label"
	"github.com/pingcap/tidb-operator/pkg/volumeprovisioner/externalstorage/controller"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type Manager struct {
	lvm      LVM
	baseDir  string
	nodeName string
	kubeCli  kubernetes.Interface
}

type LVMReport struct {
	Report []Report `json:"report"`
}

type Report struct {
	PV []PV `json:"pv"`
	VG []VG `json:"vg"`
	LV []LV `json:"lv"`
}

type LV struct {
	LVUUID string `json:"lv_uuid"`
	LVName string `json:"lv_name"`
	LVSize string `json:"lv_size"`
	LVPath string `json:"lv_path"`
	VGName string `json:"vg_name"`
}

type PV struct {
	PVUUID string `json:"pv_uuid"`
	PVName string `json:"pv_name"`
	VGName string `json:"vg_name"`
	PVSize string `json:"pv_size"`
	PVFree string `json:"pv_free"`
}

type VG struct {
	VGUUID  string `json:"vg_uuid"`
	VGName  string `json:"vg_name"`
	VGSize  string `json:"vg_size"`
	VGFree  string `json:"vg_free"`
	LVCount string `json:"lv_count"`
	PVCount string `json:"pv_count"`
	VGTags  string `json:"vg_tags"`
}

type LVM struct {
	PV map[string]PV
	VG map[string]VG
	LV map[string]LV
}

type fsRow struct {
	FS      string
	Dir     string
	Type    string
	Options string
	Dump    uint
	Pass    uint
}

type fsTab struct {
	path    string
	modTime time.Time
	fsTable []fsRow
}

type NodePatch struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value string `json:"value,omitempty"`
}

const (
	fstabFile              = "/etc/fstab"
	AnnProvisionerNode     = "volume-provisioner.pingcap.com/node"
	AnnProvisionerHostPath = "volume-provisioner.pingcap.com/hostpath"
	AnnProvisionerLVName   = "volume-provisioner.pingcap.com/lvName"
	AnnProvisionerLVFsType = "volume-provisioner.pingcap.com/fsType"
)

var (
	kubeconfig  string
	kubeVersion string
	fstab       fsTab
	domainName  string
	fsType      string
	duration    = 5 * time.Second
)

func init() {
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig file, omit this if run in cluster")
	flag.StringVar(&kubeVersion, "kube-version", "v1.7", "kubernetes version")
	flag.StringVar(&domainName, "domain-name", "pingcap.com", "domain name of extended resource")
	flag.StringVar(&fsType, "fs-type", "ext4", "LV fs type")
	flag.Parse()

}

var _ controller.Provisioner = &Manager{}

func NewManager(baseDir, nodeName string, kubeCli kubernetes.Interface) controller.Provisioner {
	return &Manager{
		baseDir:  baseDir,
		nodeName: nodeName,
		kubeCli:  kubeCli,
	}
}

func (m *Manager) Provision(opts controller.VolumeOptions) (*v1.PersistentVolume, error) {
	pvc := opts.PVC
	ns := pvc.GetNamespace()
	name := pvc.GetName()
	ann := pvc.GetAnnotations()

	annNode := ann[AnnProvisionerNode]
	podName := ann[label.AnnPodNameKey]
	if podName == "" {
		return nil, errors.New("pvc doesn't contain pod annotation")
	}
	pod, err := m.kubeCli.CoreV1().Pods(ns).Get(podName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	var vgName string
	var quantity resource.Quantity
	// NOTE: only support one PVC
	for _, container := range pod.Spec.Containers {
		for resourceName, q := range container.Resources.Requests {
			r := resourceName.String()
			if strings.HasPrefix(r, domainName) {
				vgName := strings.Split(r, "/")[1]
				if _, found := m.lvm.VG[vgName]; found {
					vgName = r
					quantity = q
					break
				}
			}
		}
	}

	if err := m.AllocateLV(name, vgName, quantity.String()); err != nil {
		return nil, err
	}
	if err := m.FormatLV(name, fsType); err != nil {
		return nil, err
	}
	if err := m.MountLV(name); err != nil {
		return nil, err
	}

	annHostPath := path.Join(m.baseDir, name)

	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: opts.PVName,
			Annotations: map[string]string{
				AnnProvisionerNode:     annNode,
				AnnProvisionerHostPath: annHostPath,
				label.AnnPodNameKey:    podName,
			},
			// Labels: pvLabel.Labels(),
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: opts.PersistentVolumeReclaimPolicy,
			AccessModes:                   opts.PVC.Spec.AccessModes,
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): quantity,
			},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				HostPath: &v1.HostPathVolumeSource{
					Path: annHostPath,
				},
			},
		},
	}
	return pv, nil
}

func (m *Manager) Delete(pv *v1.PersistentVolume) error {
	pvName := pv.GetName()
	ann := pv.GetAnnotations()
	node := ann[AnnProvisionerNode]
	if node != m.nodeName {
		return &controller.IgnoredError{fmt.Sprintf("PV[%s] is not managed by this provisioner, managed by: %s, skipping", pvName, node)}
	}

	lvName := ann[AnnProvisionerLVName]

	if err := m.UnmountLV(lvName); err != nil {
		return err
	}
	if err := m.RemoveLV(lvName); err != nil {
		return err
	}

	return nil
}

// TODO: persistent LV in fstab
// func (t *fsTab) load() error {
// 	f, err := os.Open(fstabFile)
// 	if err != nil {
// 		return err
// 	}
// 	defer f.Close()

// 	scanner := bufio.NewScanner(f)
// 	for scanner.Scan() {
// 		text := scanner.Text()
// 		if strings.HasPrefix(text, "#") {
// 			continue
// 		}
// 		tab := strings.Fields(text)
// 		if len(tab) != 6 {
// 			log.Printf("invalid fstab format: %s\n", tab)
// 			continue
// 		}
// 		row := fsRow{
// 			FS:      tab[0],
// 			Dir:     tab[1],
// 			Type:    tab[2],
// 			Options: tab[3],
// 			Dump:    tab[4],
// 			Pass:    tab[5],
// 		}
// 		t.fsTable = append(t.fsTable, row)
// 	}
// }

func main() {
	nodeName := os.Getenv("MY_NODE_NAME")
	if nodeName == "" {
		log.Fatalf("MY_NODE_NAME environment variable not set")
	}

	manager := &Manager{nodeName: nodeName}

	provisionerName := fmt.Sprintf("%s/lvm-volume-provisioner", domainName)

	if err := manager.SyncLVMStatus(); err != nil {
		panic(err)
	}
	var err error
	var cfg *rest.Config
	if kubeconfig == "" {
		cfg, err = rest.InClusterConfig()
	} else {
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	if err != nil {
		log.Fatalf("failed to get kube config: %v", err)
	}

	fmt.Printf("LVM: %+v\n", manager.lvm)

	manager.kubeCli, err = kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("failed to get kubernetes clientset: %v", err)
	}
	if err := manager.UpdateNodeStatus(); err != nil {
		panic(err)
	}
	pc := controller.NewProvisionController(
		kubeCli,
		provisionerName,
		manager,
		kubeVersion,
	)
	wait.Forever(func() {
		pc.Run(wait.NeverStop)
	}, duration)
}

func scanLVM() (LVMReport, error) {
	var report LVMReport
	vg_cols := "vg_uuid,vg_name,vg_size,vg_free,lv_count,pv_count,vg_tags"
	vgs, err := exec.Command("vgs", "-o", vg_cols, "--reportformat", "json").Output()
	if err != nil {
		return report, err
	}
	fmt.Printf("vgs: %s\n", vgs)
	if err := json.Unmarshal(vgs, &report); err != nil {
		return report, err
	}
	fmt.Printf("lvm: %+v\n", report)

	pv_cols := "pv_uuid,pv_name,vg_name,pv_size,pv_free"
	pvs, err := exec.Command("pvs", "-o", pv_cols, "--reportformat", "json").Output()
	if err != nil {
		return report, err
	}
	fmt.Printf("pvs: %s\n", pvs)
	if err := json.Unmarshal(pvs, &report); err != nil {
		return report, err
	}
	fmt.Printf("lvm: %+v\n", report)

	lv_cols := "lv_uuid,lv_name,lv_size,lv_path,vg_name"
	lvs, err := exec.Command("lvs", "-o", lv_cols, "--reportformat", "json").Output()
	if err != nil {
		return report, err
	}
	fmt.Printf("lvs: %s\n", lvs)
	if err := json.Unmarshal(lvs, &report); err != nil {
		return report, err
	}
	fmt.Printf("lvm: %+v\n", report)
	return report, nil
}

func (m *Manager) SyncLVMStatus() error {
	pvs := map[string]PV{}
	vgs := map[string]VG{}
	lvs := map[string]LV{}
	report, err := scanLVM()
	if err != nil {
		return err
	}
	for _, lvm := range report.Report {
		for _, vg := range lvm.VG {
			vgs[vg.VGName] = vg
		}
		for _, pv := range lvm.PV {
			pvs[pv.PVName] = pv
		}
		for _, lv := range lvm.LV {
			lvs[lv.LVName] = lv
		}
	}
	m.lvm = LVM{
		PV: pvs,
		VG: vgs,
		LV: lvs,
	}
	return nil
}

func (m *Manager) UpdateNodeStatus() error {
	patches := make([]NodePatch, len(m.lvm.VG))
	for _, vg := range m.lvm.VG {
		patch := NodePatch{
			Op:    "add",
			Path:  fmt.Sprintf("/status/capacity/%s~1%s", domainName, vg.VGName),
			Value: vg.VGSize,
		}
		patches = append(patches, patch)
	}
	if len(patches) == 0 {
		return nil
	}
	data, err := json.Marshal(patches)
	if err != nil {
		return err
	}
	_, err = m.kubeCli.CoreV1().Nodes().PatchStatus(m.nodeName, data)
	return err
}

func (m *Manager) AllocateLV(lvName, vgName string, size string) error {
	output, err := exec.Command("lvcreate", "--name", lvName, "--size", size, vgName).Output()
	if err != nil {
		return err
	}
	fmt.Printf("lvcreate output: %s\n", output)
	return nil
}

func (m *Manager) FormatLV(name string, fsType string) error {
	output, err := exec.Command("mkfs", "--type", fsType, name).Output()
	if err != nil {
		return err
	}
	fmt.Printf("mkfs output: %s\n", output)
	return nil
}

func (m *Manager) MountLV(name string) error {
	dir := path.Join(m.baseDir, name)
	output, err := exec.Command("mount", name, dir).Output()
	if err != nil {
		return err
	}
	fmt.Printf("mount output: %s\n", output)
	return nil
}

func (m *Manager) UnmountLV(name string) error {
	dir := path.Join(m.baseDir, name)
	output, err := exec.Command("umount", dir).Output()
	if err != nil {
		return err
	}
	fmt.Printf("umount output: %s\n", output)
	return nil
}

func (m *Manager) RemoveLV(name string) error {
	output, err := exec.Command("lvremove", name, "--yes").Output()
	if err != nil {
		return err
	}
	fmt.Printf("lvremove output: %s\n", output)
	return nil
}
