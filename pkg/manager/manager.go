package manager

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strings"

	"github.com/golang/glog"
)

type LVManager struct {
	BaseDir string
	LVM     map[string]VolumeGroup
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

type PhysicalVolume struct {
	UUID string
	Name string
	Size string
	Free string
}

type LogicalVolume struct {
	UUID string
	Name string
	Size string
	Path string
}

type VolumeGroup struct {
	UUID string
	Name string
	Size string
	Free string
	Tags []string
	PVs  map[string]PhysicalVolume
	LVs  map[string]LogicalVolume
}

func scanLVM() (LVMReport, error) {
	var report LVMReport
	vg_cols := "vg_uuid,vg_name,vg_size,vg_free,lv_count,pv_count,vg_tags"
	vgs, err := exec.Command("vgs", "-o", vg_cols, "--units", "H", "--reportformat", "json").Output()
	if err != nil {
		glog.Errorf("failed to list vg: %v", err)
		return report, err
	}
	glog.Infof("vgs: %s", vgs)
	if err := json.Unmarshal(vgs, &report); err != nil {
		glog.Errorf("failed to unmarshal %s: %v", vgs, err)
		return report, err
	}
	glog.Infof("lvm: %+v", report)

	pv_cols := "pv_uuid,pv_name,vg_name,pv_size,pv_free"
	pvs, err := exec.Command("pvs", "-o", pv_cols, "--units", "H", "--reportformat", "json").Output()
	if err != nil {
		glog.Errorf("failed to list pv: %v", err)
		return report, err
	}
	glog.Infof("pvs: %s", pvs)
	if err := json.Unmarshal(pvs, &report); err != nil {
		glog.Errorf("failed to unmarshal %s: %v", pvs, err)
		return report, err
	}
	glog.Infof("lvm: %+v", report)

	lv_cols := "lv_uuid,lv_name,lv_size,lv_path,vg_name"
	lvs, err := exec.Command("lvs", "-o", lv_cols, "--units", "H", "--reportformat", "json").Output()
	if err != nil {
		glog.Errorf("failed to list lv: %v", err)
		return report, err
	}
	glog.Infof("lvs: %s", lvs)
	if err := json.Unmarshal(lvs, &report); err != nil {
		glog.Errorf("failed to unmarshal %s: %v", lvs, err)
		return report, err
	}
	glog.Infof("lvm: %+v", report)
	return report, nil
}

func (m *LVManager) SyncLVMStatus() error {
	vgs := map[string]VolumeGroup{}
	report, err := scanLVM()
	if err != nil {
		return err
	}
	for _, lvm := range report.Report {
		for _, vg := range lvm.VG {
			vgs[vg.VGName] = VolumeGroup{
				UUID: vg.VGUUID,
				Name: vg.VGName,
				Size: vg.VGSize,
				Free: vg.VGFree,
				PVs:  make(map[string]PhysicalVolume),
				LVs:  make(map[string]LogicalVolume),
				Tags: strings.Split(vg.VGTags, ","),
			}
		}
		for _, pv := range lvm.PV {
			p := PhysicalVolume{
				UUID: pv.PVUUID,
				Name: pv.PVName,
				Size: pv.PVSize,
				Free: pv.PVFree,
			}
			pvs := vgs[pv.VGName].PVs
			pvs[pv.PVName] = p
		}
		for _, lv := range lvm.LV {
			l := LogicalVolume{
				UUID: lv.LVUUID,
				Name: lv.LVName,
				Size: lv.LVSize,
				Path: lv.LVPath,
			}
			lvs := vgs[lv.VGName].LVs
			lvs[lv.LVName] = l
		}
	}
	m.LVM = vgs
	return nil
}

func (m *LVManager) AllocateLV(lvName, vgName string, size string) error {
	vg, ok := m.LVM[vgName]
	if !ok {
		return fmt.Errorf("no vg named %s", vgName)
	}
	if _, ok := vg.LVs[lvName]; ok {
		glog.Infof("lv %s already exist", lvName)
		return nil
	}
	output, err := exec.Command("lvcreate", "--zero", "n", "--name", lvName, "--size", size, vgName).Output()
	if err != nil {
		glog.Errorf("failed to create LV %s with size %s: %v", lvName, size, err)
		return err
	}
	glog.Infof("lvcreate output: %s", output)
	return nil
}

func (m *LVManager) FormatLV(lvName, vgName string, fsType string) error {
	devPath := getDevPath(lvName, vgName)
	output, err := exec.Command("mkfs", "--type", fsType, devPath).Output()
	if err != nil {
		glog.Errorf("failed to format LV %s to %s: %v", devPath, fsType, err)
		return err
	}
	glog.Infof("mkfs output: %s", output)
	return nil
}

func (m *LVManager) MountLV(lvName, vgName string) (string, error) {
	mntPath := path.Join(m.BaseDir, lvName)
	if err := os.MkdirAll(mntPath, os.ModeDir); err != nil {
		glog.Errorf("failed to create mount directory %s: %v", mntPath, err)
		return "", err
	}
	devPath := getDevPath(lvName, vgName)
	output, err := exec.Command("mount", devPath, mntPath).Output()
	if err != nil {
		glog.Infof("failed to mount LV %s to %s: %v", devPath, mntPath, err)
		return "", err
	}
	glog.Infof("mount output: %s", output)
	return mntPath, nil
}

func (m *LVManager) UnmountLV(name string) error {
	mntPath := path.Join(m.BaseDir, name)
	output, err := exec.Command("umount", mntPath).Output()
	if err != nil {
		glog.Errorf("failed to umount LV %s: %v", name, err)
		return err
	}
	glog.Infof("umount output: %s", output)
	return nil
}

func (m *LVManager) RemoveLV(lvName string, vgName string) error {
	devPath := getDevPath(lvName, vgName)
	output, err := exec.Command("lvremove", devPath, "--yes").Output()
	if err != nil {
		glog.Errorf("failed to remove LV %s: %v", devPath, err)
		return err
	}
	glog.Infof("lvremove output: %s", output)
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
// 			glog.Printf("invalid fstab format: %s\n", tab)
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

func getDevPath(lvName, vgName string) string {
	return path.Join(
		"/dev/mapper",
		fmt.Sprintf("%s-%s",
			strings.Replace(vgName, "-", "--", -1),
			strings.Replace(lvName, "-", "--", -1),
		),
	)
}
