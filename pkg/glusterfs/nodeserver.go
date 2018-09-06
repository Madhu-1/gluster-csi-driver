package glusterfs

import (
	"context"
	"os"
	"os/exec"
	"strconv"
	"strings"

	csi "github.com/container-storage-interface/spec/lib/go/csi/v0"
	"github.com/golang/glog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/kubernetes/pkg/util/mount"
	"k8s.io/kubernetes/pkg/volume/util"
)

type NodeServer struct {
	*GfDriver
}

var glusterMounter = mount.New("")

// NodeStageVolume mounts the volume to a staging path on the node.
func (ns *NodeServer) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

// NodeUnstageVolume unstages the volume from the staging path
func (ns *NodeServer) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

// NodePublishVolume mounts the volume mounted to the staging path to the target path
func (ns *NodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {

	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "request cannot be empty")
	}

	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "NodePublishVolume Volume ID must be provided")
	}

	glog.V(2).Infof("Request received %+v", req)
	targetPath := req.GetTargetPath()

	if targetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "NodePublishVolume Target Path cannot be empty")
	}
	notMnt, err := glusterMounter.IsLikelyNotMountPoint(targetPath)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(targetPath, 0750); err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}
			notMnt = true
		} else {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	if !notMnt {
		return &csi.NodePublishVolumeResponse{}, nil
	}

	mo := req.GetVolumeCapability().GetMount().GetMountFlags()
	if req.GetReadonly() {
		mo = append(mo, "ro")
	}
	/* For 'block' we will use 'loopback' driver */

	srcFile := req.GetVolumeAttributes()["file-name"]

	mountPath := "/mnt/glusterfs/volume1/"
	// execute below command
	// fileName = mountPath + volName
	// $(truncate -s volSizeBytes fileName)
	// `device=$(losetup --show --find fileName)`
	// mkfs.xfs $device

	err = os.MkdirAll(mountPath, 0750)
	if err != nil {
		glog.V(4).Infof("failed to create directory: %+v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	file := srcFile
	// _, err = os.Create(file)
	// if err != nil {s
	// 	glog.V(4).Infof("failed to create directory: %+v", err)
	// }
	volSizeBytes := req.GetVolumeAttributes()["size"]
	_, err = os.Create(file)
	if err != nil {
		glog.V(4).Infof("failed to create file: %+v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	var size int64
	if size, err = strconv.ParseInt(volSizeBytes, 10, 64); err == nil {
		glog.V(4).Infof("failed to parse size: %+v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	err = os.Truncate(file, size)
	if err != nil {
		glog.V(4).Infof("failed to truncate file: %+v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	cmd := exec.Command("losetup", "--show", "--find", file)
	device := ""
	out, err := cmd.Output()
	if err == nil {
		deviceName := strings.Split(string(out), " \n")
		device = strings.Trim(deviceName[0], "\n")
	} else {
		glog.V(4).Infof("failed to device directory: %+v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	cmd = exec.Command("mkfs.xfs", "-f", device)
	_, err = cmd.Output()
	if err != nil {
		glog.V(4).Infof("failed to mkfs directory: %+v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	//mo = []string{}
	err = glusterMounter.Mount(srcFile, targetPath, "xfs", []string{})
	if err != nil {
		if os.IsPermission(err) {
			return nil, status.Error(codes.PermissionDenied, err.Error())
		}
		if strings.Contains(err.Error(), "invalid argument") {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &csi.NodePublishVolumeResponse{}, nil
}

// NodeUnpublishVolume unmounts the volume from the target path
func (ns *NodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "request cannot be empty")
	}

	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "NodeUnpublishVolume Volume ID must be provided")
	}

	if req.TargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "NodeUnpublishVolume Target Path must be provided")
	}

	targetPath := req.GetTargetPath()
	notMnt, err := glusterMounter.IsLikelyNotMountPoint(targetPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, status.Error(codes.NotFound, "Targetpath not found")
		}
		return nil, status.Error(codes.Internal, err.Error())

	}

	if notMnt {
		return nil, status.Error(codes.NotFound, "Volume not mounted")
	}

	err = util.UnmountPath(req.GetTargetPath(), glusterMounter)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (ns *NodeServer) NodeGetId(ctx context.Context, req *csi.NodeGetIdRequest) (*csi.NodeGetIdResponse, error) {
	return &csi.NodeGetIdResponse{
		NodeId: ns.GfDriver.NodeID,
	}, nil
}

//NodeGetInfo info
func (ns *NodeServer) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId: ns.GfDriver.NodeID,
	}, nil
}

// NodeGetCapabilities returns the supported capabilities of the node server
func (ns *NodeServer) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	// currently there is a single NodeServer capability according to the spec
	nscap := &csi.NodeServiceCapability{
		Type: &csi.NodeServiceCapability_Rpc{
			Rpc: &csi.NodeServiceCapability_RPC{
				Type: csi.NodeServiceCapability_RPC_UNKNOWN,
			},
		},
	}
	glog.V(1).Infof("node capabiilities: %+v", nscap)
	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: []*csi.NodeServiceCapability{
			nscap,
		},
	}, nil
}
