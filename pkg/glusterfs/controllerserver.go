package glusterfs

import (
	"context"
	"errors"
	"os"
	"os/exec"

	"github.com/gluster/gluster-csi-driver/pkg/glusterfs/utils"

	csi "github.com/container-storage-interface/spec/lib/go/csi/v0"
	//"github.com/gluster/glusterd2/pkg/api"
	"github.com/golang/glog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	glusterDescAnn            = "GlusterFS-CSI"
	glusterDescAnnValue       = "gluster.org/glusterfs-csi"
	defaultVolumeSize   int64 = 1000 * utils.MB // default volume size ie 1 GB
	defaultReplicaCount       = 3
	mountPath                 = "/mnt/glusterfs/volume1/"
)

var errVolumeNotFound = errors.New("volume not found")

type ControllerServer struct {
	*GfDriver
}

//CreateVolume creates and starts the volume
func (cs *ControllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	//	var glusterServer string
	//	var mountPath string

	if req == nil {
		glog.Errorf("volume create request is nil")
		return nil, status.Errorf(codes.InvalidArgument, "request cannot be empty")
	}

	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "Name is a required field")
	}
	glog.V(1).Infof("creating volume with name ", req.Name)

	if req.VolumeCapabilities == nil || len(req.VolumeCapabilities) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume capabilities is a required field")
	}

	// If capacity mentioned, pick that or use default size 1 GB
	volSizeBytes := defaultVolumeSize
	if req.GetCapacityRange() != nil {
		volSizeBytes = int64(req.GetCapacityRange().GetRequiredBytes())
	}

	// // execute below command
	fileName := mountPath + req.Name
	_, err := os.Create(fileName)
	if err != nil {
		glog.Errorf("failed to create file: %+v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	err = os.Truncate(fileName, volSizeBytes)
	if err != nil {
		glog.Errorf("failed to truncate file: %+v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	cmd := exec.Command("mkfs.xfs", "-f", fileName)
	_, err = cmd.Output()
	if err != nil {
		glog.Errorf("failed to mkfs directory: %+v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	resp := &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			Id:            req.Name,
			CapacityBytes: volSizeBytes,
			Attributes:    map[string]string{"volume": fileName},
		},
	}
	glog.V(4).Infof("CSI Volume response: %+v", resp)
	return resp, nil
}

// DeleteVolume deletes the given volume.
func (cs *ControllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "Volume delete request is nil")
	}

	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is nil")
	}
	glog.V(2).Infof("Deleting volume with ID: %v", req.VolumeId)
	err := os.Remove(mountPath + req.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to delete volume", err.Error())
	}
	return &csi.DeleteVolumeResponse{}, nil
}

// ControllerPublishVolume return Unimplemented error
func (cs *ControllerServer) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

//ControllerUnpublishVolume return Unimplemented error
func (cs *ControllerServer) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

// ValidateVolumeCapabilities checks whether the volume capabilities requested
// are supported.
func (cs *ControllerServer) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "volume capabilities request is nil")
	}

	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "ValidateVolumeCapabilities() - Volume ID is nil")
	}

	if req.VolumeCapabilities == nil {
		return nil, status.Error(codes.InvalidArgument, "ValidateVolumeCapabilities is nil")
	}
	_, err := cs.client.VolumeStatus(req.VolumeId)
	if err != nil {
		return nil, status.Error(codes.NotFound, "ValidateVolumeCapabilities() - Invalid Volume ID")
	}
	var vcaps []*csi.VolumeCapability_AccessMode
	for _, mode := range []csi.VolumeCapability_AccessMode_Mode{
		csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY,
		csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
		csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
	} {
		vcaps = append(vcaps, &csi.VolumeCapability_AccessMode{Mode: mode})
	}
	capSupport := true
	IsSupport := func(mode csi.VolumeCapability_AccessMode_Mode) bool {
		for _, m := range vcaps {
			if mode == m.Mode {
				return true
			}
		}
		return false
	}

	for _, cap := range req.VolumeCapabilities {
		if !IsSupport(cap.AccessMode.Mode) {
			capSupport = false
		}
	}
	resp := &csi.ValidateVolumeCapabilitiesResponse{
		Supported: capSupport,
	}
	glog.V(1).Infof("glusterfs CSI driver support capabilities: %v", resp)
	return resp, nil
}

// ListVolumes returns a list of all requested volumes
func (cs *ControllerServer) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	//Fetch all the volumes in the TSP
	//volumes, err := cs.client.Volumes("")
	//if err != nil {
	//return nil, status.Error(codes.Internal, err.Error())
	//}
	//mountPath := "/mnt/glusterfs/volume1"
	// volumes = ls -l mountPath

	var entries []*csi.ListVolumesResponse_Entry
	// for _, vol := range volumes {
	// 	entries = append(entries, &csi.ListVolumesResponse_Entry{
	// 		Volume: &csi.Volume{
	// 			Id:            vol.Name,
	// 			CapacityBytes: (int64(v.Size.Capacity)) * utils.MB,
	// 		},
	// 	})
	// }

	resp := &csi.ListVolumesResponse{
		Entries: entries,
	}

	return resp, nil
}

// GetCapacity returns the capacity of the storage pool
func (cs *ControllerServer) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

// ControllerGetCapabilities returns the capabilities of the controller service.
func (cs *ControllerServer) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	newCap := func(cap csi.ControllerServiceCapability_RPC_Type) *csi.ControllerServiceCapability {
		return &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: cap,
				},
			},
		}
	}

	var caps []*csi.ControllerServiceCapability
	for _, cap := range []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_LIST_VOLUMES,
	} {
		caps = append(caps, newCap(cap))
	}

	resp := &csi.ControllerGetCapabilitiesResponse{
		Capabilities: caps,
	}

	return resp, nil
}

//CreateSnapshot
func (cs *ControllerServer) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

//DeleteSnapshot
func (cs *ControllerServer) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

//ListSnapshots
func (cs *ControllerServer) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}
