package glusterfs

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gluster/gluster-csi-driver/pkg/glusterfs/utils"

	csi "github.com/container-storage-interface/spec/lib/go/csi/v0"
	"github.com/gluster/glusterd2/pkg/api"
	"github.com/golang/glog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	glusterDescAnn            = "GlusterFS-CSI"
	glusterDescAnnValue       = "gluster.org/glusterfs-csi"
	defaultVolumeSize   int64 = 1000 * utils.MB // default volume size ie 1 GB
	defaultReplicaCount       = 3
)

var errVolumeNotFound = errors.New("volume not found")

type ControllerServer struct {
	*GfDriver
}

//CreateVolume creates and starts the volume
func (cs *ControllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	var glusterServer string
	var bkpServers []string

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

	volSizeMB := int(utils.RoundUpSize(volSizeBytes, 1024*1024))

	// Get Volume name : TODO use the values from request
	volumeName := req.Name
	glusterVol := req.GetParameters()["glustervol"]
	glusterServer = req.GetParameters()["glusterserver"]
	glusterURL := req.GetParameters()["glusterurl"]
	glusterURLPort := req.GetParameters()["glusterurlport"]
	glusterUser := req.GetParameters()["glusteruser"]
	glusterUserSecret := req.GetParameters()["glusterusersecret"]

	glog.V(3).Infof("Request fields:[ %v %v %v %v %v %v]", glusterVol, glusterServer, glusterURL, glusterURLPort, glusterUser, glusterUserSecret)

	glusterServer, bkpServers, err := cs.checkExistingVolume(volumeName, volSizeMB)
	if err != nil && err != errVolumeNotFound {
		return nil, err

	}
	if err == nil {
		resp := &csi.CreateVolumeResponse{
			Volume: &csi.Volume{
				Id:            volumeName,
				CapacityBytes: int64(volSizeBytes),
				Attributes: map[string]string{
					"glustervol":        volumeName,
					"glusterserver":     glusterServer,
					"glusterbkpservers": strings.Join(bkpServers, ":"),
				},
			},
		}
		return resp, nil
	}

	if req.VolumeContentSource.GetSnapshot().GetId() != "" {
		snapName := req.VolumeContentSource.GetSnapshot().GetId()

		_, err := cs.GfDriver.client.SnapshotInfo(snapName)
		if err != nil {
			errResp := cs.client.LastErrorResponse()
			//errResp will be nil in case of No route to host error
			if errResp != nil && errResp.StatusCode == http.StatusNotFound {
				return nil, status.Error(codes.NotFound, err.Error())
			}
			return nil, status.Error(codes.Internal, err.Error())
		}
		//TODO create snapshot clone
		var snapreq api.SnapCloneReq
		snapreq.CloneName = req.Name
		resp, err := cs.client.SnapshotClone(snapName, snapreq)
		if err != nil {
			glog.Errorf("failed to create volume clone: %v", err)
			return nil, status.Error(codes.Internal, fmt.Sprintf("failed to create volume clone: %s", err.Error()))
		}
		volumeResp, err := cs.startVolume(req.Name, volSizeBytes)
		if err != nil {
			return nil, err
		}
		glog.V(4).Infof("CSI Volume response: %+v", resp)
		return volumeResp, nil

	}
	// If volume does not exist, provision volume
	glog.V(4).Infof("Received request to create/provision volume name:%s with size:%d", volumeName, volSizeMB)
	volMetaMap := make(map[string]string)
	volMetaMap[glusterDescAnn] = glusterDescAnnValue
	volumeReq := api.VolCreateReq{
		Name:         volumeName,
		Metadata:     volMetaMap,
		ReplicaCount: defaultReplicaCount,
		Size:         uint64(volSizeMB),
	}

	glog.V(2).Infof("volume request: %+v", volumeReq)
	volumeCreateResp, err := cs.client.VolumeCreate(volumeReq)
	if err != nil {
		glog.Errorf("failed to create volume : %v", err)
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to create volume: %s", err.Error()))
	}

	glog.V(3).Infof("volume create response : %+v", volumeCreateResp)
	resp, err := cs.startVolume(volumeName, volSizeBytes)
	if err != nil {
		return nil, err
	}
	glog.V(4).Infof("CSI Volume response: %+v", resp)
	return resp, nil
}
func (cs *ControllerServer) startVolume(volumeName string, volSizeBytes int64) (*csi.CreateVolumeResponse, error) {
	err := cs.client.VolumeStart(volumeName, true)
	if err != nil {
		//we dont need to delete the volume if volume start fails
		//as we are listing the volumes and starting it again
		//before sending back the response
		glog.Errorf("failed to start volume:%v", err)
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to start volume %s", err.Error()))
	}

	glusterServer, bkpServers, err := cs.getClusterNodes()

	if err != nil {
		glog.Errorf("failed to fetch details of cluster nodes: %v", err)
		return nil, status.Error(codes.Internal, fmt.Sprintf("error in fecthing peer details %s", err.Error()))
	}

	resp := &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			Id:            volumeName,
			CapacityBytes: int64(volSizeBytes),
			Attributes: map[string]string{
				"glustervol":        volumeName,
				"glusterserver":     glusterServer,
				"glusterbkpservers": strings.Join(bkpServers, ":"),
			},
		},
	}
	return resp, nil
}
func (cs *ControllerServer) checkExistingSnapshot(snapName string) error {
	_, err := cs.GfDriver.client.SnapshotInfo(snapName)
	if err != nil {
		errResp := cs.client.LastErrorResponse()
		//errResp will be nil in case of No route to host error
		if errResp != nil && errResp.StatusCode != http.StatusNotFound {
			return status.Error(codes.Internal, err.Error())
		}
		return err
	}
	return nil
}
func (cs *ControllerServer) checkExistingVolume(volumeName string, volSizeMB int) (string, []string, error) {
	var (
		tspServers  []string
		mountServer string
		err         error
	)

	vol, err := cs.client.VolumeStatus(volumeName)
	if err != nil {
		glog.Errorf("failed to fetch volume : %v", err)
		errResp := cs.client.LastErrorResponse()
		//errResp will be nil in case of No route to host error
		if errResp != nil && errResp.StatusCode == http.StatusNotFound {
			return "", nil, errVolumeNotFound
		}
		return "", nil, status.Error(codes.Internal, fmt.Sprintf("error in fetching volume details %s", err.Error()))

	}

	// Do the owner validation
	if glusterAnnVal, found := vol.Info.Metadata[glusterDescAnn]; found {
		if glusterAnnVal != glusterDescAnnValue {
			return "", nil, status.Errorf(codes.Internal, "volume %s (%s) is not owned by Gluster CSI driver",
				vol.Info.Name, vol.Info.Metadata)
		}
	} else {
		return "", nil, status.Errorf(codes.Internal, "volume %s (%s) is not owned by Gluster CSI driver",
			vol.Info.Name, vol.Info.Metadata)
	}

	if int(vol.Size.Capacity) != volSizeMB {
		return "", nil, status.Error(codes.AlreadyExists, fmt.Sprintf("volume already exits with different size: %d", vol.Size.Capacity))
	}

	//volume not started, start the volume
	if !vol.Online {
		err := cs.client.VolumeStart(vol.Info.Name, true)
		if err != nil {
			return "", nil, status.Error(codes.Internal, fmt.Sprintf("failed to start volume"))
		}
	}

	glog.Info("Requested volume (%s) already exists in the storage pool", volumeName)
	mountServer, tspServers, err = cs.getClusterNodes()

	if err != nil {
		return "", nil, status.Error(codes.Internal, fmt.Sprintf("error in fetching backup/peer server details %s", err.Error()))
	}

	return mountServer, tspServers, nil
}

func (cs *ControllerServer) getClusterNodes() (string, []string, error) {
	peers, err := cs.client.Peers()
	if err != nil {
		return "", nil, err
	}
	glusterServer := ""
	bkpservers := []string{}

	for i, p := range peers {
		if i == 0 {
			for _, a := range p.PeerAddresses {
				ip := strings.Split(a, ":")
				glusterServer = ip[0]
			}

			continue
		}
		for _, a := range p.PeerAddresses {
			ip := strings.Split(a, ":")
			bkpservers = append(bkpservers, ip[0])
		}

	}
	glog.V(2).Infof("Gluster server and Backup servers [%+v,%+v]", glusterServer, bkpservers)

	return glusterServer, bkpservers, err
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

	err := cs.client.VolumeStop(req.VolumeId)

	if err != nil {
		errResp := cs.client.LastErrorResponse()
		//errResp will be nil in case of No route to host error
		if errResp != nil && errResp.StatusCode == http.StatusNotFound {
			return &csi.DeleteVolumeResponse{}, nil
		}
		return nil, status.Errorf(codes.Internal, "failed to stop volume %s", err.Error())
	}

	err = cs.client.VolumeDelete(req.VolumeId)
	if err != nil {
		errResp := cs.client.LastErrorResponse()
		//errResp will be nil in case of No route to host error
		if errResp != nil && errResp.StatusCode == http.StatusNotFound {
			return &csi.DeleteVolumeResponse{}, nil
		}
		glog.Errorf("Volume delete failed :%v", err)
		return nil, status.Errorf(codes.Internal, "error deleting volume: %s", err.Error())
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
	volumes, err := cs.client.Volumes("")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	var entries []*csi.ListVolumesResponse_Entry
	for _, vol := range volumes {
		v, e := cs.client.VolumeStatus(vol.Name)
		if e != nil {
			return nil, status.Errorf(codes.Internal, "failed to get volume status %s", e.Error())
		}
		entries = append(entries, &csi.ListVolumesResponse_Entry{
			Volume: &csi.Volume{
				Id:            vol.Name,
				CapacityBytes: (int64(v.Size.Capacity)) * utils.MB,
			},
		})
	}

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
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
		csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
	} {
		caps = append(caps, newCap(cap))
	}

	resp := &csi.ControllerGetCapabilitiesResponse{
		Capabilities: caps,
	}

	return resp, nil
}

//CreateSnapshot creates a snapshot with given name
func (cs *ControllerServer) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "CreateSnapshot request is nil")
	}
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "CreateSnapshot - name cannot be empty")
	}

	if req.GetSourceVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "CreateSnapshot - sourceVolumeId is nil")
	}

	if req.GetName() == req.GetSourceVolumeId() {
		//TODO in glusterd2 we cannot create a snapshot as same name as volume name
		return nil, status.Error(codes.InvalidArgument, "CreateSnapshot - sourceVolumeId  and snapshot name cannot be same")
	}
	snapInfo, err := cs.GfDriver.client.SnapshotInfo(req.Name)
	if err != nil {

		errResp := cs.client.LastErrorResponse()
		//errResp will be nil in case of No route to host error
		if errResp != nil && errResp.StatusCode != http.StatusNotFound {

			return nil, status.Errorf(codes.Internal, "CreateSnapshot - failed to get snapshot info", err.Error())
		}
		if errResp == nil {
			return nil, status.Error(codes.Internal, err.Error())
		}

	} else {

		if snapInfo.ParentVolName != req.GetSourceVolumeId() {
			return nil, status.Error(codes.AlreadyExists, fmt.Sprintf("CreateSnapshot - snapshot %s belongs to different volume %s", snapInfo.ParentVolName, req.GetSourceVolumeId()))
		}

		t, e := time.Parse(time.RFC3339, snapInfo.SnapTime)
		if e != nil {
			return nil, status.Errorf(codes.Internal, "failed to parse time from snapshot response", e.Error())
		}
		return &csi.CreateSnapshotResponse{
			Snapshot: &csi.Snapshot{
				Id:             snapInfo.VolInfo.Name,
				SourceVolumeId: snapInfo.ParentVolName,
				CreatedAt:      t.Unix(),
				Status: &csi.SnapshotStatus{
					Type: csi.SnapshotStatus_READY,
				},
				//TODO need to add size
				//SizeBytes:snapInfo.VolInfo

			},
		}, nil
	}
	//create snapshot with gd2
	snapReq := api.SnapCreateReq{
		VolName:  req.SourceVolumeId,
		SnapName: req.Name,
		Force:    true,
	}
	snapResp, err := cs.client.SnapshotCreate(snapReq)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "CreateSnapshot - snapshot create failed", err.Error())
	}
	t1, e := time.Parse(time.RFC3339, snapResp.SnapTime)
	if e != nil {
		return nil, status.Errorf(codes.Internal, "failed to parse time from snapshot response", e.Error())
	}
	return &csi.CreateSnapshotResponse{
		Snapshot: &csi.Snapshot{
			Id:             snapResp.VolInfo.Name,
			SourceVolumeId: snapResp.ParentVolName,
			CreatedAt:      t1.Unix(),
			Status: &csi.SnapshotStatus{
				Type: csi.SnapshotStatus_READY,
			},
			//TODO need to add size
			//SizeBytes:snapInfo.VolInfo

		},
	}, nil
}

//DeleteSnapshot deletes snapshot with given name
func (cs *ControllerServer) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "DeleteSnapshot request is nil")
	}
	if req.GetSnapshotId() == "" {
		return nil, status.Error(codes.InvalidArgument, "DeleteSnapshot - snapshotId is empty")
	}
	glog.V(4).Infof("deleting snapshot %s", req.GetSnapshotId())

	err := cs.client.SnapshotDeactivate(req.GetSnapshotId())
	if err != nil {
		errResp := cs.client.LastErrorResponse()
		if errResp != nil && errResp.StatusCode == http.StatusNotFound {
			return &csi.DeleteSnapshotResponse{}, nil
		}
		return nil, status.Errorf(codes.Internal, "DeleteSnapshot - failed to deactivate snapshot", err.Error())

	}
	err = cs.client.SnapshotDelete(req.SnapshotId)
	if err != nil {
		errResp := cs.client.LastErrorResponse()
		if errResp != nil && errResp.StatusCode == http.StatusNotFound {
			return &csi.DeleteSnapshotResponse{}, nil
		}
		return nil, status.Errorf(codes.Internal, "DeleteSnapshot - failed to delete snapshot", err.Error())
	}
	return &csi.DeleteSnapshotResponse{}, nil
}

//ListSnapshots list snapshosts
//if snapshot id is present it will fetch snapshot info
//if volume id is sent it will fetch snapshots belongs to that volume
//or else it will fetch complete snapshot list
func (cs *ControllerServer) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	var (
		snaplist   api.SnapListResp
		err        error
		startToken int32
	)

	if req.GetStartingToken() != "" {
		i, err := strconv.ParseUint(req.StartingToken, 10, 32)
		if err != nil {
			return nil, status.Errorf(codes.Aborted, "invalid starting token", err.Error())
		}
		startToken = int32(i)
	}

	if len(req.GetSnapshotId()) != 0 {
		var entries []*csi.ListSnapshotsResponse_Entry
		snap, err := cs.GfDriver.client.SnapshotInfo(req.SnapshotId)
		if err != nil {
			errResp := cs.client.LastErrorResponse()
			if errResp != nil && errResp.StatusCode == http.StatusNotFound {
				resp := csi.ListSnapshotsResponse{}
				return &resp, nil
			}
			return nil, status.Errorf(codes.NotFound, "ListSnapshot - failed to get snapshot info", err.Error())

		}
		entries = append(entries, &csi.ListSnapshotsResponse_Entry{
			Snapshot: &csi.Snapshot{
				Id:             snap.VolInfo.Name,
				SourceVolumeId: snap.ParentVolName,
				CreatedAt:      time.Now().Unix(),
				//Status: snapInfo.VolInfo.State.String(),
				//TODO need to add size also
				Status: &csi.SnapshotStatus{
					Type: csi.SnapshotStatus_READY,
				},
			},
		})

		resp := csi.ListSnapshotsResponse{}
		resp.Entries = entries
		return &resp, nil
	}

	//If volume id is sent
	if len(req.GetSourceVolumeId()) != 0 {
		snaplist, err = cs.client.SnapshotList(req.SourceVolumeId)
		if err != nil {
			errResp := cs.client.LastErrorResponse()
			if errResp != nil && errResp.StatusCode == http.StatusNotFound {
				resp := csi.ListSnapshotsResponse{}
				return &resp, nil
			}
			return nil, status.Errorf(codes.Internal, "ListSnapshot - failed to get snapshots", err.Error())
		}
	} else {
		//get all snashot and send back the response
		snaplist, err = cs.client.SnapshotList("")
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to get snapshots", err.Error())
		}
	}

	var entries []*csi.ListSnapshotsResponse_Entry
	for _, snap := range snaplist {
		for _, s := range snap.SnapList {
			t, e := time.Parse(time.RFC3339, s.SnapTime)
			if e != nil {
				return nil, status.Errorf(codes.Internal, "failed to parse time ", err.Error())
			}

			entries = append(entries, &csi.ListSnapshotsResponse_Entry{
				Snapshot: &csi.Snapshot{
					Id:             s.VolInfo.Name,
					SourceVolumeId: snap.ParentName,
					CreatedAt:      t.Unix(),
					//Status: snapInfo.VolInfo.State.String(),
					//TODO need to add size also
					Status: &csi.SnapshotStatus{
						Type: csi.SnapshotStatus_READY,
					},
				},
			})
		}

	}

	if req.GetStartingToken() != "" && int(startToken) > len(snaplist) {
		return nil, status.Error(codes.Aborted, "invalid starting token")
	}

	var (
		maximumEntries   = req.MaxEntries
		nextToken        int32
		remainingEntries = int32(len(snaplist)) - startToken
	)

	if maximumEntries == 0 || maximumEntries > remainingEntries {
		maximumEntries = remainingEntries
	}

	resp := csi.ListSnapshotsResponse{}

	resp.Entries = entries[startToken : startToken+maximumEntries]

	if nextToken = startToken + maximumEntries; nextToken < int32(len(snaplist)) {
		resp.NextToken = fmt.Sprintf("%d", nextToken)
	}

	return &resp, nil
}
