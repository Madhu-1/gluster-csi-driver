package glusterfs

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/gluster/gluster-csi-driver/pkg/glusterfs/utils"
	"github.com/gluster/glusterd2/pkg/api"
	"github.com/gluster/glusterd2/pkg/restclient"
	"github.com/kubernetes-csi/csi-test/pkg/sanity"
	"github.com/pborman/uuid"
	"k8s.io/kubernetes/pkg/util/mount"
)

type volume struct {
	Size     uint64
	snapList []string
}

var volumeCache = make(map[string]volume)

func TestDriverSuite(t *testing.T) {
	glusterMounter = &mount.FakeMounter{}
	socket := "/tmp/csi.sock"
	endpoint := "unix://" + socket

	//cleanup socket file if already present
	os.Remove(socket)

	_, err := os.Create(socket)
	if err != nil {
		t.Fatal("Failed to create a socket file")
	}
	defer os.Remove(socket)

	id := uuid.Parse("02dfdd19-e01e-46ec-a887-97b309a7dd2f")
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		switch r.Method {
		case "GET":
			if strings.Contains(r.URL.String(), "/v1/peers") {
				var resp api.PeerListResp
				resp = make(api.PeerListResp, 1)
				resp[0] = api.PeerGetResp{
					Name: "node1.com",
					PeerAddresses: []string{
						"127.0.0.1:24008"},
					ClientAddresses: []string{
						"127.0.0.1:24007",
						"127.0.0.1:24007"},
					Online: true,
					PID:    24935,
					Metadata: map[string]string{
						"_zone": "02dfdd19-e01e-46ec-a887-97b309a7dd2f",
					},
				}
				resp = append(resp, api.PeerGetResp{
					Name: "node2.com",
					PeerAddresses: []string{
						"127.0.0.1:24008"},
					ClientAddresses: []string{
						"127.0.0.1:24007"},
					Online: true,
					PID:    24935,
					Metadata: map[string]string{
						"_zone": "02dfdd19-e01e-46ec-a887-97b309a7dd2f",
					},
				})
				writeResp(w, http.StatusOK, resp, t)
				return
			}

			if strings.HasSuffix(r.URL.String(), "/v1/volumes") {
				var resp api.VolumeListResp
				resp = make(api.VolumeListResp, 1)
				resp[0] = api.VolumeGetResp{
					ID:       id,
					Name:     "test1",
					Metadata: map[string]string{glusterDescAnn: glusterDescAnnValue},
				}

				resp = append(resp, api.VolumeGetResp{
					ID:       id,
					Name:     "test1",
					Metadata: map[string]string{glusterDescAnn: glusterDescAnnValue},
				})
				writeResp(w, http.StatusOK, resp, t)
				volumeCache["test1"] = volume{Size: 1000}
				return
			}

			if strings.Contains(r.URL.String(), "/v1/snapshots/") {
				vol := strings.Split(strings.Trim(r.URL.String(), "/"), "/")
				if getVolumeNameFromSnap(vol[2]) != "" {
					var res api.SnapInfo
					res.VolInfo.Name = vol[2]
					res.SnapTime = "2006-01-02T15:04:05Z"
					res.ParentVolName = getVolumeNameFromSnap(vol[2])
					writeResp(w, http.StatusOK, res, t)
					return
				}
				resp := api.ErrorResp{}
				resp.Errors = append(resp.Errors, api.HTTPError{
					Code:    1,
					Message: "failed to get snapshot",
					Fields: map[string]string{
						"failed": "failed",
					},
				})
				writeResp(w, http.StatusNotFound, resp, t)
				return
			}

			if strings.Contains(r.URL.String(), "/v1/snapshots") {
				if v, ok := r.URL.Query()["volume"]; ok {
					if getSnapNameFromVol(v[0]) == "" {
						writeResp(w, http.StatusOK, api.SnapListResp{}, t)
						return
					}
					var res api.SnapListResp
					res = make(api.SnapListResp, 0)
					snapList := api.SnapList{}
					for _, snap := range volumeCache[v[0]].snapList {

						listresp := api.SnapInfo{}

						listresp.VolInfo.Name = snap
						listresp.ParentVolName = v[0]
						listresp.SnapTime = "2006-01-02T15:04:05Z"
						snapList.ParentName = v[0]
						snapList.SnapList = append(snapList.SnapList, listresp)

					}
					res = append(res, snapList)

					writeResp(w, http.StatusOK, res, t)
					return

				}

				if isSnapsPresent() {
					var res api.SnapListResp
					res = make(api.SnapListResp, 0)
					for vol, snap := range volumeCache {
						snapList := api.SnapList{}
						for _, s := range snap.snapList {
							listresp := api.SnapInfo{}

							listresp.VolInfo.Name = s
							listresp.ParentVolName = vol
							listresp.SnapTime = "2006-01-02T15:04:05Z"
							snapList.ParentName = vol
							snapList.SnapList = append(snapList.SnapList, listresp)

						}
						if snapList.ParentName != "" {
							res = append(res, snapList)
						}
					}
					writeResp(w, http.StatusOK, res, t)
					return
				}
				var res api.SnapListResp
				res = make(api.SnapListResp, 1)
				listresp := api.SnapInfo{}
				listresp.VolInfo.Name = "snaptest1"
				listresp.ParentVolName = "voleTest"
				listresp.SnapTime = "2006-01-02T15:04:05Z"
				res[0].ParentName = "voleTest"
				res[0].SnapList = append(res[0].SnapList, listresp)
				volumeCache["volTest"] = volume{
					snapList: []string{"snaptest1"},
				}
				writeResp(w, http.StatusOK, res, t)
				return
			}

			vol := strings.Split(strings.Trim(r.URL.String(), "/"), "/")
			if checkVolume(vol[2]) {
				var resp api.VolumeStatusResp
				resp = api.VolumeStatusResp{
					Info: api.VolumeInfo{
						ID:       id,
						Name:     vol[2],
						Metadata: map[string]string{glusterDescAnn: glusterDescAnnValue},
					},
					Online: true,
					Size: api.SizeInfo{
						Capacity: volumeCache[vol[2]].Size,
					},
				}
				writeResp(w, http.StatusOK, resp, t)
				return
			}
			resp := api.ErrorResp{}
			resp.Errors = append(resp.Errors, api.HTTPError{
				Code: 1,
			})
			writeResp(w, http.StatusNotFound, resp, t)
			return

		case "DELETE":
			if strings.HasPrefix(r.URL.String(), "/v1/snapshot") {
				key := strings.Split(strings.Trim(r.URL.String(), "/"), "/")
				deleteSnap(key[2])
			}
			w.WriteHeader(http.StatusNoContent)
			return

		case "POST":

			if strings.HasSuffix(r.URL.String(), "start") || strings.HasSuffix(r.URL.String(), "stop") {
				w.WriteHeader(http.StatusOK)
				return
			}
			if strings.HasSuffix(r.URL.String(), "activate") || strings.HasSuffix(r.URL.String(), "deactivate") {
				w.WriteHeader(http.StatusOK)
				return
			}
			if strings.HasPrefix(r.URL.String(), "/v1/snapshot") {
				var resp api.SnapCreateResp

				var req api.SnapCreateReq
				defer r.Body.Close()
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				resp.VolInfo.Name = req.SnapName
				resp.ParentVolName = req.VolName
				resp.SnapTime = "2006-01-02T15:04:05Z"
				volResp := volumeCache[req.VolName]
				volResp.snapList = append(volResp.snapList, req.SnapName)
				volumeCache[req.VolName] = volResp
				// volumeCache[req.VolName].snapList = append(volumeCache[req.VolName].snapList, req.SnapName)
				//snapCache[req.SnapName] = req.VolName
				writeResp(w, http.StatusCreated, resp, t)
				return
			}

			if strings.HasPrefix(r.URL.String(), "/v1/volumes") {
				var resp api.VolumeCreateResp

				var req api.VolCreateReq
				defer r.Body.Close()
				json.NewDecoder(r.Body).Decode(&req)
				resp.Name = req.Name

				volumeCache[req.Name] = volume{Size: req.Size}
				writeResp(w, http.StatusCreated, resp, t)
				return
			}

		}
	}))

	defer ts.Close()

	url, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	doClient, err := restclient.New(url.String(), "", "", "", false)
	if err != nil {
		t.Fatal(err)
	}

	d := GfDriver{

		client: doClient,
	}
	d.Config = new(utils.Config)
	d.Endpoint = endpoint
	d.NodeID = "testing"
	go d.Run()
	defer d.Stop()

	mntStageDir := "/tmp/mntStageDir"
	mntDir := "/tmp/mntDir"
	defer os.RemoveAll(mntStageDir)
	defer os.RemoveAll(mntDir)

	cfg := &sanity.Config{
		StagingPath: mntStageDir,
		TargetPath:  mntDir,
		Address:     endpoint,
	}

	sanity.Test(t, cfg)
}

func isSnapsPresent() bool {
	found := false
	for _, value := range volumeCache {
		if len(value.snapList) > 0 {
			found = true
		}
	}
	return found
}
func checkVolume(vol string) bool {
	_, ok := volumeCache[vol]
	return ok
}

func deleteSnap(snapname string) {
	for key, value := range volumeCache {
		for i, s := range value.snapList {
			if s == snapname {
				resp := volumeCache[key]
				resp.snapList = append(resp.snapList[:i], resp.snapList[i+1:]...)
				volumeCache[key] = resp
				break
			}
		}
	}
}
func getVolumeNameFromSnap(snap string) string {
	for key, value := range volumeCache {
		for _, s := range value.snapList {
			if snap == s {
				return key
			}
		}
	}
	return ""
}

func getSnapNameFromVol(vol string) string {
	if len(volumeCache[vol].snapList) > 0 {
		return volumeCache[vol].snapList[0]
	}
	return ""
}

func writeResp(w http.ResponseWriter, status int, resp interface{}, t *testing.T) {
	w.WriteHeader(status)
	err := json.NewEncoder(w).Encode(&resp)
	if err != nil {
		t.Fatal("Failed to write response ", err)
	}
}
