package glusterfs

import (
	"encoding/json"
	"fmt"
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

var volumeCache = make(map[string]uint64)
var snapCache = make(map[string]string)

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
				volumeCache["test1"] = 1000
				return
			}
			fmt.Println("#@@@@@ url ", r.URL.String())
			if strings.Contains(r.URL.String(), "/v1/snapshots") {
				if v, ok := r.URL.Query()["volume"]; ok {

					var res api.SnapListResp
					res = make(api.SnapListResp, 1)
					res[0].ParentName = v[0]
					res[0].SnapName = []string{snapCache[v[0]]}
					//res[0].VolInfo.Name = snapCache[v[0]]
					//res[0].ParentVolName = v[0]
					writeResp(w, http.StatusOK, res, t)
					return
				}
				var res api.SnapListResp
				res = make(api.SnapListResp, 1)
				res[0] = api.SnapList{
					ParentName: "voleTest",
					SnapName:   []string{"snaptest1"},
				}
				snapCache["snaptest1"] = "voleTest"
				writeResp(w, http.StatusOK, res, t)
				fmt.Println("are we sending back the respones from here itself")
				return
			}
			fmt.Println("#@@@@@ url ", r.URL.String())
			fmt.Println("##########suffix is present ", strings.Contains(r.URL.String(), "/v1/snapshots"))
			fmt.Println("suffix is present ", strings.Contains(r.URL.String(), "/v1/snapshot"))
			if strings.HasPrefix(r.URL.String(), "/v1/snapshot") {
				vol := strings.Split(strings.Trim(r.URL.String(), "/"), "/")
				fmt.Println("########## snapshot cache ", snapCache)
				fmt.Println("##### snap is present with name ", vol[2])
				fmt.Println("snap present", vol[2], checkSnap(vol[2]))
				if checkSnap(vol[2]) {
					var res api.SnapInfo
					res.VolInfo.Name = vol[2]
					res.ParentVolName = snapCache[vol[2]]
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
				fmt.Println("sadfasdfsending not found error to client", resp)
				writeResp(w, http.StatusNotFound, resp, t)
				return
			}
			vol := strings.Split(strings.Trim(r.URL.String(), "/"), "/")
			fmt.Println("############### volume url", r.URL.String())
			fmt.Println("############### volume name", vol)
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
						Capacity: volumeCache[vol[2]],
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
			w.WriteHeader(http.StatusNoContent)
			fmt.Println("##########33 am i getting any request here", r.URL.String())
			return

		case "POST":

			if strings.HasSuffix(r.URL.String(), "start") || strings.HasSuffix(r.URL.String(), "stop") {
				w.WriteHeader(http.StatusOK)
				return
			}
			if strings.HasPrefix(r.URL.String(), "/v1/snapshot") {
				var resp api.SnapCreateResp

				var req api.SnapCreateReq
				defer r.Body.Close()
				json.NewDecoder(r.Body).Decode(&req)
				resp.VolInfo.Name = req.SnapName
				resp.ParentVolName = req.VolName
				snapCache[req.SnapName] = req.VolName
				writeResp(w, http.StatusCreated, resp, t)
				return
			}

			if strings.HasPrefix(r.URL.String(), "/v1/volumes") {
				var resp api.VolumeCreateResp

				var req api.VolCreateReq
				defer r.Body.Close()
				json.NewDecoder(r.Body).Decode(&req)
				resp.Name = req.Name
				volumeCache[req.Name] = req.Size
				writeResp(w, http.StatusCreated, resp, t)
				return
			}
			if strings.HasSuffix(r.URL.String(), "activate") || strings.HasSuffix(r.URL.String(), "deactivate") {
				w.WriteHeader(http.StatusOK)
				return
			} else {
				fmt.Println("########33 post URl ", r.URL.String())
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

func checkVolume(vol string) bool {
	_, ok := volumeCache[vol]
	return ok
}

func checkSnap(vol string) bool {
	fmt.Println("list of snapshots presenet ", snapCache)
	_, ok := snapCache[vol]
	return ok
}
func writeResp(w http.ResponseWriter, status int, resp interface{}, t *testing.T) {
	w.WriteHeader(status)
	err := json.NewEncoder(w).Encode(&resp)
	if err != nil {
		t.Fatal("Failed to write response ", err)
	}
}
