// Copyright 2025 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package fakeclient provides a fake etcd client implementation for testing.
package fakeclient

import (
	"context"
	"io"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// FakeClient implements a minimal fake etcd client for testing alarm commands.
type FakeClient struct {
	// Mutex to protect concurrent access to the fake client
	mu sync.Mutex
	
	// Alarm related fields
	alarmResponses map[AlarmMethod]*AlarmResponse
}

// AlarmMethod represents the different alarm methods that can be called
type AlarmMethod string

const (
	AlarmDisarm AlarmMethod = "AlarmDisarm"
	AlarmList   AlarmMethod = "AlarmList"
)

// AlarmResponse holds the response and error for alarm methods
type AlarmResponse struct {
	Response *clientv3.AlarmResponse
	Error    error
}

// NewFakeClient creates a new FakeClient instance
func NewFakeClient() *FakeClient {
	return &FakeClient{
		alarmResponses: make(map[AlarmMethod]*AlarmResponse),
	}
}

// SetAlarmResponse sets the response for a specific alarm method
func (f *FakeClient) SetAlarmResponse(method AlarmMethod, resp *clientv3.AlarmResponse, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	
	f.alarmResponses[method] = &AlarmResponse{
		Response: resp,
		Error:    err,
	}
}

// AlarmDisarm implements the AlarmDisarm method
func (f *FakeClient) AlarmDisarm(ctx context.Context, alarm *clientv3.AlarmMember) (*clientv3.AlarmResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	
	if resp, ok := f.alarmResponses[AlarmDisarm]; ok {
		return resp.Response, resp.Error
	}
	
	// Default response
	return &clientv3.AlarmResponse{
		Header: &etcdserverpb.ResponseHeader{},
		Alarms: []*etcdserverpb.AlarmMember{},
	}, nil
}

// AlarmList implements the AlarmList method
func (f *FakeClient) AlarmList(ctx context.Context) (*clientv3.AlarmResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	
	if resp, ok := f.alarmResponses[AlarmList]; ok {
		return resp.Response, resp.Error
	}
	
	// Default response
	return &clientv3.AlarmResponse{
		Header: &etcdserverpb.ResponseHeader{},
		Alarms: []*etcdserverpb.AlarmMember{},
	}, nil
}

// Close implements the Close method
func (f *FakeClient) Close() error {
	return nil
}

// Minimal implementations for other required methods to satisfy the interface
func (f *FakeClient) Endpoints() []string {
	return []string{"localhost:2379"}
}

func (f *FakeClient) SetEndpoints(eps ...string) {}

func (f *FakeClient) Sync(ctx context.Context) error {
	return nil
}

func (f *FakeClient) AutoSync(ctx context.Context, interval time.Duration) error {
	return nil
}

func (f *FakeClient) Status(ctx context.Context, endpoint string) (*clientv3.StatusResponse, error) {
	return &clientv3.StatusResponse{}, nil
}

func (f *FakeClient) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return &clientv3.GetResponse{}, nil
}

func (f *FakeClient) Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	return &clientv3.PutResponse{}, nil
}

func (f *FakeClient) Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	return &clientv3.DeleteResponse{}, nil
}

func (f *FakeClient) Compact(ctx context.Context, rev int64, opts ...clientv3.CompactOption) (*clientv3.CompactResponse, error) {
	return &clientv3.CompactResponse{}, nil
}

func (f *FakeClient) Do(ctx context.Context, op clientv3.Op) (clientv3.OpResponse, error) {
	return clientv3.OpResponse{}, nil
}

func (f *FakeClient) Txn(ctx context.Context) clientv3.Txn {
	return &fakeTxn{}
}

func (f *FakeClient) Lease() clientv3.Lease {
	return &fakeLease{}
}

func (f *FakeClient) Watch() clientv3.Watcher {
	return &fakeWatcher{}
}

func (f *FakeClient) Maintenance() clientv3.Maintenance {
	return &fakeMaintenance{client: f}
}

func (f *FakeClient) Auth() clientv3.Auth {
	return &fakeAuth{}
}

// Minimal stub implementations
type fakeTxn struct{}
func (t *fakeTxn) If(cs ...clientv3.Cmp) clientv3.Txn { return t }
func (t *fakeTxn) Then(ops ...clientv3.Op) clientv3.Txn { return t }
func (t *fakeTxn) Else(ops ...clientv3.Op) clientv3.Txn { return t }
func (t *fakeTxn) Commit() (*clientv3.TxnResponse, error) { return &clientv3.TxnResponse{}, nil }

type fakeLease struct{}
func (l *fakeLease) Grant(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error) { return &clientv3.LeaseGrantResponse{}, nil }
func (l *fakeLease) Revoke(ctx context.Context, id clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error) { return &clientv3.LeaseRevokeResponse{}, nil }
func (l *fakeLease) TimeToLive(ctx context.Context, id clientv3.LeaseID, opts ...clientv3.LeaseOption) (*clientv3.LeaseTimeToLiveResponse, error) { return &clientv3.LeaseTimeToLiveResponse{}, nil }
func (l *fakeLease) Leases(ctx context.Context) (*clientv3.LeaseLeasesResponse, error) { return &clientv3.LeaseLeasesResponse{}, nil }
func (l *fakeLease) KeepAlive(ctx context.Context, id clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) { return nil, nil }
func (l *fakeLease) KeepAliveOnce(ctx context.Context, id clientv3.LeaseID) (*clientv3.LeaseKeepAliveResponse, error) { return &clientv3.LeaseKeepAliveResponse{}, nil }
func (l *fakeLease) Close() error { return nil }

type fakeWatcher struct{}
func (w *fakeWatcher) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan { return nil }
func (w *fakeWatcher) RequestProgress(ctx context.Context) error { return nil }
func (w *fakeWatcher) Close() error { return nil }

type fakeMaintenance struct {
	client *FakeClient
}
func (m *fakeMaintenance) AlarmList(ctx context.Context) (*clientv3.AlarmResponse, error) {
	return m.client.AlarmList(ctx)
}
func (m *fakeMaintenance) AlarmDisarm(ctx context.Context, am *clientv3.AlarmMember) (*clientv3.AlarmResponse, error) {
	return m.client.AlarmDisarm(ctx, am)
}
func (m *fakeMaintenance) Defragment(ctx context.Context, endpoint string) (*clientv3.DefragmentResponse, error) { return &clientv3.DefragmentResponse{}, nil }
func (m *fakeMaintenance) Status(ctx context.Context, endpoint string) (*clientv3.StatusResponse, error) { return &clientv3.StatusResponse{}, nil }
func (m *fakeMaintenance) HashKV(ctx context.Context, endpoint string, rev int64) (*clientv3.HashKVResponse, error) { return &clientv3.HashKVResponse{}, nil }
func (m *fakeMaintenance) Snapshot(ctx context.Context) (io.ReadCloser, error) { return nil, nil }
func (m *fakeMaintenance) SnapshotWithVersion(ctx context.Context) (*clientv3.SnapshotResponse, error) { return &clientv3.SnapshotResponse{}, nil }
func (m *fakeMaintenance) MoveLeader(ctx context.Context, targetID uint64) (*clientv3.MoveLeaderResponse, error) { return &clientv3.MoveLeaderResponse{}, nil }
func (m *fakeMaintenance) Downgrade(ctx context.Context, action clientv3.DowngradeAction, version string) (*clientv3.DowngradeResponse, error) { return &clientv3.DowngradeResponse{}, nil }
func (m *fakeMaintenance) AuthStatus(ctx context.Context) (*clientv3.AuthStatusResponse, error) { return &clientv3.AuthStatusResponse{}, nil }

type fakeAuth struct{}
func (a *fakeAuth) AuthEnable(ctx context.Context) (*clientv3.AuthEnableResponse, error) { return &clientv3.AuthEnableResponse{}, nil }
func (a *fakeAuth) AuthDisable(ctx context.Context) (*clientv3.AuthDisableResponse, error) { return &clientv3.AuthDisableResponse{}, nil }
func (a *fakeAuth) AuthStatus(ctx context.Context) (*clientv3.AuthStatusResponse, error) { return &clientv3.AuthStatusResponse{}, nil }
func (a *fakeAuth) UserAdd(ctx context.Context, name string, password string) (*clientv3.AuthUserAddResponse, error) { return &clientv3.AuthUserAddResponse{}, nil }
func (a *fakeAuth) UserAddWithOptions(ctx context.Context, name string, password string, opt *clientv3.UserAddOptions) (*clientv3.AuthUserAddResponse, error) { return &clientv3.AuthUserAddResponse{}, nil }
func (a *fakeAuth) UserDelete(ctx context.Context, name string) (*clientv3.AuthUserDeleteResponse, error) { return &clientv3.AuthUserDeleteResponse{}, nil }
func (a *fakeAuth) UserChangePassword(ctx context.Context, name string, password string) (*clientv3.AuthUserChangePasswordResponse, error) { return &clientv3.AuthUserChangePasswordResponse{}, nil }
func (a *fakeAuth) UserGrantRole(ctx context.Context, user string, role string) (*clientv3.AuthUserGrantRoleResponse, error) { return &clientv3.AuthUserGrantRoleResponse{}, nil }
func (a *fakeAuth) UserRevokeRole(ctx context.Context, user string, role string) (*clientv3.AuthUserRevokeRoleResponse, error) { return &clientv3.AuthUserRevokeRoleResponse{}, nil }
func (a *fakeAuth) UserList(ctx context.Context) (*clientv3.AuthUserListResponse, error) { return &clientv3.AuthUserListResponse{}, nil }
func (a *fakeAuth) UserGet(ctx context.Context, name string) (*clientv3.AuthUserGetResponse, error) { return &clientv3.AuthUserGetResponse{}, nil }
func (a *fakeAuth) RoleAdd(ctx context.Context, name string) (*clientv3.AuthRoleAddResponse, error) { return &clientv3.AuthRoleAddResponse{}, nil }
func (a *fakeAuth) RoleDelete(ctx context.Context, role string) (*clientv3.AuthRoleDeleteResponse, error) { return &clientv3.AuthRoleDeleteResponse{}, nil }
func (a *fakeAuth) RoleGrantPermission(ctx context.Context, name string, key, rangeEnd string, perm clientv3.PermissionType) (*clientv3.AuthRoleGrantPermissionResponse, error) { return &clientv3.AuthRoleGrantPermissionResponse{}, nil }
func (a *fakeAuth) RoleRevokePermission(ctx context.Context, name string, key, rangeEnd string) (*clientv3.AuthRoleRevokePermissionResponse, error) { return &clientv3.AuthRoleRevokePermissionResponse{}, nil }
func (a *fakeAuth) RoleList(ctx context.Context) (*clientv3.AuthRoleListResponse, error) { return &clientv3.AuthRoleListResponse{}, nil }
func (a *fakeAuth) RoleGet(ctx context.Context, name string) (*clientv3.AuthRoleGetResponse, error) { return &clientv3.AuthRoleGetResponse{}, nil }
func (a *fakeAuth) Authenticate(ctx context.Context, name string, password string) (*clientv3.AuthenticateResponse, error) { return &clientv3.AuthenticateResponse{}, nil }