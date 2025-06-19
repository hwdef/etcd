package command

import (
	"context"
	"io"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

// --- Fake Client ---

func NewFakeClient() *clientv3.Client {
	c := &clientv3.Client{}
	c.KV = &fakeKV{}
	c.Lease = &fakeLease{closeChan: make(chan struct{})}
	c.Watcher = &fakeWatcher{closeChan: make(chan struct{})}
	c.Auth = &fakeAuth{}
	c.Maintenance = &fakeMaintenance{}
	c.Cluster = &fakeCluster{}
	return c
}

// --- Fake KV ---

type fakeKV struct {
	clientv3.KV

	putResp     *clientv3.PutResponse
	getResp     *clientv3.GetResponse
	delResp     *clientv3.DeleteResponse
	compactResp *clientv3.CompactResponse
	doResp      clientv3.OpResponse
	txnResp     *clientv3.TxnResponse

	putErr     error
	getErr     error
	delErr     error
	compactErr error
	doErr      error
	txnErr     error
}

func (kv *fakeKV) Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	return kv.putResp, kv.putErr
}
func (kv *fakeKV) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return kv.getResp, kv.getErr
}
func (kv *fakeKV) Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	return kv.delResp, kv.delErr
}
func (kv *fakeKV) Compact(ctx context.Context, rev int64, opts ...clientv3.CompactOption) (*clientv3.CompactResponse, error) {
	return kv.compactResp, kv.compactErr
}
func (kv *fakeKV) Do(ctx context.Context, op clientv3.Op) (clientv3.OpResponse, error) {
	return kv.doResp, kv.doErr
}
func (kv *fakeKV) Txn(ctx context.Context) clientv3.Txn { return &faketxner{kv: kv} }
func (kv *fakeKV) TxnCommit() (*clientv3.TxnResponse, error) {
	return kv.txnResp, kv.txnErr
}

type faketxner struct {
	clientv3.Txn
	kv *fakeKV
}

func (txn *faketxner) If(cs ...clientv3.Cmp) clientv3.Txn   { return txn }
func (txn *faketxner) Then(ops ...clientv3.Op) clientv3.Txn { return txn }
func (txn *faketxner) Else(ops ...clientv3.Op) clientv3.Txn { return txn }
func (txn *faketxner) Commit() (*clientv3.TxnResponse, error) {
	return txn.kv.TxnCommit()
}

// --- Fake Lease ---

type fakeLease struct {
	clientv3.Lease

	grantResp         *clientv3.LeaseGrantResponse
	revokeResp        *clientv3.LeaseRevokeResponse
	ttlResp           *clientv3.LeaseTimeToLiveResponse
	leasesResp        *clientv3.LeaseLeasesResponse
	keepAliveRespChan chan *clientv3.LeaseKeepAliveResponse
	keepAliveResp     *clientv3.LeaseKeepAliveResponse
	closeChan         chan struct{}

	grantErr     error
	revokeErr    error
	ttlErr       error
	leasesErr    error
	keepAliveErr error
}

func (l *fakeLease) Grant(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error) {
	return l.grantResp, l.grantErr
}
func (l *fakeLease) Revoke(ctx context.Context, id clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error) {
	return l.revokeResp, l.revokeErr
}
func (l *fakeLease) TimeToLive(ctx context.Context, id clientv3.LeaseID, opts ...clientv3.LeaseOption) (*clientv3.LeaseTimeToLiveResponse, error) {
	return l.ttlResp, l.ttlErr
}
func (l *fakeLease) Leases(ctx context.Context) (*clientv3.LeaseLeasesResponse, error) {
	return l.leasesResp, l.leasesErr
}
func (l *fakeLease) KeepAlive(ctx context.Context, id clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	if l.keepAliveRespChan == nil {
		l.keepAliveRespChan = make(chan *clientv3.LeaseKeepAliveResponse)
		go func() {
			select {
			case l.keepAliveRespChan <- l.keepAliveResp:
			case <-l.closeChan:
			}
		}()
	}
	return l.keepAliveRespChan, l.keepAliveErr
}
func (l *fakeLease) KeepAliveOnce(ctx context.Context, id clientv3.LeaseID) (*clientv3.LeaseKeepAliveResponse, error) {
	return l.keepAliveResp, l.keepAliveErr
}
func (l *fakeLease) Close() error {
	close(l.closeChan)
	return nil
}

// --- Fake Watcher ---

type fakeWatcher struct {
	clientv3.Watcher
	watchRespChan chan clientv3.WatchResponse
	watchErr      error
	closeChan     chan struct{}
}

func (w *fakeWatcher) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	if w.watchRespChan == nil {
		w.watchRespChan = make(chan clientv3.WatchResponse)
		go func() {
			select {
			case <-w.closeChan:
			}
		}()
	}
	return w.watchRespChan
}
func (w *fakeWatcher) RequestProgress(ctx context.Context) error { return nil }
func (w *fakeWatcher) Close() error {
	close(w.closeChan)
	return nil
}

// --- Fake Auth ---

type fakeAuth struct {
	clientv3.Auth
	authEnableResp  *clientv3.AuthEnableResponse
	authDisableResp *clientv3.AuthDisableResponse
	authStatusResp  *clientv3.AuthStatusResponse

	userAddResp    *clientv3.AuthUserAddResponse
	userGetResp    *clientv3.AuthUserGetResponse
	userDeleteResp *clientv3.AuthUserDeleteResponse
	userListResp   *clientv3.AuthUserListResponse

	userChangePasswordResp *clientv3.AuthUserChangePasswordResponse
	userGrantRoleResp      *clientv3.AuthUserGrantRoleResponse
	userRevokeRoleResp     *clientv3.AuthUserRevokeRoleResponse

	roleAddResp    *clientv3.AuthRoleAddResponse
	roleGetResp    *clientv3.AuthRoleGetResponse
	roleDeleteResp *clientv3.AuthRoleDeleteResponse
	roleListResp   *clientv3.AuthRoleListResponse

	roleGrantPermissionResp  *clientv3.AuthRoleGrantPermissionResponse
	roleRevokePermissionResp *clientv3.AuthRoleRevokePermissionResponse

	authEnableErr  error
	authDisableErr error
	authStatusErr  error

	userAddErr    error
	userGetErr    error
	userDeleteErr error
	userListErr   error

	userChangePasswordErr error
	userGrantRoleErr      error
	userRevokeRoleErr     error

	roleAddErr    error
	roleGetErr    error
	roleDeleteErr error
	roleListErr   error

	roleGrantPermissionErr  error
	roleRevokePermissionErr error
}

func (a *fakeAuth) AuthEnable(ctx context.Context) (*clientv3.AuthEnableResponse, error) {
	return a.authEnableResp, a.authEnableErr
}
func (a *fakeAuth) AuthDisable(ctx context.Context) (*clientv3.AuthDisableResponse, error) {
	return a.authDisableResp, a.authDisableErr
}
func (a *fakeAuth) AuthStatus(ctx context.Context) (*clientv3.AuthStatusResponse, error) {
	return a.authStatusResp, a.authStatusErr
}
func (a *fakeAuth) UserAdd(ctx context.Context, name, password string) (*clientv3.AuthUserAddResponse, error) {
	return a.userAddResp, a.userAddErr
}
func (a *fakeAuth) UserGet(ctx context.Context, name string) (*clientv3.AuthUserGetResponse, error) {
	return a.userGetResp, a.userGetErr
}
func (a *fakeAuth) UserDelete(ctx context.Context, name string) (*clientv3.AuthUserDeleteResponse, error) {
	return a.userDeleteResp, a.userDeleteErr
}
func (a *fakeAuth) UserList(ctx context.Context) (*clientv3.AuthUserListResponse, error) {
	return a.userListResp, a.userListErr
}
func (a *fakeAuth) UserChangePassword(ctx context.Context, name, password string) (*clientv3.AuthUserChangePasswordResponse, error) {
	return a.userChangePasswordResp, a.userChangePasswordErr
}
func (a *fakeAuth) UserGrantRole(ctx context.Context, user, role string) (*clientv3.AuthUserGrantRoleResponse, error) {
	return a.userGrantRoleResp, a.userGrantRoleErr
}
func (a *fakeAuth) UserRevokeRole(ctx context.Context, user, role string) (*clientv3.AuthUserRevokeRoleResponse, error) {
	return a.userRevokeRoleResp, a.userRevokeRoleErr
}
func (a *fakeAuth) RoleAdd(ctx context.Context, name string) (*clientv3.AuthRoleAddResponse, error) {
	return a.roleAddResp, a.roleAddErr
}
func (a *fakeAuth) RoleGet(ctx context.Context, name string) (*clientv3.AuthRoleGetResponse, error) {
	return a.roleGetResp, a.roleGetErr
}
func (a *fakeAuth) RoleDelete(ctx context.Context, name string) (*clientv3.AuthRoleDeleteResponse, error) {
	return a.roleDeleteResp, a.roleDeleteErr
}
func (a *fakeAuth) RoleList(ctx context.Context) (*clientv3.AuthRoleListResponse, error) {
	return a.roleListResp, a.roleListErr
}
func (a *fakeAuth) RoleGrantPermission(ctx context.Context, name string, key, rangeEnd string, permType clientv3.PermissionType) (*clientv3.AuthRoleGrantPermissionResponse, error) {
	return a.roleGrantPermissionResp, a.roleGrantPermissionErr
}
func (a *fakeAuth) RoleRevokePermission(ctx context.Context, role string, key, rangeEnd string) (*clientv3.AuthRoleRevokePermissionResponse, error) {
	return a.roleRevokePermissionResp, a.roleRevokePermissionErr
}
func (a *fakeAuth) UserAddWithOptions(ctx context.Context, name string, password string, opt *clientv3.UserAddOptions) (*clientv3.AuthUserAddResponse, error) {
	return a.userAddResp, a.userAddErr
}

// --- Fake Maintenance ---

type fakeMaintenance struct {
	clientv3.Maintenance
	alarmListResp   *clientv3.AlarmResponse
	alarmDisarmResp *clientv3.AlarmResponse
	defragmentResp  *clientv3.DefragmentResponse
	statusResp      *clientv3.StatusResponse
	snapshotReader  io.ReadCloser
	hashKVResp      *clientv3.HashKVResponse
	moveLeaderResp  *clientv3.MoveLeaderResponse

	alarmErr      error
	defragmentErr error
	statusErr     error
	snapshotErr   error
	hashKVErr     error
	moveLeaderErr error
}

func (m *fakeMaintenance) Alarm(ctx context.Context, memberID uint64, alarmType etcdserverpb.AlarmType) (*clientv3.AlarmResponse, error) {
	return m.alarmListResp, m.alarmErr
}
func (m *fakeMaintenance) Disarm(ctx context.Context, am *etcdserverpb.AlarmMember) (*clientv3.AlarmResponse, error) {
	return m.alarmDisarmResp, m.alarmErr
}
func (m *fakeMaintenance) Defragment(ctx context.Context, endpoint string) (*clientv3.DefragmentResponse, error) {
	return m.defragmentResp, m.defragmentErr
}
func (m *fakeMaintenance) Status(ctx context.Context, endpoint string) (*clientv3.StatusResponse, error) {
	return m.statusResp, m.statusErr
}
func (m *fakeMaintenance) Snapshot(ctx context.Context) (io.ReadCloser, error) {
	return m.snapshotReader, m.snapshotErr
}
func (m *fakeMaintenance) HashKV(ctx context.Context, endpoint string, rev int64) (*clientv3.HashKVResponse, error) {
	return m.hashKVResp, m.hashKVErr
}
func (m *fakeMaintenance) MoveLeader(ctx context.Context, targetID uint64) (*clientv3.MoveLeaderResponse, error) {
	return m.moveLeaderResp, m.moveLeaderErr
}
func (m *fakeMaintenance) Downgrade(ctx context.Context, action clientv3.DowngradeAction, version string) (*clientv3.DowngradeResponse, error) {
	return nil, nil
}

type fakeSnapshotReader struct {
	io.ReadCloser
}

// --- Fake Cluster ---

type fakeCluster struct {
	clientv3.Cluster
	memberListResp    *clientv3.MemberListResponse
	memberAddResp     *clientv3.MemberAddResponse
	memberRemoveResp  *clientv3.MemberRemoveResponse
	memberUpdateResp  *clientv3.MemberUpdateResponse
	memberPromoteResp *clientv3.MemberPromoteResponse

	memberListErr    error
	memberAddErr     error
	memberRemoveErr  error
	memberUpdateErr  error
	memberPromoteErr error
}

func (c *fakeCluster) MemberList(ctx context.Context, opts ...clientv3.OpOption) (*clientv3.MemberListResponse, error) {
	return c.memberListResp, c.memberListErr
}
func (c *fakeCluster) MemberAdd(ctx context.Context, peerAddrs []string) (*clientv3.MemberAddResponse, error) {
	return c.memberAdd(ctx, peerAddrs, false)
}
func (c *fakeCluster) MemberAddAsLearner(ctx context.Context, peerAddrs []string) (*clientv3.MemberAddResponse, error) {
	return c.memberAdd(ctx, peerAddrs, true)
}
func (c *fakeCluster) memberAdd(ctx context.Context, peerAddrs []string, isLearner bool) (*clientv3.MemberAddResponse, error) {
	return c.memberAddResp, c.memberAddErr
}
func (c *fakeCluster) MemberRemove(ctx context.Context, id uint64) (*clientv3.MemberRemoveResponse, error) {
	return c.memberRemoveResp, c.memberRemoveErr
}
func (c *fakeCluster) MemberUpdate(ctx context.Context, id uint64, peerAddrs []string) (*clientv3.MemberUpdateResponse, error) {
	return c.memberUpdateResp, c.memberUpdateErr
}
func (c *fakeCluster) MemberPromote(ctx context.Context, id uint64) (*clientv3.MemberPromoteResponse, error) {
	return c.memberPromoteResp, c.memberPromoteErr
}

// --- Mock clientConn ---

type mockClientConn struct {
	grpc.ClientConn
}
