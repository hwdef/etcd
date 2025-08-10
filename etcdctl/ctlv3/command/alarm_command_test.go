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

package command

import (
	"fmt"
	"io"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestNewAlarmCommand(t *testing.T) {
	cmd := NewAlarmCommand()
	require.NotNil(t, cmd)
	assert.Equal(t, "alarm <subcommand>", cmd.Use)
	assert.Equal(t, "Alarm related commands", cmd.Short)
	
	// Check subcommands
	assert.Equal(t, 2, len(cmd.Commands()))
	
	// Check disarm command
	disarmCmd := cmd.Commands()[0]
	assert.Equal(t, "disarm", disarmCmd.Name())
	assert.Equal(t, "Disarms all alarms", disarmCmd.Short)
	
	// Check list command
	listCmd := cmd.Commands()[1]
	assert.Equal(t, "list", listCmd.Name())
	assert.Equal(t, "Lists all alarms", listCmd.Short)
}

// addRequiredFlags adds all the required flags to a command
func addRequiredFlags(cmd *cobra.Command) {
	// Add required flags from ctl.go
	cmd.Flags().StringSlice("endpoints", []string{"127.0.0.1:2379"}, "gRPC endpoints")
	cmd.Flags().Bool("debug", false, "enable client-side debug logging")
	cmd.Flags().StringP("write-out", "w", "simple", "set the output format (fields, json, protobuf, simple, table)")
	cmd.Flags().Bool("hex", false, "print byte strings as hex encoded strings")
	cmd.Flags().Duration("dial-timeout", 2, "dial timeout for client connections")
	cmd.Flags().Duration("command-timeout", 5, "timeout for short running command (excluding dial timeout)")
	cmd.Flags().Duration("keepalive-time", 2, "keepalive time for client connections")
	cmd.Flags().Duration("keepalive-timeout", 6, "keepalive timeout for client connections")
	cmd.Flags().Int("max-request-bytes", 0, "client-side request send limit in bytes (if 0, it defaults to 2.0 MiB (2 * 1024 * 1024).)")
	cmd.Flags().Int("max-recv-bytes", 0, "client-side response receive limit in bytes (if 0, it defaults to \"math.MaxInt32\")")
	cmd.Flags().Bool("insecure-transport", true, "disable transport security for client connections")
	cmd.Flags().Bool("insecure-discovery", true, "accept insecure SRV records describing cluster endpoints")
	cmd.Flags().Bool("insecure-skip-tls-verify", false, "skip server certificate verification (CAUTION: this option should be enabled only for testing purposes)")
	cmd.Flags().String("cert", "", "identify secure client using this TLS certificate file")
	cmd.Flags().String("key", "", "identify secure client using this TLS key file")
	cmd.Flags().String("cacert", "", "verify certificates of TLS-enabled secure servers using this CA bundle")
	cmd.Flags().String("auth-jwt-token", "", "JWT token used for authentication (if this option is used, --user and --password should not be set)")
	cmd.Flags().String("user", "", "username[:password] for authentication (prompt if password is not supplied)")
	cmd.Flags().String("password", "", "password for authentication (if this option is used, --user option shouldn't include password)")
	cmd.Flags().StringP("discovery-srv", "d", "", "domain name to query for SRV records describing cluster endpoints")
	cmd.Flags().String("discovery-srv-name", "", "service name to query when using DNS discovery")
}

// testPrinter implements a minimal printer interface for testing
type testPrinter struct {
	writer io.Writer
}

func (s *testPrinter) Del(resp clientv3.DeleteResponse) {
	fmt.Fprintf(s.writer, "Deleted: %d\n", resp.Deleted)
}

func (s *testPrinter) Get(resp clientv3.GetResponse) {
	fmt.Fprintf(s.writer, "Get response: %+v\n", resp)
}

func (s *testPrinter) Put(r clientv3.PutResponse) {
	fmt.Fprintf(s.writer, "Put response: %+v\n", r)
}

func (s *testPrinter) Txn(resp clientv3.TxnResponse) {
	fmt.Fprintf(s.writer, "Txn response: %+v\n", resp)
}

func (s *testPrinter) Watch(resp clientv3.WatchResponse) {
	fmt.Fprintf(s.writer, "Watch response: %+v\n", resp)
}

func (s *testPrinter) Grant(resp clientv3.LeaseGrantResponse) {
	fmt.Fprintf(s.writer, "Grant response: %+v\n", resp)
}

func (s *testPrinter) Revoke(id clientv3.LeaseID, r clientv3.LeaseRevokeResponse) {
	fmt.Fprintf(s.writer, "Revoke response: %+v\n", r)
}

func (s *testPrinter) KeepAlive(resp clientv3.LeaseKeepAliveResponse) {
	fmt.Fprintf(s.writer, "KeepAlive response: %+v\n", resp)
}

func (s *testPrinter) TimeToLive(resp clientv3.LeaseTimeToLiveResponse, keys bool) {
	fmt.Fprintf(s.writer, "TimeToLive response: %+v\n", resp)
}

func (s *testPrinter) Leases(resp clientv3.LeaseLeasesResponse) {
	fmt.Fprintf(s.writer, "Leases response: %+v\n", resp)
}

func (s *testPrinter) Alarm(resp clientv3.AlarmResponse) {
	for _, e := range resp.Alarms {
		fmt.Fprintf(s.writer, "%+v\n", e)
	}
}

func (s *testPrinter) MemberAdd(r clientv3.MemberAddResponse) {
	fmt.Fprintf(s.writer, "MemberAdd response: %+v\n", r)
}

func (s *testPrinter) MemberRemove(id uint64, r clientv3.MemberRemoveResponse) {
	fmt.Fprintf(s.writer, "MemberRemove response: %+v\n", r)
}

func (s *testPrinter) MemberUpdate(id uint64, r clientv3.MemberUpdateResponse) {
	fmt.Fprintf(s.writer, "MemberUpdate response: %+v\n", r)
}

func (s *testPrinter) MemberPromote(id uint64, r clientv3.MemberPromoteResponse) {
	fmt.Fprintf(s.writer, "MemberPromote response: %+v\n", r)
}

func (s *testPrinter) MemberList(resp clientv3.MemberListResponse) {
	fmt.Fprintf(s.writer, "MemberList response: %+v\n", resp)
}

func (s *testPrinter) EndpointHealth(hs []epHealth) {
	fmt.Fprintf(s.writer, "EndpointHealth response: %+v\n", hs)
}

func (s *testPrinter) EndpointStatus(statusList []epStatus) {
	fmt.Fprintf(s.writer, "EndpointStatus response: %+v\n", statusList)
}

func (s *testPrinter) EndpointHashKV(hashList []epHashKV) {
	fmt.Fprintf(s.writer, "EndpointHashKV response: %+v\n", hashList)
}

func (s *testPrinter) MoveLeader(leader, target uint64, r clientv3.MoveLeaderResponse) {
	fmt.Fprintf(s.writer, "MoveLeader response: %+v\n", r)
}

func (s *testPrinter) DowngradeValidate(r clientv3.DowngradeResponse) {
	fmt.Fprintf(s.writer, "DowngradeValidate response: %+v\n", r)
}

func (s *testPrinter) DowngradeEnable(r clientv3.DowngradeResponse) {
	fmt.Fprintf(s.writer, "DowngradeEnable response: %+v\n", r)
}

func (s *testPrinter) DowngradeCancel(r clientv3.DowngradeResponse) {
	fmt.Fprintf(s.writer, "DowngradeCancel response: %+v\n", r)
}

func (s *testPrinter) RoleAdd(role string, r clientv3.AuthRoleAddResponse) {
	fmt.Fprintf(s.writer, "RoleAdd response: %+v\n", r)
}

func (s *testPrinter) RoleGet(role string, r clientv3.AuthRoleGetResponse) {
	fmt.Fprintf(s.writer, "RoleGet response: %+v\n", r)
}

func (s *testPrinter) RoleList(r clientv3.AuthRoleListResponse) {
	fmt.Fprintf(s.writer, "RoleList response: %+v\n", r)
}

func (s *testPrinter) RoleDelete(role string, r clientv3.AuthRoleDeleteResponse) {
	fmt.Fprintf(s.writer, "RoleDelete response: %+v\n", r)
}

func (s *testPrinter) RoleGrantPermission(role string, r clientv3.AuthRoleGrantPermissionResponse) {
	fmt.Fprintf(s.writer, "RoleGrantPermission response: %+v\n", r)
}

func (s *testPrinter) RoleRevokePermission(role string, key string, end string, r clientv3.AuthRoleRevokePermissionResponse) {
	fmt.Fprintf(s.writer, "RoleRevokePermission response: %+v\n", r)
}

func (s *testPrinter) UserAdd(name string, r clientv3.AuthUserAddResponse) {
	fmt.Fprintf(s.writer, "UserAdd response: %+v\n", r)
}

func (s *testPrinter) UserGet(name string, r clientv3.AuthUserGetResponse) {
	fmt.Fprintf(s.writer, "UserGet response: %+v\n", r)
}

func (s *testPrinter) UserChangePassword(r clientv3.AuthUserChangePasswordResponse) {
	fmt.Fprintf(s.writer, "UserChangePassword response: %+v\n", r)
}

func (s *testPrinter) UserGrantRole(user string, role string, r clientv3.AuthUserGrantRoleResponse) {
	fmt.Fprintf(s.writer, "UserGrantRole response: %+v\n", r)
}

func (s *testPrinter) UserRevokeRole(user string, role string, r clientv3.AuthUserRevokeRoleResponse) {
	fmt.Fprintf(s.writer, "UserRevokeRole response: %+v\n", r)
}

func (s *testPrinter) UserDelete(user string, r clientv3.AuthUserDeleteResponse) {
	fmt.Fprintf(s.writer, "UserDelete response: %+v\n", r)
}

func (s *testPrinter) UserList(r clientv3.AuthUserListResponse) {
	fmt.Fprintf(s.writer, "UserList response: %+v\n", r)
}

func (s *testPrinter) AuthStatus(r clientv3.AuthStatusResponse) {
	fmt.Fprintf(s.writer, "AuthStatus response: %+v\n", r)
}