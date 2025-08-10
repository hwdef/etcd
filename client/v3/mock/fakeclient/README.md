# Fake Client

This package provides a fake implementation of the etcd client for testing purposes. It allows you to mock etcd client responses without needing to connect to a real etcd server.

## Usage

```go
import "go.etcd.io/etcd/client/v3/mock/fakeclient"

// Create a new fake client
fakeClient := fakeclient.NewFakeClient()

// Set up responses for specific methods
fakeClient.SetAlarmResponse(fakeclient.AlarmList, &clientv3.AlarmResponse{
    Header: &etcdserverpb.ResponseHeader{},
    Alarms: []*etcdserverpb.AlarmMember{
        {
            MemberID: 12345,
            Alarm:    etcdserverpb.AlarmType_NOSPACE,
        },
    },
}, nil)

// Use the fake client in your tests
resp, err := fakeClient.AlarmList(context.Background())
```

## Supported Methods

The fake client currently supports mocking the following methods:

- Alarm-related methods (AlarmDisarm, AlarmList)
- KV-related methods (Get, Put, Delete, etc.)
- Lease-related methods
- Maintenance-related methods
- Auth-related methods
- Watch-related methods

More methods can be added as needed.

## Extending

To add support for additional methods:

1. Add the method to the appropriate interface in the FakeClient struct
2. Implement the method to return either predefined responses or default values
3. Add any necessary response types or configuration methods